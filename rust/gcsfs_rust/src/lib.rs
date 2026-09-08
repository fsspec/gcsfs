//! PyO3 bindings around the `google-cloud-storage` Rust SDK, exposing a
//! minimal range-read API that gcsfs can use as an alternative I/O backend.
//!
//! Two entry points are exposed:
//! * [`read_range_async`] returns a Python awaitable driven directly by the
//!   caller's asyncio event loop — no thread-pool hop.
//! * [`read_range`] is the blocking equivalent, kept as a fallback for callers
//!   without a running event loop.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::OnceCell as AsyncOnceCell;
use tokio::sync::RwLock;

use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::object_descriptor::ObjectDescriptor;

static HTTP_CLIENT: AsyncOnceCell<Storage> = AsyncOnceCell::const_new();
static GRPC_CLIENTS: AsyncOnceCell<Vec<Arc<Storage>>> = AsyncOnceCell::const_new();

// Thread-safe cache of open object descriptors: (bucket, object, chan_idx) -> Arc<ObjectDescriptor>
static DESCRIPTOR_CACHE: AsyncOnceCell<RwLock<HashMap<(String, String, usize), Arc<ObjectDescriptor>>>> =
    AsyncOnceCell::const_new();

/// Upper bound on speculative preallocation, so a bogus `end` from the caller
/// can't trigger an enormous up-front allocation.
const MAX_PREALLOC: usize = 64 * 1024 * 1024;

const DEFAULT_WORKER_THREADS: usize = 16;
const DEFAULT_GRPC_CHANNELS: usize = 200;

fn worker_threads() -> usize {
    std::env::var("GCSFS_RUST_WORKER_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_WORKER_THREADS)
}

fn grpc_channel_count() -> usize {
    std::env::var("GCSFS_RUST_CHANNELS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_GRPC_CHANNELS)
}

fn default_transport() -> String {
    std::env::var("GCSFS_RUST_TRANSPORT")
        .unwrap_or_else(|_| "http".to_string())
        .to_lowercase()
}

async fn http_client() -> PyResult<&'static Storage> {
    HTTP_CLIENT
        .get_or_try_init(|| async {
            Storage::builder()
                .build()
                .await
                .map_err(|e| PyIOError::new_err(format!("failed to build GCS HTTP client: {e}")))
        })
        .await
}

async fn grpc_clients() -> PyResult<&'static Vec<Arc<Storage>>> {
    GRPC_CLIENTS
        .get_or_try_init(|| async {
            let n = grpc_channel_count();
            let mut list = Vec::with_capacity(n);
            for _ in 0..n {
                let storage = Storage::builder()
                    .build()
                    .await
                    .map_err(|e| PyIOError::new_err(format!("failed to build GCS gRPC client: {e}")))?;
                list.push(Arc::new(storage));
            }
            Ok(list)
        })
        .await
}

async fn descriptor_cache() -> &'static RwLock<HashMap<(String, String, usize), Arc<ObjectDescriptor>>> {
    DESCRIPTOR_CACHE
        .get_or_init(|| async {
            RwLock::new(HashMap::new())
        })
        .await
}

fn get_grpc_chan_idx(object: &str, chunk_offset: u64, total_channels: usize) -> usize {
    if total_channels <= 4 {
        return ((chunk_offset / (16 * 1024 * 1024)) as usize) % total_channels;
    }
    let mut s = DefaultHasher::new();
    object.hash(&mut s);
    let obj_hash = s.finish() as usize;
    let k = 4;
    let num_groups = (total_channels / k).max(1);
    let group = obj_hash % num_groups;
    let sub = ((chunk_offset / (16 * 1024 * 1024)) as usize) % k;
    (group * k + sub) % total_channels
}

async fn get_or_open_descriptor(
    bucket: &str,
    object: &str,
    chan_idx: usize,
    client: &Storage,
) -> PyResult<Arc<ObjectDescriptor>> {
    let key = (bucket.to_string(), object.to_string(), chan_idx);
    let cache = descriptor_cache().await;
    {
        let r = cache.read().await;
        if let Some(desc) = r.get(&key) {
            return Ok(Arc::clone(desc));
        }
    }
    let mut w = cache.write().await;
    if let Some(desc) = w.get(&key) {
        return Ok(Arc::clone(desc));
    }
    let b_path = format!("projects/_/buckets/{bucket}");
    let desc = client
        .open_object(&b_path, object)
        .send()
        .await
        .map_err(|e| PyIOError::new_err(format!("failed to open gRPC descriptor for {object}: {e}")))?;
    let arc_desc = Arc::new(desc);
    w.insert(key, Arc::clone(&arc_desc));
    Ok(arc_desc)
}

async fn read_range_inner(
    bucket: String,
    object: String,
    start: Option<u64>,
    end: Option<u64>,
    generation: Option<i64>,
    transport: Option<String>,
) -> PyResult<Vec<u8>> {
    let trans = transport.unwrap_or_else(default_transport);
    let capacity = match (start, end) {
        (Some(s), Some(e)) if e > s => ((e - s) as usize).min(MAX_PREALLOC),
        _ => 0,
    };
    let mut contents = Vec::with_capacity(capacity);

    if trans == "grpc" && start.is_some() && end.is_some() {
        let s = start.unwrap();
        let e = end.unwrap();
        let len = e.saturating_sub(s);
        let clients = grpc_clients().await?;
        let chan_idx = get_grpc_chan_idx(&object, s, clients.len());
        let client = &clients[chan_idx];
        let desc = get_or_open_descriptor(&bucket, &object, chan_idx, client).await?;

        let mut reader = desc.read_range(ReadRange::segment(s, len)).await;
        while let Some(res) = reader.next().await {
            let chunk: bytes::Bytes = res
                .map_err(|err| PyIOError::new_err(format!("gRPC read failed for {object}: {err}")))?;
            contents.extend_from_slice(&chunk);
        }
    } else {
        let storage = http_client().await?;
        let mut builder = storage.read_object(format!("projects/_/buckets/{bucket}"), &object);
        if let Some(generation) = generation {
            builder = builder.set_generation(generation);
        }
        builder = match (start, end) {
            (Some(s), Some(e)) if e > s => builder.set_read_range(ReadRange::segment(s, e - s)),
            (Some(s), Some(_)) => builder.set_read_range(ReadRange::segment(s, 0)),
            (Some(s), None) => builder.set_read_range(ReadRange::offset(s)),
            (None, Some(e)) => builder.set_read_range(ReadRange::segment(0, e)),
            (None, None) => builder,
        };
        let mut reader = builder
            .send()
            .await
            .map_err(|err| PyIOError::new_err(format!("GCS HTTP read failed for {object}: {err}")))?;

        while let Some(chunk) = reader
            .next()
            .await
            .transpose()
            .map_err(|e| PyIOError::new_err(format!("GCS HTTP read failed for {object}: {e}")))?
        {
            contents.extend_from_slice(&chunk);
        }
    }
    Ok(contents)
}

/// Raw pointer into a Python `bytes` buffer, moved into the read task.
///
/// Safe to send because exactly one task writes to it, and the owning object is
/// never visible to Python until that task completes.
struct BufPtr(*mut u8);
unsafe impl Send for BufPtr {}

/// Streams a range straight into `dst`, returning how many bytes arrived.
async fn read_range_into(
    bucket: String,
    object: String,
    start: u64,
    end: u64,
    generation: Option<i64>,
    dst: BufPtr,
    cap: usize,
    transport: Option<String>,
) -> PyResult<usize> {
    let trans = transport.unwrap_or_else(default_transport);
    let len = end.saturating_sub(start);
    let mut written = 0usize;

    if trans == "grpc" {
        let clients = grpc_clients().await?;
        let chan_idx = get_grpc_chan_idx(&object, start, clients.len());
        let client = &clients[chan_idx];
        let desc = get_or_open_descriptor(&bucket, &object, chan_idx, client).await?;

        let mut reader = desc.read_range(ReadRange::segment(start, len)).await;
        while let Some(res) = reader.next().await {
            let chunk: bytes::Bytes = res
                .map_err(|e| PyIOError::new_err(format!("gRPC read failed for {object}: {e}")))?;
            if written + chunk.len() > cap {
                return Err(PyIOError::new_err(format!(
                    "GCS returned more data than requested for {object}: {} > {cap}",
                    written + chunk.len()
                )));
            }
            unsafe { std::ptr::copy_nonoverlapping(chunk.as_ptr(), dst.0.add(written), chunk.len()) };
            written += chunk.len();
        }
    } else {
        let storage = http_client().await?;
        let mut builder = storage.read_object(format!("projects/_/buckets/{bucket}"), &object);
        if let Some(generation) = generation {
            builder = builder.set_generation(generation);
        }
        let mut reader = builder
            .set_read_range(ReadRange::segment(start, len))
            .send()
            .await
            .map_err(|e| PyIOError::new_err(format!("HTTP read failed for {object}: {e}")))?;

        while let Some(res) = reader.next().await {
            let chunk: bytes::Bytes = res
                .map_err(|e| PyIOError::new_err(format!("HTTP read failed for {object}: {e}")))?;
            if written + chunk.len() > cap {
                return Err(PyIOError::new_err(format!(
                    "GCS returned more data than requested for {object}: {} > {cap}",
                    written + chunk.len()
                )));
            }
            unsafe { std::ptr::copy_nonoverlapping(chunk.as_ptr(), dst.0.add(written), chunk.len()) };
            written += chunk.len();
        }
    }
    Ok(written)
}

/// Read a byte range of a GCS object, returning an awaitable resolving to `bytes`.
///
/// `start` is inclusive and `end` exclusive, matching Python slice semantics;
/// omit both to read the whole object.
///
/// When the exact length is known the data is streamed directly into the
/// destination `bytes` object, avoiding an intermediate `Vec` and its copy.
#[pyfunction]
#[pyo3(signature = (bucket, object, start=None, end=None, generation=None, transport=None))]
fn read_range_async<'py>(
    py: Python<'py>,
    bucket: String,
    object: String,
    start: Option<u64>,
    end: Option<u64>,
    generation: Option<i64>,
    transport: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let exact_len = match (start, end) {
        (Some(s), Some(e)) if e > s && (e - s) as usize <= MAX_PREALLOC => Some((e - s) as usize),
        _ => None,
    };

    let Some(len) = exact_len else {
        return pyo3_async_runtimes::tokio::future_into_py(py, async move {
            read_range_inner(bucket, object, start, end, generation, transport).await
        });
    };

    // Allocate the target `bytes` up front (contents uninitialized until the
    // read fills them) so chunks land directly in Python-owned memory.
    let target: Py<PyAny> = unsafe {
        let raw = pyo3::ffi::PyBytes_FromStringAndSize(
            std::ptr::null(),
            len as pyo3::ffi::Py_ssize_t,
        );
        Bound::from_owned_ptr_or_err(py, raw)?.unbind()
    };
    let dst = BufPtr(unsafe { pyo3::ffi::PyBytes_AsString(target.as_ptr()) as *mut u8 });
    let (start, end) = (start.unwrap(), end.unwrap());

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let written =
            read_range_into(bucket, object, start, end, generation, dst, len, transport).await?;
        Python::attach(|py| {
            if written == len {
                return Ok(target);
            }
            // Short read (e.g. range past EOF): return a right-sized copy so the
            // uninitialized tail of the original allocation is never exposed.
            let filled = unsafe { std::slice::from_raw_parts(dst_ptr(&target), written) };
            Ok(PyBytes::new(py, filled).into_any().unbind())
        })
    })
}

fn dst_ptr(obj: &Py<PyAny>) -> *const u8 {
    unsafe { pyo3::ffi::PyBytes_AsString(obj.as_ptr()) as *const u8 }
}

/// Blocking variant of [`read_range_async`], for callers without an event loop.
#[pyfunction]
#[pyo3(signature = (bucket, object, start=None, end=None, generation=None, transport=None))]
fn read_range(
    py: Python<'_>,
    bucket: String,
    object: String,
    start: Option<u64>,
    end: Option<u64>,
    generation: Option<i64>,
    transport: Option<String>,
) -> PyResult<Py<PyBytes>> {
    let data = py.detach(|| {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(read_range_inner(bucket, object, start, end, generation, transport))
    })?;
    Ok(PyBytes::new(py, &data).into())
}

#[pymodule]
fn gcsfs_rust_backend(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(worker_threads()).enable_all();
    pyo3_async_runtimes::tokio::init(builder);

    m.add_function(wrap_pyfunction!(read_range_async, m)?)?;
    m.add_function(wrap_pyfunction!(read_range, m)?)?;
    Ok(())
}
