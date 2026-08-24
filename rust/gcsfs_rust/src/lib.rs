//! PyO3 bindings around the `google-cloud-storage` Rust SDK, exposing a
//! minimal range-read API that gcsfs can use as an alternative I/O backend.
//!
//! Two entry points are exposed:
//! * [`read_range_async`] returns a Python awaitable driven directly by the
//!   caller's asyncio event loop — no thread-pool hop.
//! * [`read_range`] is the blocking equivalent, kept as a fallback for callers
//!   without a running event loop.

use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::OnceCell as AsyncOnceCell;

use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;

static CLIENT: AsyncOnceCell<Storage> = AsyncOnceCell::const_new();

/// Upper bound on speculative preallocation, so a bogus `end` from the caller
/// can't trigger an enormous up-front allocation.
const MAX_PREALLOC: usize = 64 * 1024 * 1024;

const DEFAULT_WORKER_THREADS: usize = 16;

/// Reads are I/O-bound, so a worker per core is wasteful: each extra thread
/// gets its own glibc malloc arena that retains freed read buffers, inflating
/// RSS. Override with `GCSFS_RUST_WORKER_THREADS`.
fn worker_threads() -> usize {
    std::env::var("GCSFS_RUST_WORKER_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_WORKER_THREADS)
}

async fn client() -> PyResult<&'static Storage> {
    CLIENT
        .get_or_try_init(|| async {
            Storage::builder()
                .build()
                .await
                .map_err(|e| PyIOError::new_err(format!("failed to build GCS client: {e}")))
        })
        .await
}

async fn read_range_inner(
    bucket: String,
    object: String,
    start: Option<u64>,
    end: Option<u64>,
    generation: Option<i64>,
) -> PyResult<Vec<u8>> {
    let storage = client().await?;
    let mut builder = storage.read_object(format!("projects/_/buckets/{bucket}"), &object);
    if let Some(generation) = generation {
        builder = builder.set_generation(generation);
    }
    builder = match (start, end) {
        (Some(start), Some(end)) if end > start => {
            builder.set_read_range(ReadRange::segment(start, end - start))
        }
        (Some(start), Some(_)) => builder.set_read_range(ReadRange::segment(start, 0)),
        (Some(start), None) => builder.set_read_range(ReadRange::offset(start)),
        (None, Some(end)) => builder.set_read_range(ReadRange::segment(0, end)),
        (None, None) => builder,
    };

    let mut reader = builder
        .send()
        .await
        .map_err(|e| PyIOError::new_err(format!("GCS read failed for {object}: {e}")))?;

    // Sizing the buffer up front avoids repeated realloc+memcpy as chunks arrive.
    let capacity = match (start, end) {
        (Some(s), Some(e)) if e > s => ((e - s) as usize).min(MAX_PREALLOC),
        _ => 0,
    };
    let mut contents = Vec::with_capacity(capacity);
    while let Some(chunk) = reader
        .next()
        .await
        .transpose()
        .map_err(|e| PyIOError::new_err(format!("GCS read failed for {object}: {e}")))?
    {
        contents.extend_from_slice(&chunk);
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
) -> PyResult<usize> {
    let storage = client().await?;
    let mut builder = storage.read_object(format!("projects/_/buckets/{bucket}"), &object);
    if let Some(generation) = generation {
        builder = builder.set_generation(generation);
    }
    let mut reader = builder
        .set_read_range(ReadRange::segment(start, end - start))
        .send()
        .await
        .map_err(|e| PyIOError::new_err(format!("GCS read failed for {object}: {e}")))?;

    let mut written = 0usize;
    while let Some(chunk) = reader
        .next()
        .await
        .transpose()
        .map_err(|e| PyIOError::new_err(format!("GCS read failed for {object}: {e}")))?
    {
        // The service should never exceed the requested range; refuse rather
        // than overrun the Python-owned allocation if it ever does.
        if written + chunk.len() > cap {
            return Err(PyIOError::new_err(format!(
                "GCS returned more data than requested for {object}: {} > {cap}",
                written + chunk.len()
            )));
        }
        unsafe { std::ptr::copy_nonoverlapping(chunk.as_ptr(), dst.0.add(written), chunk.len()) };
        written += chunk.len();
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
#[pyo3(signature = (bucket, object, start=None, end=None, generation=None))]
fn read_range_async<'py>(
    py: Python<'py>,
    bucket: String,
    object: String,
    start: Option<u64>,
    end: Option<u64>,
    generation: Option<i64>,
) -> PyResult<Bound<'py, PyAny>> {
    let exact_len = match (start, end) {
        (Some(s), Some(e)) if e > s && (e - s) as usize <= MAX_PREALLOC => Some((e - s) as usize),
        _ => None,
    };

    let Some(len) = exact_len else {
        return pyo3_async_runtimes::tokio::future_into_py(py, async move {
            read_range_inner(bucket, object, start, end, generation).await
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
            read_range_into(bucket, object, start, end, generation, dst, len).await?;
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
#[pyo3(signature = (bucket, object, start=None, end=None, generation=None))]
fn read_range(
    py: Python<'_>,
    bucket: String,
    object: String,
    start: Option<u64>,
    end: Option<u64>,
    generation: Option<i64>,
) -> PyResult<Py<PyBytes>> {
    let data = py.detach(|| {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(read_range_inner(bucket, object, start, end, generation))
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
