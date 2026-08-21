//! PyO3 bindings around the `google-cloud-storage` Rust SDK, exposing a
//! minimal range-read API that gcsfs can use as an alternative I/O backend.

use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::OnceLock;
use tokio::runtime::Runtime;
use tokio::sync::OnceCell as AsyncOnceCell;

use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();
static CLIENT: AsyncOnceCell<Storage> = AsyncOnceCell::const_new();

fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| Runtime::new().expect("failed to create tokio runtime"))
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

/// Read a (possibly partial) range of bytes from a GCS object.
///
/// `start`/`end` follow Python slice semantics: `start` is inclusive, `end`
/// is exclusive. Omit both to read the whole object.
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
    let bucket_path = format!("projects/_/buckets/{bucket}");
    let object_name = object.clone();

    let data: Vec<u8> = py.detach(|| {
        runtime().block_on(async move {
            let storage = client().await?;
            let mut builder = storage.read_object(bucket_path, object);
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
                .map_err(|e| PyIOError::new_err(format!("GCS read failed for {object_name}: {e}")))?;

            let mut contents = Vec::new();
            while let Some(chunk) = reader
                .next()
                .await
                .transpose()
                .map_err(|e| PyIOError::new_err(format!("GCS read failed: {e}")))?
            {
                contents.extend_from_slice(&chunk);
            }
            Ok::<Vec<u8>, PyErr>(contents)
        })
    })?;

    Ok(PyBytes::new(py, &data).into())
}

#[pymodule]
fn gcsfs_rust_backend(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_range, m)?)?;
    Ok(())
}
