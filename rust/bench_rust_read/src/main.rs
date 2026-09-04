//! Standalone benchmark: read a GCS object in N parallel range requests using
//! only the Rust google-cloud-storage SDK, with no Python/gcsfs in the loop.
//!
//! Usage:
//!   bench_rust_read <bucket> <object> <size_bytes> <parallelism>
//!
//! Example:
//!   bench_rust_read princer-bucket 10gfile.bin 10737418240 16

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 5 {
        eprintln!(
            "usage: {} <bucket> <object> <size_bytes> <parallelism>",
            args[0]
        );
        std::process::exit(1);
    }
    let bucket = args[1].clone();
    let object = args[2].clone();
    let size: u64 = args[3].parse()?;
    let parallelism: u64 = args[4].parse()?;

    let bucket_path = format!("projects/_/buckets/{bucket}");
    let client = Arc::new(Storage::builder().build().await?);

    let chunk = size.div_ceil(parallelism);
    let total_bytes = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut tasks = Vec::with_capacity(parallelism as usize);
    for i in 0..parallelism {
        let range_start = i * chunk;
        if range_start >= size {
            break;
        }
        let range_len = chunk.min(size - range_start);
        let client = Arc::clone(&client);
        let bucket_path = bucket_path.clone();
        let object = object.clone();
        let total_bytes = Arc::clone(&total_bytes);
        tasks.push(tokio::spawn(async move {
            let mut reader = client
                .read_object(bucket_path, object)
                .set_read_range(ReadRange::segment(range_start, range_len))
                .send()
                .await?;
            let mut n = 0u64;
            while let Some(chunk) = reader.next().await.transpose()? {
                n += chunk.len() as u64;
            }
            total_bytes.fetch_add(n, Ordering::Relaxed);
            Ok::<(), anyhow::Error>(())
        }));
    }

    for task in tasks {
        task.await??;
    }
    let elapsed = start.elapsed();

    let bytes = total_bytes.load(Ordering::Relaxed);
    let mb = bytes as f64 / 1024.0 / 1024.0;
    let secs = elapsed.as_secs_f64();
    println!(
        "Read {bytes} bytes ({parallelism} parallel range reads) in {secs:.2}s, throughput: {:.2} MB/second",
        mb / secs
    );

    Ok(())
}
