//! Standalone benchmark: read a GCS object in N parallel range requests using
//! only the Rust google-cloud-storage SDK, with no Python/gcsfs in the loop.
//!
//! Usage:
//!   bench_rust_read <bucket> <object> <size_bytes> <parallelism>
//!
//! Example:
//!   bench_rust_read princer-bucket 10gfile.bin 10737418240 16
//! High-performance benchmark for Cloud-Path gRPC vs HTTP JSON on Google Cloud Storage.

use std::collections::BTreeMap;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;

#[derive(Clone, Debug)]
struct BenchConfig {
    bucket: String,
    object: String,
    size_bytes: u64,
    transport: String,     // "grpc" or "http"
    num_channels: usize,   // 1, 2, 4, 8, 16, 32, 64
    concurrency: usize,    // parallel worker tasks (e.g. 16, 32, 64)
    chunk_size: u64,       // range chunk size (e.g. 16 MiB = 16777216)
    object_pattern: String, // e.g. "10gfile.bin" or "test_10g/file_{}.bin"
    num_files: usize,       // number of distinct 10 GiB files to read concurrently
    size_per_file: u64,
    transport: String,      // "grpc" or "http"
    num_channels: usize,    // e.g. 48
    concurrency_per_file: usize, // e.g. 1
    chunk_size: u64,        // 16 MiB = 16777216
    json_output: bool,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            bucket: "princer-bucket".to_string(),
            object: "10gfile.bin".to_string(),
            size_bytes: 10 * 1024 * 1024 * 1024, // 10 GiB
            object_pattern: "10gfile.bin".to_string(),
            num_files: 1,
            size_per_file: 10 * 1024 * 1024 * 1024, // 10 GiB
            transport: "grpc".to_string(),
            num_channels: 1,
            concurrency: 16,
            num_channels: 48,
            concurrency_per_file: 1,
            chunk_size: 16 * 1024 * 1024, // 16 MiB
            json_output: false,
        }
    }
}

fn parse_args() -> BenchConfig {
    let mut config = BenchConfig::default();
    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--bucket" => {
                i += 1;
                if i < args.len() {
                    config.bucket = args[i].clone();
                }
            }
            "--object" => {
            "--object" | "--url-pattern" => {
                i += 1;
                if i < args.len() {
                    config.object = args[i].clone();
                    config.object_pattern = args[i].clone();
                }
            }
            "--files" | "--num-files" => {
                i += 1;
                if i < args.len() {
                    config.num_files = args[i].parse().unwrap_or(config.num_files);
                }
            }
            "--size" | "--size-bytes" => {
                i += 1;
                if i < args.len() {
                    config.size_bytes = args[i].parse().unwrap_or(config.size_bytes);
                    config.size_per_file = args[i].parse().unwrap_or(config.size_per_file);
                }
            }
            "--transport" => {
                i += 1;
                if i < args.len() {
                    config.transport = args[i].to_lowercase();
                }
            }
            "--channels" | "--grpc-channels" => {
                i += 1;
                if i < args.len() {
                    config.num_channels = args[i].parse().unwrap_or(config.num_channels);
                }
            }
            "--concurrency" | "--parallelism" | "-c" => {
                i += 1;
                if i < args.len() {
                    config.concurrency = args[i].parse().unwrap_or(config.concurrency);
                    config.concurrency_per_file = args[i].parse().unwrap_or(config.concurrency_per_file);
                }
            }
            "--chunk-size" | "--io-size" => {
                i += 1;
                if i < args.len() {
                    config.chunk_size = args[i].parse().unwrap_or(config.chunk_size);
                }
            }
            "--json" => {
                config.json_output = true;
            }
            "--help" | "-h" => {
                eprintln!(
                    "Usage: bench_rust_read [OPTIONS]\n\n\
                    Options:\n  \
                      --bucket <NAME>        GCS bucket [default: princer-bucket]\n  \
                      --object <PATH>        Object path [default: 10gfile.bin]\n  \
                      --size <BYTES>         Total bytes to read [default: 10737418240 (10 GiB)]\n  \
                      --object <PATTERN>     Object path pattern [default: 10gfile.bin]\n  \
                      --files <N>            Number of files to read [default: 1]\n  \
                      --size <BYTES>         Bytes per file [default: 10737418240 (10 GiB)]\n  \
                      --transport <grpc|http> Transport backend [default: grpc]\n  \
                      --channels <N>         Number of gRPC client channels [default: 1]\n  \
                      --concurrency <N>      Parallel range reader tasks [default: 16]\n  \
                      --chunk-size <BYTES>   Size of each range request [default: 16777216 (16 MiB)]\n  \
                      --channels <N>         Number of gRPC client channels [default: 48]\n  \
                      --concurrency <N>      Parallel readers per file [default: 1]\n  \
                      --chunk-size <BYTES>   Size of each range request [default: 16777216]\n  \
                      --json                 Output metrics in JSON format\n"
                );
                std::process::exit(0);
            }
            _ => {
                if i == 1 && !args[i].starts_with('-') {
                    config.bucket = args[i].clone();
                } else if i == 2 && !args[i].starts_with('-') {
                    config.object = args[i].clone();
                    config.object_pattern = args[i].clone();
                } else if i == 3 && !args[i].starts_with('-') {
                    config.size_bytes = args[i].parse().unwrap_or(config.size_bytes);
                    config.size_per_file = args[i].parse().unwrap_or(config.size_per_file);
                } else if i == 4 && !args[i].starts_with('-') {
                    config.concurrency = args[i].parse().unwrap_or(config.concurrency);
                    config.concurrency_per_file = args[i].parse().unwrap_or(config.concurrency_per_file);
                } else if i == 5 && !args[i].starts_with('-') {
                    config.transport = args[i].to_lowercase();
                } else if i == 6 && !args[i].starts_with('-') {
                    config.num_channels = args[i].parse().unwrap_or(config.num_channels);
                }
            }
        }
        i += 1;
    }
    config
}

fn get_peak_rss_mb() -> f64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmHWM:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(kb) = parts[1].parse::<f64>() {
                            return kb / 1024.0;
                        }
                    }
                }
            }
fn get_cpu_and_rss() -> (f64, f64, f64) {
    // Returns (user_time_sec, sys_time_sec, max_rss_mb)
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0 {
            let user = usage.ru_utime.tv_sec as f64 + (usage.ru_utime.tv_usec as f64 / 1_000_000.0);
            let sys = usage.ru_stime.tv_sec as f64 + (usage.ru_stime.tv_usec as f64 / 1_000_000.0);
            let rss_mb = usage.ru_maxrss as f64 / 1024.0;
            return (user, sys, rss_mb);
        }
    }
    0.0
    (0.0, 0.0, 0.0)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 5 {
        eprintln!(
            "usage: {} <bucket> <object> <size_bytes> <parallelism>",
            args[0]
        );
        std::process::exit(1);
    let config = parse_args();
    let bucket_path = format!("projects/_/buckets/{}", config.bucket);
    let total_expected_bytes = config.size_per_file * (config.num_files as u64);

    if !config.json_output {
        println!("============================================================");
        println!(" GCS Read Benchmark: {} | {} Channels", config.transport.to_uppercase(), config.num_channels);
        println!(" Target: gs://{}/{} ({:.2} GiB)", config.bucket, config.object, config.size_bytes as f64 / (1024.0 * 1024.0 * 1024.0));
        println!(" Concurrency: {} tasks | Chunk Size: {:.1} MiB", config.concurrency, config.chunk_size as f64 / (1024.0 * 1024.0));
        println!("============================================================");
    }
    let bucket = args[1].clone();
    let object = args[2].clone();
    let size: u64 = args[3].parse()?;
    let parallelism: u64 = args[4].parse()?;
    let (user_start, sys_start, _) = get_cpu_and_rss();
    let t_init = Instant::now();

    let bucket_path = format!("projects/_/buckets/{bucket}");
    let client = Arc::new(Storage::builder().build().await?);
    let t_init = Instant::now();
    let mut clients = Vec::with_capacity(config.num_channels);
    for _ in 0..config.num_channels {
        clients.push(Arc::new(Storage::builder().build().await?));
    }
    let init_duration = t_init.elapsed();

    let chunk = size.div_ceil(parallelism);
    let total_bytes = Arc::new(AtomicU64::new(0));
    // Prepare range chunks: list of (offset, length)
    let mut ranges = Vec::new();
    let mut offset = 0u64;
    while offset < config.size_bytes {
        let len = config.chunk_size.min(config.size_bytes - offset);
        ranges.push((offset, len));
        offset += len;
    // Prepare files to read
    let mut file_objects = Vec::new();
    for idx in 0..config.num_files {
        let obj_name = if config.object_pattern.contains("{}") {
            config.object_pattern.replace("{}", &idx.to_string())
        } else if config.num_files > 1 {
            format!("test_10g/file_{idx}.bin")
        } else {
            config.object_pattern.clone()
        };
        file_objects.push(obj_name);
    }
    let total_chunks = ranges.len();
    let next_chunk_idx = Arc::new(AtomicUsize::new(0));
    let shared_ranges = Arc::new(ranges);

    let start = Instant::now();
    let mut tasks = Vec::with_capacity(parallelism as usize);
    for i in 0..parallelism {
        let range_start = i * chunk;
        if range_start >= size {
            break;
    if !config.json_output {
        println!("============================================================");
        println!(" 16MB IO Benchmark: {} | {} Channels | {} Worker Files", config.transport.to_uppercase(), config.num_channels, config.num_files);
        println!(" Total Target Size: {:.2} GiB ({} x {:.2} GiB)", total_expected_bytes as f64 / (1024.0 * 1024.0 * 1024.0), config.num_files, config.size_per_file as f64 / (1024.0 * 1024.0 * 1024.0));
        println!(" Chunk Size: {:.1} MiB | Concurrency/file: {}", config.chunk_size as f64 / (1024.0 * 1024.0), config.concurrency_per_file);
        println!("============================================================");
    }

    let total_bytes_read = Arc::new(AtomicU64::new(0));
    let channel_bytes: Vec<Arc<AtomicU64>> = (0..config.num_channels)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    // Benchmarking start
    let start_instant = Instant::now();
    let mut worker_handles = Vec::with_capacity(config.concurrency);
    let mut file_handles = Vec::with_capacity(config.num_files);

    if config.transport == "grpc" {
        // gRPC Cloud-Path via open_object (BidiReadObject)
        let mut descriptors = Vec::with_capacity(config.num_channels);
        for client in &clients {
            let desc = client.open_object(&bucket_path, &config.object).send().await?;
            descriptors.push(Arc::new(desc));
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

        for worker_id in 0..config.concurrency {
            let next_chunk = Arc::clone(&next_chunk_idx);
            let ranges_ref = Arc::clone(&shared_ranges);
        // gRPC Cloud-Path
        // gRPC Cloud-Path: distribute range chunks across all channels for each file
        for (f_idx, obj_name) in file_objects.into_iter().enumerate() {
            let clients_ref = clients.clone();
            let b_path = bucket_path.clone();
            let total_bytes = Arc::clone(&total_bytes_read);
            let descriptors_ref = descriptors.clone();
            let channel_bytes_ref = channel_bytes.clone();
            let size = config.size_per_file;
            let chunk_size = config.chunk_size;
            let conc = config.concurrency_per_file;
            let num_chans = config.num_channels;

            worker_handles.push(tokio::spawn(async move {
                let mut local_bytes = 0u64;
                loop {
                    let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                    if idx >= ranges_ref.len() {
                        break;
                    }
                    let (range_start, range_len) = ranges_ref[idx];
                    let chan_idx = (worker_id + idx) % num_chans;
                    let desc = &descriptors_ref[chan_idx];
            file_handles.push(tokio::spawn(async move {
                // Open descriptors across channels for this file
                let mut descriptors = Vec::with_capacity(num_chans);
                for client in &clients_ref {
                    let desc = client.open_object(&b_path, &obj_name).send().await?;
                    descriptors.push(Arc::new(desc));
                }

                    let mut reader = desc.read_range(ReadRange::segment(range_start, range_len)).await;
                    let mut chunk_bytes = 0u64;
                    while let Some(chunk) = reader.next().await.transpose()? {
                        let len = chunk.len() as u64;
                        chunk_bytes += len;
                    }
                    local_bytes += chunk_bytes;
                    total_bytes.fetch_add(chunk_bytes, Ordering::Relaxed);
                    channel_bytes_ref[chan_idx].fetch_add(chunk_bytes, Ordering::Relaxed);
                let mut ranges = Vec::new();
                let mut offset = 0u64;
                while offset < size {
                    let len = chunk_size.min(size - offset);
                    ranges.push((offset, len));
                    offset += len;
                }
                Ok::<u64, anyhow::Error>(local_bytes)
                let shared_ranges = Arc::new(ranges);
                let next_idx = Arc::new(AtomicUsize::new(0));

                let mut tasks = Vec::with_capacity(conc);
                for worker_id in 0..conc {
                    let next = Arc::clone(&next_idx);
                    let ranges_ref = Arc::clone(&shared_ranges);
                    let desc_pool = descriptors.clone();
                    let t_bytes = Arc::clone(&total_bytes);
                    let c_bytes_ref = channel_bytes_ref.clone();

                    tasks.push(tokio::spawn(async move {
                        loop {
                            let idx = next.fetch_add(1, Ordering::Relaxed);
                            if idx >= ranges_ref.len() {
                                break;
                            }
                            let (range_start, range_len) = ranges_ref[idx];
                            let chan_idx = (f_idx + worker_id + idx) % num_chans;
                            let d = &desc_pool[chan_idx];

                            let mut reader = d.read_range(ReadRange::segment(range_start, range_len)).await;
                            let mut n = 0u64;
                            while let Some(chunk) = reader.next().await.transpose()? {
                                n += chunk.len() as u64;
                            }
                            t_bytes.fetch_add(n, Ordering::Relaxed);
                            c_bytes_ref[chan_idx].fetch_add(n, Ordering::Relaxed);
                        }
                        Ok::<(), anyhow::Error>(())
                    }));
                }
                for t in tasks {
                    t.await??;
                }
                Ok::<(), anyhow::Error>(())
            }));
        }
    } else {
        // HTTP REST/JSON via read_object
        for worker_id in 0..config.concurrency {
            let next_chunk = Arc::clone(&next_chunk_idx);
            let ranges_ref = Arc::clone(&shared_ranges);
        // HTTP REST/JSON
        for (f_idx, obj_name) in file_objects.into_iter().enumerate() {
            let clients_ref = clients.clone();
            let b_path = bucket_path.clone();
            let total_bytes = Arc::clone(&total_bytes_read);
            let clients_ref = clients.clone();
            let channel_bytes_ref = channel_bytes.clone();
            let b_path = bucket_path.clone();
            let obj = config.object.clone();
            let size = config.size_per_file;
            let chunk_size = config.chunk_size;
            let conc = config.concurrency_per_file;
            let num_chans = config.num_channels;

            worker_handles.push(tokio::spawn(async move {
                let mut local_bytes = 0u64;
                loop {
                    let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                    if idx >= ranges_ref.len() {
                        break;
                    }
                    let (range_start, range_len) = ranges_ref[idx];
                    let chan_idx = (worker_id + idx) % num_chans;
                    let client = &clients_ref[chan_idx];
            file_handles.push(tokio::spawn(async move {
                let mut ranges = Vec::new();
                let mut offset = 0u64;
                while offset < size {
                    let len = chunk_size.min(size - offset);
                    ranges.push((offset, len));
                    offset += len;
                }
                let shared_ranges = Arc::new(ranges);
                let next_idx = Arc::new(AtomicUsize::new(0));

                    let mut reader = client
                        .read_object(&b_path, &obj)
                        .set_read_range(ReadRange::segment(range_start, range_len))
                        .send()
                        .await?;
                    let mut chunk_bytes = 0u64;
                    while let Some(chunk) = reader.next().await.transpose()? {
                        let len = chunk.len() as u64;
                        chunk_bytes += len;
                    }
                    local_bytes += chunk_bytes;
                    total_bytes.fetch_add(chunk_bytes, Ordering::Relaxed);
                    channel_bytes_ref[chan_idx].fetch_add(chunk_bytes, Ordering::Relaxed);
                let mut tasks = Vec::with_capacity(conc);
                for worker_id in 0..conc {
                    let next = Arc::clone(&next_idx);
                    let ranges_ref = Arc::clone(&shared_ranges);
                    let client_pool = clients_ref.clone();
                    let bp = b_path.clone();
                    let obj = obj_name.clone();
                    let t_bytes = Arc::clone(&total_bytes);
                    let c_bytes_ref = channel_bytes_ref.clone();

                    tasks.push(tokio::spawn(async move {
                        loop {
                            let idx = next.fetch_add(1, Ordering::Relaxed);
                            if idx >= ranges_ref.len() {
                                break;
                            }
                            let (range_start, range_len) = ranges_ref[idx];
                            let chan_idx = (f_idx + worker_id + idx) % num_chans;
                            let cl = &client_pool[chan_idx];

                            let mut reader = cl
                                .read_object(&bp, &obj)
                                .set_read_range(ReadRange::segment(range_start, range_len))
                                .send()
                                .await?;
                            let mut n = 0u64;
                            while let Some(chunk) = reader.next().await.transpose()? {
                                n += chunk.len() as u64;
                            }
                            t_bytes.fetch_add(n, Ordering::Relaxed);
                            c_bytes_ref[chan_idx].fetch_add(n, Ordering::Relaxed);
                        }
                        Ok::<(), anyhow::Error>(())
                    }));
                }
                Ok::<u64, anyhow::Error>(local_bytes)
                for t in tasks {
                    t.await??;
                }
                Ok::<(), anyhow::Error>(())
            }));
        }
    }

    for task in tasks {
        task.await??;
    for handle in worker_handles {
    for handle in file_handles {
        handle.await??;
    }
    let elapsed = start.elapsed();
    let elapsed = start_instant.elapsed();
    let (user_end, sys_end, peak_rss_mb) = get_cpu_and_rss();

    let bytes = total_bytes.load(Ordering::Relaxed);
    let mb = bytes as f64 / 1024.0 / 1024.0;
    let secs = elapsed.as_secs_f64();
    println!(
        "Read {bytes} bytes ({parallelism} parallel range reads) in {secs:.2}s, throughput: {:.2} MB/second",
        mb / secs
    );
    let bytes = total_bytes_read.load(Ordering::Relaxed);
    let total_secs = elapsed.as_secs_f64();
    let mb_read = bytes as f64 / (1024.0 * 1024.0);
    let gib_read = bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    let throughput_mbps = mb_read / total_secs;
    let throughput_mbps = (bytes as f64 / (1024.0 * 1024.0)) / total_secs;
    let throughput_gbps = (bytes as f64 * 8.0) / (total_secs * 1_000_000_000.0);
    let peak_rss_mb = get_peak_rss_mb();

    let user_time = (user_end - user_start).max(0.0);
    let sys_time = (sys_end - sys_start).max(0.0);
    let total_cpu_time = user_time + sys_time;
    let cpu_percent = (total_cpu_time / total_secs) * 100.0;
    let cpu_s_per_gib = if gib_read > 0.0 { total_cpu_time / gib_read } else { 0.0 };

    if config.json_output {
        let mut per_channel_mbps = BTreeMap::new();
        for (i, cb) in channel_bytes.iter().enumerate() {
            let c_bytes = cb.load(Ordering::Relaxed);
            let c_mb = c_bytes as f64 / (1024.0 * 1024.0);
            per_channel_mbps.insert(format!("channel_{i}"), c_mb / total_secs);
        }

        println!(
            "{{\n  \
              \"transport\": \"{}\",\n  \
              \"channels\": {},\n  \
              \"concurrency\": {},\n  \
              \"chunk_size_mb\": {:.1},\n  \
              \"total_chunks\": {},\n  \
              \"num_files\": {},\n  \
              \"bytes_read\": {},\n  \
              \"gib_read\": {:.3},\n  \
              \"elapsed_secs\": {:.3},\n  \
              \"throughput_mbps\": {:.2},\n  \
              \"throughput_gbps\": {:.2},\n  \
              \"peak_rss_mb\": {:.1}\n\
              \"peak_rss_mb\": {:.1},\n  \
              \"user_cpu_s\": {:.2},\n  \
              \"sys_cpu_s\": {:.2},\n  \
              \"total_cpu_s\": {:.2},\n  \
              \"cpu_percent\": {:.1},\n  \
              \"cpu_s_per_gib\": {:.2}\n\
            }}",
            config.transport,
            config.num_channels,
            config.concurrency,
            config.chunk_size as f64 / (1024.0 * 1024.0),
            total_chunks,
            config.num_files,
            bytes,
            gib_read,
            total_secs,
            throughput_mbps,
            throughput_gbps,
            peak_rss_mb
            peak_rss_mb,
            user_time,
            sys_time,
            total_cpu_time,
            cpu_percent,
            cpu_s_per_gib
        );
    } else {
        println!("\nBenchmark Results:");
        println!("  Total Data Read    : {:.2} GiB ({} bytes in {} chunks)", gib_read, bytes, total_chunks);
        println!("\nBenchmark Results (16MB IO):");
        println!("  Workers / Files    : {} files (10 GiB each)", config.num_files);
        println!("  Total Data Read    : {:.2} GiB ({} bytes)", gib_read, bytes);
        println!("  Elapsed Time       : {:.2} seconds (init: {:.2}s)", total_secs, init_duration.as_secs_f64());
        println!("  Aggregate Throughput: \x1b[1;32m{:.2} MB/s\x1b[0m ({:.2} Gbps)", throughput_mbps, throughput_gbps);
        println!("  Peak RSS Memory    : {:.1} MB", peak_rss_mb);
        println!("------------------------------------------------------------");
        if config.num_channels > 1 {
            println!("  Per-Channel Distribution:");
            for (i, cb) in channel_bytes.iter().enumerate() {
                let c_bytes = cb.load(Ordering::Relaxed);
                let c_gib = c_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                let c_mbps = (c_bytes as f64 / (1024.0 * 1024.0)) / total_secs;
                println!("    Channel {:2}: {:5.2} GiB ({:7.2} MB/s | {:4.1}%)", i, c_gib, c_mbps, (c_bytes as f64 / bytes as f64) * 100.0);
            }
        }
        println!("  Peak RSS Memory    : {:.1} MB (per file: {:.1} MB)", peak_rss_mb, peak_rss_mb / config.num_files as f64);
        println!("  CPU Utilization    : {:.1}% (User: {:.2}s, Sys: {:.2}s, Total: {:.2}s)", cpu_percent, user_time, sys_time, total_cpu_time);
        println!("  CPU Cost per GiB   : \x1b[1;33m{:.2} CPU-s / GiB\x1b[0m", cpu_s_per_gib);
        println!("============================================================\n");
    }

    Ok(())
}
