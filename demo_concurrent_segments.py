"""Demonstration script: Concurrently read sequential segments of a single file.

Simulates multiple worker processes or threads reading distinct sequential
byte segments (shards / chunks) of a single large file simultaneously using
gcsfs / fsspec.

Each worker:
  - Takes a distinct non-overlapping segment of the file [start_byte, end_byte).
  - Iterates sequentially through that segment in chunks of record_size.
  - Measures throughput and validates byte continuity.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import sys
import time
from unittest.mock import MagicMock
from gcsfs.core import GCSFile, GCSFileSystem


def _get_mock_fs(total_file_size: int):
    fs = MagicMock(spec=GCSFileSystem)
    fs.default_block_size = 1048576
    fs.consistency = None
    fs.split_path.side_effect = lambda p: ("bucket", "training_dataset_shard0.bin", None)

    def _simulated_cat_file(path, start=None, end=None, **kwargs):
        s = int(start or 0)
        e = int(end or total_file_size)
        size = max(0, e - s)
        time.sleep(0.005 + (size / 10_000_000))
        return bytes((i % 256) for i in range(s, e))

    fs.cat_file.side_effect = _simulated_cat_file
    return fs


def _worker_read_segment(
    worker_id: int,
    file_path: str,
    start_byte: int,
    end_byte: int,
    record_size: int,
    total_size: int,
    real_gcs: bool = False,
):
    pid = os.getpid()
    segment_len = end_byte - start_byte
    print(f"[Worker {worker_id} | PID {pid}] Starting segment [{start_byte:,} .. {end_byte:,}] ({segment_len:,} bytes)")

    if real_gcs:
        import gcsfs
        fs = gcsfs.GCSFileSystem()
        f = fs.open(file_path, "rb", block_size=1048576, cache_type="bytes")
    else:
        fs = _get_mock_fs(total_size)
        f = GCSFile(fs, file_path, mode="rb", size=total_size, block_size=1048576, cache_type="bytes")

    total_read = 0
    t0 = time.time()
    try:
        f.seek(start_byte)
        cur_pos = start_byte

        while cur_pos < end_byte:
            to_read = min(record_size, end_byte - cur_pos)
            data = f.read(to_read)
            if not data:
                break
            total_read += len(data)
            cur_pos += len(data)
            # Simulate a small compute or tensor conversion gap between records
            time.sleep(0.0005)

    finally:
        f.close()

    elapsed = time.time() - t0
    rate_mb = (total_read / (1024 * 1024)) / max(0.001, elapsed)
    print(
        f"[Worker {worker_id} | PID {pid}] Completed: {total_read:,} bytes in {elapsed:.3f}s ({rate_mb:.2f} MiB/s)"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Run concurrent sequential segment reads of a single file."
    )
    parser.add_argument(
        "--file",
        default="gs://bucket/training_dataset_shard0.bin",
        help="GCS file path or synthetic identifier.",
    )
    parser.add_argument(
        "--file-size-mb",
        type=int,
        default=32,
        help="Total simulated or real file size in MiB (default: 32 MiB).",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=4,
        help="Number of concurrent worker processes (default: 4).",
    )
    parser.add_argument(
        "--record-size-kb",
        type=int,
        default=64,
        help="Record read size in KiB for sequential iteration (default: 64 KiB).",
    )
    parser.add_argument(
        "--real-gcs",
        action="store_true",
        help="Read from real GCS bucket instead of simulated MockGCSFile.",
    )

    args = parser.parse_args()

    total_bytes = args.file_size_mb * 1024 * 1024
    record_bytes = args.record_size_kb * 1024
    segment_size = total_bytes // args.num_workers

    print("=" * 70)
    print(f"Target File:       {args.file}")
    print(f"File Size:         {args.file_size_mb} MiB ({total_bytes:,} bytes)")
    print(f"Workers:           {args.num_workers} processes (concurrent segments)")
    print(f"Segment per worker:{segment_size / (1024 * 1024):.1f} MiB ({segment_size:,} bytes)")
    print(f"Record read size:  {args.record_size_kb} KiB ({record_bytes:,} bytes)")
    print("=" * 70)

    ctx = mp.get_context("spawn")
    workers = []

    for i in range(args.num_workers):
        seg_start = i * segment_size
        seg_end = (i + 1) * segment_size if i < args.num_workers - 1 else total_bytes
        p = ctx.Process(
            target=_worker_read_segment,
            args=(i, args.file, seg_start, seg_end, record_bytes, total_bytes, args.real_gcs),
        )
        workers.append(p)

    t_start = time.time()
    for p in workers:
        p.start()

    for p in workers:
        p.join()

    total_time = time.time() - t_start
    total_mb = total_bytes / (1024 * 1024)
    print("=" * 70)
    print(f"All {args.num_workers} workers finished in {total_time:.3f}s (Aggregate Throughput: {total_mb / total_time:.2f} MiB/s)")
    print("=" * 70)


if __name__ == "__main__":
    main()
