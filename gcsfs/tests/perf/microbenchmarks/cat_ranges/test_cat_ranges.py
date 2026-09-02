import logging
import random

import pytest

from gcsfs.tests.perf.microbenchmarks.cat_ranges.configs import (
    get_cat_ranges_benchmark_cases,
)
from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_single_threaded,
)

BENCHMARK_GROUP = "cat_ranges"


def _generate_ranges(
    file_paths,
    file_size_bytes,
    chunk_sizes_bytes,
    num_ranges,
    pattern,
    seed=42,
):
    """Pre-generate paths, starts, and ends lists for cat_ranges benchmarking.

    Supports uniform chunk size (int) or varying chunk sizes (list of ints)
    within the same test case.
    """
    rng = random.Random(seed)
    paths = []
    starts = []
    ends = []

    current_seq_offset = 0
    for i in range(num_ranges):
        # 1. Path allocation (round-robin across available files)
        path = file_paths[i % len(file_paths)]

        # 2. Pick range size for this specific range
        if isinstance(chunk_sizes_bytes, list) and len(chunk_sizes_bytes) > 0:
            range_size = rng.choice(chunk_sizes_bytes)
        else:
            range_size = chunk_sizes_bytes

        if range_size > file_size_bytes:
            raise ValueError(
                f"Range size {range_size} bytes exceeds file size {file_size_bytes} bytes."
            )

        # 3. Determine start and end offsets within file bounds
        max_offset = max(0, file_size_bytes - range_size)
        if pattern == "seq":
            if current_seq_offset > max_offset:
                break
            start = current_seq_offset
            current_seq_offset += range_size
        else:
            start = rng.randint(0, max_offset) if max_offset > 0 else 0

        paths.append(path)
        starts.append(start)
        ends.append(start + range_size)

    return paths, starts, ends


def _cat_ranges_op(gcs, paths, starts, ends, max_gap=None, batch_size=None):
    """Fetch byte ranges from GCS files using cat_ranges."""
    try:
        kwargs = {}
        if max_gap is not None:
            kwargs["max_gap"] = max_gap
        if batch_size is not None:
            kwargs["batch_size"] = batch_size
        return gcs.cat_ranges(paths, starts, ends, **kwargs)
    except Exception as e:
        logging.error(f"Error in cat_ranges: {e}")
        raise


all_benchmark_cases = get_cat_ranges_benchmark_cases()
single_threaded_cases, _, _ = filter_test_cases(all_benchmark_cases)


@pytest.mark.parametrize(
    "gcsfs_benchmark_cat_ranges",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_cat_ranges_single_threaded(benchmark, gcsfs_benchmark_cat_ranges, monitor):
    gcs, file_paths, params = gcsfs_benchmark_cat_ranges

    chunk_sizes = params.chunk_sizes_bytes or params.chunk_size_bytes
    paths, starts, ends = _generate_ranges(
        file_paths,
        params.file_size_bytes,
        chunk_sizes,
        params.num_ranges,
        params.pattern,
    )
    op_args = (gcs, paths, starts, ends, params.max_gap, params.batch_size)

    run_single_threaded(
        benchmark,
        monitor,
        params,
        _cat_ranges_op,
        op_args,
        BENCHMARK_GROUP,
    )
