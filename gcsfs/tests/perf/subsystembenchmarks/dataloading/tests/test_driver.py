import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading import driver


def test_timestamp_is_monotonic():
    a = driver.timestamp()
    b = driver.timestamp()
    assert b >= a


def test_reduce_split_spans_rank_wall_time_and_global_first_batch_readiness():
    results = [
        ([(0.0, 2.0, 10)], 0.5),
        ([(1.0, 4.0, 5)], 0.9),
    ]
    durations, rows, ttfb = driver.reduce_split(results, rounds=1)
    assert durations == [4.0]
    assert rows == [15]
    assert ttfb == 1.9


def test_assert_fsspec_gcsfs_ignores_non_gs_prefix():
    driver.assert_fsspec_gcsfs("file:///tmp/x")


def test_assert_gcsfs_backed_rejects_non_pyfilesystem():
    with pytest.raises(AssertionError, match="PyFileSystem"):
        driver.assert_gcsfs_backed(object())


def test_read_driver_protocol_is_runtime_checkable():
    class Good:
        formats = ("pretok_parquet",)

        def run_read(self, prefix, params):
            return driver.ReadResult(
                durations=[1.0], rows_per_epoch=[1], ttfb_seconds=0.0, build_seconds=0.0
            )

    assert isinstance(Good(), driver.ReadDriver)


def test_read_result_extra_columns_default_empty():
    r = driver.ReadResult([1.0], [1], 0.0, 0.0)
    assert r.extra_columns == {}
