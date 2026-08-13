import math
from types import SimpleNamespace

import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading import driver


class _Barrier:
    def __init__(self):
        self.calls = []

    def wait(self, timeout):
        self.calls.append(timeout)


def test_measure_epochs_times_rows_and_runs_epoch_setup(monkeypatch):
    clock = iter([10.0, 10.25, 11.0, 20.0, 21.0])
    monkeypatch.setattr(driver, "timestamp", lambda: next(clock))
    barrier = _Barrier()
    epochs = []

    per_epoch, ttfb = driver.measure_epochs(
        [[1, 2], [3]],
        2,
        len,
        barrier=barrier,
        on_epoch=epochs.append,
    )

    assert per_epoch == [(10.0, 11.0, 3), (20.0, 21.0, 3)]
    assert ttfb == 0.25
    assert epochs == [0, 1]
    assert barrier.calls == [driver.ROUND_BARRIER_TIMEOUT_SECONDS] * 2


def test_measure_epochs_trims_row_count_at_target(monkeypatch):
    clock = iter([1.0, 1.1, 2.0])
    monkeypatch.setattr(driver, "timestamp", lambda: next(clock))

    per_epoch, _ = driver.measure_epochs([[0] * 8, [0] * 8], 1, len, target=10)

    assert per_epoch == [(1.0, 2.0, 10)]


def test_measure_epochs_reads_nothing_at_a_zero_target(monkeypatch):
    """Verify a rank with zero target budget fetches no batches."""
    clock = iter([1.0, 2.0])
    monkeypatch.setattr(driver, "timestamp", lambda: next(clock))
    pulled = []

    def loader():
        pulled.append(1)
        yield [0] * 8

    per_epoch, ttfb = driver.measure_epochs(loader(), 1, len, target=0)

    assert per_epoch == [(1.0, 2.0, 0)]
    assert math.isinf(ttfb)
    assert pulled == [], "a zero-target rank pulled a batch anyway"


def test_measure_epochs_reports_infinite_ttfb_for_an_empty_epoch(monkeypatch):
    clock = iter([1.0, 2.0])
    monkeypatch.setattr(driver, "timestamp", lambda: next(clock))

    per_epoch, ttfb = driver.measure_epochs([], 1, len)

    assert per_epoch == [(1.0, 2.0, 0)]
    assert math.isinf(ttfb)


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
    pytest.importorskip("pyarrow")
    with pytest.raises(AssertionError, match="PyFileSystem"):
        driver.assert_gcsfs_backed(object())


# Module-level probe functions for mp.spawn pickling.


def _asymmetric_round_probe(rank, barrier, queue):
    import time

    for round_index in range(3):
        driver.await_round(barrier)
        begin = driver.timestamp()
        # Vary rank sleep durations to test barrier synchronization across rounds.
        time.sleep(0.05 * (rank + 1))
        queue.put((round_index, begin, driver.timestamp()))


def test_round_barrier_keeps_every_rank_inside_its_own_round():
    mp = pytest.importorskip("torch.multiprocessing")

    world_size = 3
    ctx = mp.get_context("spawn")
    with ctx.Manager() as manager:
        queue = manager.Queue()
        mp.spawn(
            _asymmetric_round_probe,
            args=(driver.round_barrier(ctx, world_size), queue),
            nprocs=world_size,
            join=True,
        )
        marks = [queue.get() for _ in range(world_size * 3)]

    for round_index in range(2):
        latest_end = max(end for r, _, end in marks if r == round_index)
        earliest_begin = min(begin for r, begin, _ in marks if r == round_index + 1)
        assert (
            earliest_begin >= latest_end
        ), f"round {round_index + 1} started before round {round_index} finished"


def _rank_result_probe(rank, world_size, prefix, params, barrier, queue):
    assert world_size == params.world_size
    assert prefix == "unused"
    driver.await_round(barrier)
    queue.put(
        (
            [(float(rank), float(2 + 2 * rank), 10 - 5 * rank)],
            0.5 + 0.4 * rank,
            1.0 + rank,
        )
    )


def test_spawn_rank_epochs_reduces_results_and_uses_slowest_build():
    params = SimpleNamespace(world_size=2, rounds=1)

    result = driver.spawn_rank_epochs(_rank_result_probe, "unused", params)

    assert result == ([4.0], [15], 1.9, 2.0)
