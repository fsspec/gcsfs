import dataclasses
import json
import math
import sys
import types
from types import SimpleNamespace

import pytest

from gcsfs.tests.perf.subsystembenchmarks._common import env, launcher
from gcsfs.tests.perf.subsystembenchmarks.dataloading import driver


@dataclasses.dataclass
class _DummyParams:
    world_size: int = 2
    rounds: int = 2
    batch_size: int = 8


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


def test_run_rank_epochs_distributed_invokes_torchrun_and_reads_result(monkeypatch):
    captured = {}

    def fake_run_torchrun(module, args, *, world_size, rdzv_id, **kwargs):
        captured["module"] = module
        captured["args"] = list(args)
        captured["world_size"] = world_size
        captured["rdzv_id"] = rdzv_id
        captured["kwargs"] = kwargs
        spec_path = args[args.index("--spec-file") + 1]
        result_path = args[args.index("--result-file") + 1]
        with open(spec_path, encoding="utf-8") as f:
            captured["spec"] = json.load(f)
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "durations": [4.0, 3.5],
                    "rows": [15, 15],
                    "ttfb": 1.9,
                    "build_seconds": 2.0,
                },
                f,
            )
        return True

    monkeypatch.setattr(launcher, "run_torchrun", fake_run_torchrun)

    params = _DummyParams(world_size=4, rounds=2, batch_size=16)
    result = driver.run_rank_epochs_distributed(
        "hf",
        "gs://bucket/prefix",
        params,
        extra={"sample_count": 30},
        prepare_tpu_env=False,
    )

    assert result == ([4.0, 3.5], [15, 15], 1.9, 2.0)
    assert (
        captured["module"] == "gcsfs.tests.perf.subsystembenchmarks.dataloading._worker"
    )
    assert "--distributed" in captured["args"]
    assert captured["args"][captured["args"].index("--kind") + 1] == "hf"
    assert captured["world_size"] == 4
    assert captured["rdzv_id"]
    assert captured["kwargs"]["prepare_tpu_env"] is False
    assert captured["spec"] == {
        "prefix": "gs://bucket/prefix",
        "params": {"world_size": 4, "rounds": 2, "batch_size": 16},
        "extra": {"sample_count": 30},
    }


def test_run_single_rank_isolated_invokes_run_subprocess_and_reads_result(monkeypatch):
    captured = {}

    def fake_run_subprocess(module, args, **kwargs):
        captured["module"] = module
        captured["args"] = list(args)
        captured["kwargs"] = kwargs
        spec_path = args[args.index("--spec-file") + 1]
        result_path = args[args.index("--result-file") + 1]
        with open(spec_path, encoding="utf-8") as f:
            captured["spec"] = json.load(f)
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "durations": [1.2],
                    "rows": [10],
                    "ttfb": 0.3,
                    "build_seconds": 0.4,
                },
                f,
            )

    monkeypatch.setattr(launcher, "run_subprocess", fake_run_subprocess)

    params = _DummyParams(world_size=1, rounds=1, batch_size=4)
    result = driver.run_single_rank_isolated(
        "wds",
        "file:///tmp/data",
        params,
        extra={"cache_root": "/tmp/cache"},
    )

    assert result == ([1.2], [10], 0.3, 0.4)
    assert (
        captured["module"] == "gcsfs.tests.perf.subsystembenchmarks.dataloading._worker"
    )
    assert "--distributed" not in captured["args"]
    assert captured["args"][captured["args"].index("--kind") + 1] == "wds"
    assert captured["spec"] == {
        "prefix": "file:///tmp/data",
        "params": {"world_size": 1, "rounds": 1, "batch_size": 4},
        "extra": {"cache_root": "/tmp/cache"},
    }


def test_worker_main_single_rank_executes_registered_kind(tmp_path, monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import _worker

    seen = {}

    def fake_worker(rank, world_size, prefix, params, barrier, extra):
        seen["args"] = (rank, world_size, prefix, params.rounds, barrier, extra)
        return ([(10.0, 11.5, 20)], 0.25, 0.5)

    monkeypatch.setitem(_worker._WORKERS, "mock_kind", fake_worker)

    spec_file = tmp_path / "spec.json"
    result_file = tmp_path / "result.json"
    spec_file.write_text(
        json.dumps(
            {
                "prefix": "file:///data",
                "params": {"world_size": 1, "rounds": 1, "batch_size": 4},
                "extra": {"tag": "ok"},
            }
        ),
        encoding="utf-8",
    )

    _worker.main(
        [
            "--kind",
            "mock_kind",
            "--spec-file",
            str(spec_file),
            "--result-file",
            str(result_file),
        ]
    )

    assert seen["args"] == (0, 1, "file:///data", 1, None, {"tag": "ok"})
    assert json.loads(result_file.read_text(encoding="utf-8")) == {
        "durations": [1.5],
        "rows": [20],
        "ttfb": 0.25,
        "build_seconds": 0.5,
    }


def test_worker_main_distributed_uses_gloo_barrier_and_gather(tmp_path, monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import _worker

    events = []
    rank0_tuple = ([(0.0, 2.0, 10)], 0.5, 1.0)
    rank1_tuple = ([(1.0, 4.0, 5)], 0.9, 2.0)

    fake_dist = types.SimpleNamespace(
        init_process_group=lambda **kw: events.append(("init", kw["backend"])),
        get_rank=lambda: 0,
        get_world_size=lambda: 2,
        barrier=lambda: events.append(("barrier",)),
        gather_object=lambda obj, out, dst=0: (
            events.append(("gather", dst)),
            out.__setitem__(0, obj),
            out.__setitem__(1, rank1_tuple),
        ),
        destroy_process_group=lambda: events.append(("destroy",)),
    )
    fake_torch = types.SimpleNamespace(distributed=fake_dist)
    monkeypatch.setitem(sys.modules, "torch", fake_torch)
    monkeypatch.setitem(sys.modules, "torch.distributed", fake_dist)

    def fake_worker(rank, world_size, prefix, params, barrier, extra):
        assert rank == 0 and world_size == 2
        driver.await_round(barrier)
        return rank0_tuple

    monkeypatch.setitem(_worker._WORKERS, "mock_dist", fake_worker)

    spec_file = tmp_path / "spec.json"
    result_file = tmp_path / "result.json"
    spec_file.write_text(
        json.dumps(
            {
                "prefix": "file:///data",
                "params": {"world_size": 2, "rounds": 1},
                "extra": {},
            }
        ),
        encoding="utf-8",
    )

    _worker.main(
        [
            "--kind",
            "mock_dist",
            "--spec-file",
            str(spec_file),
            "--result-file",
            str(result_file),
            "--distributed",
        ]
    )

    assert events == [
        ("init", "gloo"),
        ("barrier",),
        ("gather", 0),
        ("destroy",),
    ]
    assert json.loads(result_file.read_text(encoding="utf-8")) == {
        "durations": [4.0],
        "rows": [15],
        "ttfb": 1.9,
        "build_seconds": 2.0,
    }


def test_worker_main_rejects_unknown_kind(tmp_path):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import _worker

    spec_file = tmp_path / "spec.json"
    result_file = tmp_path / "result.json"
    spec_file.write_text(
        json.dumps({"prefix": "file:///data", "params": {"rounds": 1}, "extra": {}}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unknown worker kind"):
        _worker.main(
            [
                "--kind",
                "nonexistent",
                "--spec-file",
                str(spec_file),
                "--result-file",
                str(result_file),
            ]
        )


def test_env_no_longer_defines_accelerator_helpers():
    for removed in (
        "detect_accelerator",
        "detect_backend",
        "gpu_model",
        "_cuda_available",
    ):
        assert not hasattr(env, removed), f"_common/env.py still defines {removed}"


# Module-level probe functions for mp.spawn pickling (temporary until Task 7).


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
