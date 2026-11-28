import math
import sys
import types

import pytest

from gcsfs.tests.perf.subsystembenchmarks._common import launcher
from gcsfs.tests.perf.subsystembenchmarks.dataloading import driver
from gcsfs.tests.perf.subsystembenchmarks.dataloading.tests import _worker_entries
from gcsfs.tests.perf.subsystembenchmarks.dataloading.tests._worker_entries import (
    RecordParams,
)

_WORKER_MODULE = "gcsfs.tests.perf.subsystembenchmarks.dataloading._worker"


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


_ENTRY = "gcsfs.tests.perf.subsystembenchmarks.dataloading.tests._worker_entries:record"
_RESULT = {"durations": [4.0, 3.5], "rows": [15, 15], "ttfb": 1.9, "build_seconds": 2.0}


@pytest.fixture
def entry_calls():
    _worker_entries.calls.clear()
    yield _worker_entries.calls
    _worker_entries.calls.clear()


def _fake_launch(captured):
    """Record a launch, read the spec it was given, and write a canned result."""

    def launch(module, args, **kwargs):
        captured.update(module=module, args=list(args), kwargs=kwargs)
        spec_path = args[args.index("--spec-file") + 1]
        captured["spec"] = driver.read_worker_spec(spec_path)
        result_path = args[args.index("--result-file") + 1]
        driver.write_worker_result(result_path, **_RESULT)
        return True

    return launch


@pytest.mark.parametrize("cpu_only", [False, True])
def test_run_rank_epochs_distributed_launches_torchrun(monkeypatch, cpu_only):
    captured = {}
    monkeypatch.setattr(launcher, "run_torchrun", _fake_launch(captured))
    params = RecordParams(world_size=4, rounds=2, batch_size=16)

    result = driver.run_rank_epochs_distributed(
        _worker_entries.record,
        "gs://bucket/prefix",
        params,
        extra={"sample_count": 30},
        cpu_only=cpu_only,
    )

    assert result == ([4.0, 3.5], [15, 15], 1.9, 2.0)
    assert captured["module"] == _WORKER_MODULE
    args = captured["args"]
    assert args[args.index("--entry") + 1] == _ENTRY
    assert "--distributed" in args
    assert captured["kwargs"]["world_size"] == 4
    assert captured["kwargs"]["rdzv_id"]
    assert captured["kwargs"]["cpu_only"] is cpu_only
    assert captured["spec"] == ("gs://bucket/prefix", params, {"sample_count": 30})


def test_run_single_rank_isolated_launches_a_subprocess(monkeypatch):
    captured = {}
    monkeypatch.setattr(launcher, "run_subprocess", _fake_launch(captured))
    params = RecordParams(world_size=1, rounds=1, batch_size=4)

    result = driver.run_single_rank_isolated(
        _worker_entries.record,
        "file:///tmp/data",
        params,
        extra={"cache_root": "/tmp/cache"},
    )

    assert result == ([4.0, 3.5], [15, 15], 1.9, 2.0)
    assert captured["module"] == _WORKER_MODULE
    assert "--distributed" not in captured["args"]
    assert captured["args"][captured["args"].index("--entry") + 1] == _ENTRY
    assert captured["kwargs"] == {}
    assert captured["spec"] == (
        "file:///tmp/data",
        params,
        {"cache_root": "/tmp/cache"},
    )


def test_worker_spec_round_trips_the_params_dataclass(tmp_path):
    spec_file = str(tmp_path / "spec.json")
    params = RecordParams(world_size=2, rounds=3, batch_size=5)

    driver.write_worker_spec(spec_file, "gs://b/p/", params)
    prefix, rebuilt, extra = driver.read_worker_spec(spec_file)

    assert (prefix, rebuilt, extra) == ("gs://b/p/", params, {})
    assert type(rebuilt) is RecordParams
    assert rebuilt.total_rows == 15


def test_write_worker_spec_rejects_non_dataclass_params(tmp_path):
    with pytest.raises(TypeError, match="dataclass instance"):
        driver.write_worker_spec(str(tmp_path / "spec.json"), "gs://b/p/", object())


def test_qualified_names_resolve_module_level_objects_only():
    assert driver.qualified_name(_worker_entries.record) == _ENTRY
    assert driver.resolve_qualified_name(_ENTRY) is _worker_entries.record

    def local():
        pass

    with pytest.raises(ValueError, match="module level"):
        driver.qualified_name(local)
    with pytest.raises(ValueError, match="module:qualname"):
        driver.resolve_qualified_name("no_colon")


def _worker_args(tmp_path, params, extra=None, *, distributed=False):
    spec_file = tmp_path / "spec.json"
    result_file = tmp_path / "result.json"
    driver.write_worker_spec(str(spec_file), "file:///data", params, extra)
    args = ["--entry", _ENTRY, "--spec-file", str(spec_file)]
    args += ["--result-file", str(result_file)]
    if distributed:
        args.append("--distributed")
    return args, result_file


def test_worker_main_single_rank_runs_the_entry(tmp_path, entry_calls):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import _worker

    params = RecordParams(world_size=1, rounds=1, batch_size=20)
    args, result_file = _worker_args(tmp_path, params, {"tag": "ok"})

    _worker.main(args)

    assert entry_calls == [(0, 1, "file:///data", params, None, {"tag": "ok"})]
    assert type(entry_calls[0][3]) is RecordParams
    assert driver.read_worker_result(str(result_file)) == ([1.5], [20], 0.25, 0.5)


def test_worker_main_distributed_uses_gloo_barrier_and_gather(
    tmp_path, monkeypatch, entry_calls
):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import _worker

    events = []
    rank1_result = ([(1.0, 4.0, 5)], 0.9, 2.0)
    fake_dist = types.SimpleNamespace(
        init_process_group=lambda **kw: events.append(("init", kw["backend"])),
        get_rank=lambda: 0,
        get_world_size=lambda: 2,
        barrier=lambda: events.append(("barrier",)),
        gather_object=lambda obj, out, dst=0: (
            events.append(("gather", dst)),
            out.__setitem__(0, obj),
            out.__setitem__(1, rank1_result),
        ),
        destroy_process_group=lambda: events.append(("destroy",)),
    )
    monkeypatch.setitem(
        sys.modules, "torch", types.SimpleNamespace(distributed=fake_dist)
    )
    monkeypatch.setitem(sys.modules, "torch.distributed", fake_dist)
    params = RecordParams(world_size=2, rounds=1, batch_size=10)
    args, result_file = _worker_args(tmp_path, params, distributed=True)

    _worker.main(args)

    assert [call[:2] for call in entry_calls] == [(0, 2)]
    assert events == [("init", "gloo"), ("barrier",), ("gather", 0), ("destroy",)]
    # Rank 0 spans 10.0-11.5 with first batch at 10.25; rank 1 spans 1.0-4.0.
    assert driver.read_worker_result(str(result_file)) == ([10.5], [15], 9.25, 2.0)
