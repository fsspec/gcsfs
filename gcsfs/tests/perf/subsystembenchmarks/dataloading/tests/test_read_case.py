import contextlib
import statistics
from dataclasses import dataclass

import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading import read_case
from gcsfs.tests.perf.subsystembenchmarks.dataloading.configurator import ReadParameters


@pytest.fixture(autouse=True)
def _bucket_env(monkeypatch):
    monkeypatch.setenv("GCSFS_SUBSYSTEM_BUCKET_PREFIX", "test-prefix")
    monkeypatch.setenv("GCSFS_SUBSYSTEM_PROJECT", "test-project")
    monkeypatch.setenv("GCSFS_SUBSYSTEM_LOCATION", "us-central1")


@dataclass
class _P(ReadParameters):
    LOADER_TAG = "fk"


class _FakeDriver:
    formats = ("pretok_parquet",)

    def __init__(self, rows, build_seconds=0.5, extra_columns=None):
        self._rows = rows
        self._build = build_seconds
        self._extra = extra_columns or {}

    def run_read(self, prefix, params):
        from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import ReadResult

        return ReadResult(
            durations=[1.0, 2.0],
            rows_per_epoch=[self._rows, self._rows],
            ttfb_seconds=0.25,
            build_seconds=self._build,
            extra_columns=self._extra,
        )


class _Bench:
    def __init__(self):
        self.extra_info = {}
        self.group = None

    def pedantic(self, fn, rounds, iterations, warmup_rounds):
        fn()


class _Monitor:
    max_cpu = 1.0
    max_mem = 2.0
    net_recv = 100.0
    net_sent = 50.0
    duration = 2.0
    vcpus = 4

    def __call__(self):
        return contextlib.nullcontext(self)


def _params(**over):
    kw = dict(
        name="c",
        bucket_name="",
        bucket_type="regional",
        rounds=2,
        scenario="read",
        framework="fake",
        fmt="pretok_parquet",
        seq_len=8,
        file_count=2,
        rows_per_file=5,
        row_group_size=5,
        access="sequential",
        num_workers=2,
        batch_size=4,
    )
    kw.update(over)
    return _P(**kw)


def _local_bucket_ctx(tmp_path):
    @contextlib.contextmanager
    def ctx(spec, case_id, **kw):
        yield str(tmp_path)

    return ctx


def test_run_read_case_publishes_throughput_and_passes_guard(tmp_path, monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import datagen

    man = {
        "file_count": 2,
        "corpus_bytes": 1000,
        "sample_count": 10,
        "fmt": "pretok_parquet",
        "rows_per_file": 5,
    }
    monkeypatch.setattr(datagen, "ingest_dataset", lambda *a, **k: man)
    monkeypatch.setattr(read_case, "assert_fsspec_gcsfs", lambda prefix: None)

    bench = _Bench()
    params = _params()
    read_case.run_read_case(
        bench,
        _Monitor(),
        params,
        _FakeDriver(rows=10, extra_columns={"fake_driver_col": 7}),
        bucket_ctx=_local_bucket_ctx(tmp_path),
    )
    assert bench.group == "read"
    assert bench.extra_info["fake_driver_col"] == 7
    assert bench.extra_info["sample_count"] == 10
    assert bench.extra_info["framework"] == "fake"
    assert bench.extra_info["sweep_axis"] == "baseline"
    assert bench.extra_info["mean_samples_per_second"] == statistics.mean(
        [10 / 1.0, 10 / 2.0]
    )
    assert bench.extra_info["dataloader_num_workers"] == 2


def test_run_read_case_publishes_build_seconds(tmp_path, monkeypatch):
    """Verify that dataset build latency is published in dataset_build_seconds."""
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import datagen

    man = {
        "file_count": 2,
        "corpus_bytes": 1000,
        "sample_count": 10,
        "fmt": "pretok_parquet",
        "rows_per_file": 5,
    }
    monkeypatch.setattr(datagen, "ingest_dataset", lambda *a, **k: man)
    monkeypatch.setattr(read_case, "assert_fsspec_gcsfs", lambda prefix: None)

    bench = _Bench()
    read_case.run_read_case(
        bench,
        _Monitor(),
        _params(),
        _FakeDriver(rows=10, build_seconds=1.75),
        bucket_ctx=_local_bucket_ctx(tmp_path),
    )
    assert bench.extra_info["dataset_build_seconds"] == 1.75


def test_run_read_case_fails_partial_read(tmp_path, monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import datagen

    man = {
        "file_count": 2,
        "corpus_bytes": 1000,
        "sample_count": 99,
        "fmt": "pretok_parquet",
        "rows_per_file": 5,
    }
    monkeypatch.setattr(datagen, "ingest_dataset", lambda *a, **k: man)
    monkeypatch.setattr(read_case, "assert_fsspec_gcsfs", lambda prefix: None)

    with pytest.raises(ValueError, match="partial read"):
        read_case.run_read_case(
            _Bench(),
            _Monitor(),
            _params(),
            _FakeDriver(rows=10),
            bucket_ctx=_local_bucket_ctx(tmp_path),
        )


def test_run_read_case_rejects_unsupported_format(tmp_path):
    class _JsonOnly(_FakeDriver):
        formats = ("pretok_jsonl",)

    with pytest.raises(ValueError, match="format"):
        read_case.run_read_case(
            _Bench(),
            _Monitor(),
            _params(fmt="pretok_parquet"),
            _JsonOnly(rows=10),
            bucket_ctx=_local_bucket_ctx(tmp_path),
        )
