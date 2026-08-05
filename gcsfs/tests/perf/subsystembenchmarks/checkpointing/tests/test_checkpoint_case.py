import contextlib
import os

import fsspec
import pytest

from gcsfs.tests.perf.subsystembenchmarks.checkpointing import checkpoint_case
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.configurator import (
    CheckpointParameters,
)


@pytest.fixture(autouse=True)
def _bucket_env(monkeypatch):
    monkeypatch.setenv("GCSFS_SUBSYSTEM_BUCKET_PREFIX", "test-prefix")
    monkeypatch.setenv("GCSFS_SUBSYSTEM_PROJECT", "test-project")
    monkeypatch.setenv("GCSFS_SUBSYSTEM_LOCATION", "us-central1")


class _FakeWriteResult:
    def __init__(self, durations, extra_columns=None):
        self.durations = durations
        self.extra_columns = extra_columns or {}


class _FakeDriver:
    def __init__(self, durations=None):
        self._durations = durations or [1.0, 1.5]

    def run_save(self, prefix, params):
        return _FakeWriteResult(durations=self._durations)


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
        scenario="checkpoint_write",
        framework="fake",
        model_id="fake-model",
        strategy="single",
    )
    kw.update(over)
    return CheckpointParameters(**kw)


def _local_bucket_ctx(tmp_path):
    @contextlib.contextmanager
    def ctx(spec, case_id, **kw):
        yield str(tmp_path)

    return ctx


def test_run_checkpoint_case(tmp_path, monkeypatch):
    monkeypatch.setattr(checkpoint_case, "assert_fsspec_gcsfs", lambda p: None)

    original_url_to_fs = fsspec.core.url_to_fs

    def mock_url_to_fs(url, **kwargs):
        if url.startswith("gs://"):
            mem_url = url.replace("gs://", "memory://")
            fs, path = original_url_to_fs(mem_url)
            # Create a mock checkpoint file
            model_file = os.path.join(path, "model.ckpt")
            fs.makedirs(os.path.dirname(model_file), exist_ok=True)
            with fs.open(model_file, "wb") as f:
                f.write(b"0" * 500)  # 500 bytes
            return fs, path
        return original_url_to_fs(url, **kwargs)

    monkeypatch.setattr(fsspec.core, "url_to_fs", mock_url_to_fs)

    bench = _Bench()
    params = _params()
    driver = _FakeDriver()

    checkpoint_case.run_checkpoint_case(
        bench,
        _Monitor(),
        params,
        driver,
        bucket_ctx=_local_bucket_ctx(tmp_path),
    )

    assert bench.group == "checkpoint_write"
    assert bench.extra_info["workload_implementation"] == "fake"
    assert bench.extra_info["checkpoint_physical_size_bytes"] == 500
    assert bench.extra_info["checkpoint_write_throughput_mean_bytes_per_second"] > 0
