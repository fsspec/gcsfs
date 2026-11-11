import contextlib
import os

import fsspec
import pytest

from gcsfs.tests.perf.subsystembenchmarks.checkpointing import checkpoint_case
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.configurator import (
    CheckpointParameters,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import CheckpointDriver


@pytest.fixture(autouse=True)
def _bucket_env(monkeypatch):
    monkeypatch.setenv("GCSFS_SUBSYSTEM_BUCKET_PREFIX", "test-prefix")
    monkeypatch.setenv("GCSFS_SUBSYSTEM_PROJECT", "test-project")
    monkeypatch.setenv("GCSFS_SUBSYSTEM_LOCATION", "us-central1")


class _FakeWriteResult:
    def __init__(self, durations, extra_columns=None):
        self.durations = durations
        self.extra_columns = extra_columns or {}


class _FakeWriteDriver(CheckpointDriver):
    def __init__(self, durations=None):
        self._durations = durations or [1.0, 1.5]
        self.setup_prefix = None
        self.run_prefix = None

    def setup(self, prefix, params):
        self.setup_prefix = prefix

    def run(self, prefix, params):
        self.run_prefix = prefix
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


class _FakeReadDriver(CheckpointDriver):
    def setup(self, prefix, params):
        pass

    def __init__(self, durations=None, read_count=1):
        self._durations = durations or [0.8, 1.2]
        self._read_count = read_count

    def run(self, prefix, params):
        return _FakeWriteResult(durations=self._durations)

    def read_count(self, params):
        return self._read_count


def test_run_checkpoint_write_case(tmp_path, monkeypatch):
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
    driver = _FakeWriteDriver()

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
    assert bench.extra_info["world_size"] == 1
    assert bench.extra_info["tensor_parallel_size"] == 1
    assert bench.extra_info["data_parallel_size"] == 1


@pytest.mark.parametrize("read_count", [1, 8])
def test_run_checkpoint_read_case(tmp_path, monkeypatch, read_count):
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
    params = _params(scenario="checkpoint_read")
    driver = _FakeReadDriver(durations=[1.0], read_count=read_count)

    checkpoint_case.run_checkpoint_case(
        bench,
        _Monitor(),
        params,
        driver,
        bucket_ctx=_local_bucket_ctx(tmp_path),
    )

    assert bench.group == "checkpoint_read"
    assert bench.extra_info["workload_implementation"] == "fake"
    assert bench.extra_info["checkpoint_physical_size_bytes"] == 500
    assert (
        bench.extra_info["checkpoint_read_throughput_mean_bytes_per_second"]
        == 500 * read_count
    )


def test_checkpoint_case_prefix_generation(monkeypatch):
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

    # Test with full URI returned by bucket_ctx
    driver_uri = _FakeWriteDriver()

    @contextlib.contextmanager
    def uri_bucket_ctx(spec, case_id, **kw):
        yield "gs://my-bucket/data/"

    checkpoint_case.run_checkpoint_case(
        bench,
        _Monitor(),
        params,
        driver_uri,
        bucket_ctx=uri_bucket_ctx,
    )

    assert driver_uri.run_prefix == "gs://my-bucket/checkpoint/"

    # Test with plain bucket name returned by bucket_ctx
    driver_name = _FakeWriteDriver()

    @contextlib.contextmanager
    def name_bucket_ctx(spec, case_id, **kw):
        yield "my-bucket-name"

    checkpoint_case.run_checkpoint_case(
        bench,
        _Monitor(),
        params,
        driver_name,
        bucket_ctx=name_bucket_ctx,
    )

    assert driver_name.run_prefix == "gs://my-bucket-name/checkpoint/"


class _FakeDcpDriver(_FakeWriteDriver):
    def is_checkpoint_file(self, path: str) -> bool:
        return path.endswith(".distcp") or path.endswith(".metadata")


def test_physical_size_uses_driver_filter(tmp_path, monkeypatch):
    monkeypatch.setattr(checkpoint_case, "assert_fsspec_gcsfs", lambda p: None)

    original_url_to_fs = fsspec.core.url_to_fs

    def mock_url_to_fs(url, **kwargs):
        if url.startswith("gs://"):
            mem_url = url.replace("gs://", "memory://")
            fs, path = original_url_to_fs(mem_url)
            # Create files: .distcp (300 B), .metadata (100 B), and a model.ckpt decoy (999 B)
            fs.makedirs(path, exist_ok=True)
            for fname, size in [
                ("shard_0.distcp", 300),
                (".metadata", 100),
                ("model.ckpt", 999),
            ]:
                with fs.open(os.path.join(path, fname), "wb") as f:
                    f.write(b"x" * size)
            return fs, path
        return original_url_to_fs(url, **kwargs)

    monkeypatch.setattr(fsspec.core, "url_to_fs", mock_url_to_fs)

    bench = _Bench()
    params = _params()
    driver = _FakeDcpDriver()

    checkpoint_case.run_checkpoint_case(
        bench,
        _Monitor(),
        params,
        driver,
        bucket_ctx=_local_bucket_ctx(tmp_path),
    )

    assert bench.extra_info["checkpoint_physical_size_bytes"] == 400


_EVENTS = []


class _VerifyingDriver(_FakeWriteDriver):
    def run(self, prefix, params):
        from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import (
            CheckpointResult,
        )

        _EVENTS.append("run")
        return CheckpointResult(
            durations=[1.0], verify=lambda: _EVENTS.append("verify")
        )


def test_result_verify_runs_after_monitor_window(tmp_path, monkeypatch):
    monkeypatch.setattr(checkpoint_case, "assert_fsspec_gcsfs", lambda p: None)
    original_url_to_fs = fsspec.core.url_to_fs

    def mock_url_to_fs(url, **kwargs):
        if url.startswith("gs://"):
            fs, path = original_url_to_fs(url.replace("gs://", "memory://"))
            fs.makedirs(path, exist_ok=True)
            fs.pipe_file(os.path.join(path, "model.ckpt"), b"0" * 500)
            return fs, path
        return original_url_to_fs(url, **kwargs)

    monkeypatch.setattr(fsspec.core, "url_to_fs", mock_url_to_fs)
    _EVENTS.clear()

    class _RecordingMonitor(_Monitor):
        @contextlib.contextmanager
        def __call__(self):
            _EVENTS.append("monitor_enter")
            yield self
            _EVENTS.append("monitor_exit")

    checkpoint_case.run_checkpoint_case(
        _Bench(),
        _RecordingMonitor(),
        _params(),
        _VerifyingDriver(),
        bucket_ctx=_local_bucket_ctx(tmp_path),
    )
    assert _EVENTS == ["monitor_enter", "run", "monitor_exit", "verify"]


def test_run_checkpoint_case_warms_rapid_cache_for_read_only(tmp_path, monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import rapid_cache

    monkeypatch.setattr(checkpoint_case, "assert_fsspec_gcsfs", lambda p: None)
    # Even when GCSFS_SUBSYSTEM_BUCKET_TYPE=rapid_cache_warm is set without GCSFS_SUBSYSTEM_ZONE,
    # a custom bucket_ctx should not fail BucketSpec.from_env() validation, and warm_if_needed
    # must receive the fs resolved by fsspec.core.url_to_fs(prefix).
    monkeypatch.setenv("GCSFS_SUBSYSTEM_BUCKET_TYPE", "rapid_cache_warm")
    monkeypatch.delenv("GCSFS_SUBSYSTEM_ZONE", raising=False)

    warm_calls = []
    orig_warm = rapid_cache.warm_if_needed

    def spy_warm(prefix, bucket_type, *, fs=None):
        warm_calls.append((prefix, bucket_type, fs))
        return orig_warm(prefix, bucket_type, fs=fs)

    monkeypatch.setattr(rapid_cache, "warm_if_needed", spy_warm)

    url_to_fs_kwargs = []
    original_url_to_fs = fsspec.core.url_to_fs

    def mock_url_to_fs(url, **kwargs):
        url_to_fs_kwargs.append(kwargs)
        if url.startswith("gs://"):
            mem_url = url.replace("gs://", "memory://")
            fs, path = original_url_to_fs(mem_url)
            model_file = os.path.join(path, "model.ckpt")
            fs.makedirs(os.path.dirname(model_file), exist_ok=True)
            with fs.open(model_file, "wb") as f:
                f.write(b"0" * 500)
            return fs, path
        return original_url_to_fs(url, **kwargs)

    monkeypatch.setattr(fsspec.core, "url_to_fs", mock_url_to_fs)

    # Read case on rapid_cache_warm must delegate to warm_if_needed (with fs=None),
    # which calls url_to_fs(skip_instance_cache=True).
    checkpoint_case.run_checkpoint_case(
        _Bench(),
        _Monitor(),
        _params(scenario="checkpoint_read", bucket_type="rapid_cache_warm"),
        _FakeReadDriver(durations=[1.0]),
        bucket_ctx=_local_bucket_ctx(tmp_path),
    )
    assert len(warm_calls) == 1
    assert warm_calls[0][1] == "rapid_cache_warm"
    assert warm_calls[0][2] is None
    assert url_to_fs_kwargs[0] == {"skip_instance_cache": True}

    # Read case on rapid_cache_cold must call warm_if_needed (which is a no-op) without pre-run url_to_fs
    warm_calls.clear()
    url_to_fs_kwargs.clear()
    checkpoint_case.run_checkpoint_case(
        _Bench(),
        _Monitor(),
        _params(scenario="checkpoint_read", bucket_type="rapid_cache_cold"),
        _FakeReadDriver(durations=[1.0]),
        bucket_ctx=_local_bucket_ctx(tmp_path),
    )
    assert len(warm_calls) == 1
    assert warm_calls[0][1] == "rapid_cache_cold"
    assert warm_calls[0][2] is None
    assert len(url_to_fs_kwargs) == 1  # Only the post-run physical size check

    # Write case must not call warm_if_needed
    warm_calls.clear()
    checkpoint_case.run_checkpoint_case(
        _Bench(),
        _Monitor(),
        _params(scenario="checkpoint_write", bucket_type="rapid_cache_warm"),
        _FakeWriteDriver(durations=[1.0]),
        bucket_ctx=_local_bucket_ctx(tmp_path),
    )
    assert warm_calls == []


@contextlib.contextmanager
def _rcu_case_ctx(spec, case_id, **kw):
    yield "gs://rcu-bkt/data/"


_RCU_PREFIX = "gs://rcu-bkt/checkpoint/"


@pytest.fixture
def rcu_case(monkeypatch):
    """Serves gs:// from memory and records the RCU storage-class check and warm-up."""
    monkeypatch.setattr(checkpoint_case, "assert_fsspec_gcsfs", lambda p: None)
    original_url_to_fs = fsspec.core.url_to_fs

    def mock_url_to_fs(url, **kwargs):
        if url.startswith("gs://"):
            fs, path = original_url_to_fs(url.replace("gs://", "memory://"))
            fs.pipe_file(os.path.join(path, "model.ckpt"), b"0" * 500)
            return fs, path
        return original_url_to_fs(url, **kwargs)

    monkeypatch.setattr(fsspec.core, "url_to_fs", mock_url_to_fs)
    events = []
    monkeypatch.setattr(
        checkpoint_case.rcu,
        "assert_storage_class",
        lambda prefix, bt: events.append(("check", prefix, bt)),
    )
    monkeypatch.setattr(
        checkpoint_case.rcu,
        "warm_if_needed",
        lambda prefix, bt: events.append(("warm", prefix, bt)),
    )
    return events


class _EventMonitor(_Monitor):
    def __init__(self, events):
        self.events = events

    @contextlib.contextmanager
    def __call__(self):
        self.events.append("monitor_enter")
        yield self
        self.events.append("monitor_exit")


def test_rcu_checkpoint_loads_are_checked_and_warmed_before_the_window(rcu_case):
    params = _params(scenario="checkpoint_read", bucket_type="standard_rcu_warm")

    checkpoint_case.run_checkpoint_case(
        _Bench(),
        _EventMonitor(rcu_case),
        params,
        _FakeReadDriver(durations=[1.0]),
        bucket_ctx=_rcu_case_ctx,
    )

    assert rcu_case == [
        ("check", _RCU_PREFIX, "standard_rcu_warm"),
        ("warm", _RCU_PREFIX, "standard_rcu_warm"),
        "monitor_enter",
        "monitor_exit",
    ]


def test_rcu_z_checkpoint_saves_are_checked_after_the_window(rcu_case):
    params = _params(scenario="checkpoint_write", bucket_type="standard_rcu_z")

    checkpoint_case.run_checkpoint_case(
        _Bench(),
        _EventMonitor(rcu_case),
        params,
        _FakeWriteDriver(durations=[1.0]),
        bucket_ctx=_rcu_case_ctx,
    )

    assert rcu_case == [
        "monitor_enter",
        "monitor_exit",
        ("check", _RCU_PREFIX, "standard_rcu_z"),
    ]


class _FlagRecordingDriver(_FakeReadDriver):
    """setup() runs in a spawned process and records the flag that process got."""

    def __init__(self, flag_file):
        super().__init__(durations=[1.0])
        self.flag_file = flag_file

    def setup(self, prefix, params):
        with open(self.flag_file, "w") as f:
            f.write(os.environ.get("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "unset"))


@pytest.mark.parametrize(
    "bucket_type, staging_flag",
    [("standard_rcu_cold", "false"), ("standard_rcu_z", "true")],
)
def test_checkpoint_setup_stages_rcu_cold_and_warm_with_the_http_only_filesystem(
    rcu_case, monkeypatch, tmp_path, bucket_type, staging_flag
):
    monkeypatch.setenv("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "true")
    flag_file = tmp_path / "flag"

    checkpoint_case.run_checkpoint_case(
        _Bench(),
        _Monitor(),
        _params(scenario="checkpoint_read", bucket_type=bucket_type),
        _FlagRecordingDriver(str(flag_file)),
        bucket_ctx=_rcu_case_ctx,
    )

    assert flag_file.read_text() == staging_flag
