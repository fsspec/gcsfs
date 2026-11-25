import contextlib
import multiprocessing as mp
import os
import signal
import time

import fsspec
import psutil
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
    assert "dcp_read_amplification_planned" not in bench.extra_info


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
            durations=[1.0],
            verify=lambda: _EVENTS.append("verify"),
            close=lambda: _EVENTS.append("close"),
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
    assert _EVENTS == ["monitor_enter", "run", "monitor_exit", "verify", "close"]


def test_result_close_runs_when_monitor_exit_fails_and_skips_verify(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(checkpoint_case, "assert_fsspec_gcsfs", lambda p: None)
    _EVENTS.clear()

    class _FailingExitMonitor(_Monitor):
        @contextlib.contextmanager
        def __call__(self):
            _EVENTS.append("monitor_enter")
            yield self
            _EVENTS.append("monitor_exit_raise")
            raise RuntimeError("monitor boom")

    with pytest.raises(RuntimeError, match="monitor boom"):
        checkpoint_case.run_checkpoint_case(
            _Bench(),
            _FailingExitMonitor(),
            _params(),
            _VerifyingDriver(),
            bucket_ctx=_local_bucket_ctx(tmp_path),
        )
    assert _EVENTS == ["monitor_enter", "run", "monitor_exit_raise", "close"]


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


class _BytesTransferredReadDriver(CheckpointDriver):
    def __init__(
        self, durations, bytes_transferred=None, extra_columns=None, read_count=8
    ):
        self._durations = durations
        self._bytes_transferred = bytes_transferred
        self._extra_columns = extra_columns or {}
        self._read_count = read_count

    def setup(self, prefix, params):
        pass

    def run(self, prefix, params):
        from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import (
            CheckpointResult,
        )

        return CheckpointResult(
            durations=self._durations,
            bytes_transferred=self._bytes_transferred,
            extra_columns=dict(self._extra_columns),
        )

    def read_count(self, params):
        return self._read_count


def _memory_checkpoint(monkeypatch, size):
    """Serves gs:// prefixes from memory:// with a size-byte model.ckpt."""
    monkeypatch.setattr(checkpoint_case, "assert_fsspec_gcsfs", lambda p: None)
    original_url_to_fs = fsspec.core.url_to_fs

    def mock_url_to_fs(url, **kwargs):
        if url.startswith("gs://"):
            fs, path = original_url_to_fs(url.replace("gs://", "memory://"))
            fs.makedirs(path, exist_ok=True)
            if size > 0:
                fs.pipe_file(os.path.join(path, "model.ckpt"), b"0" * size)
            return fs, path
        return original_url_to_fs(url, **kwargs)

    monkeypatch.setattr(fsspec.core, "url_to_fs", mock_url_to_fs)


def test_throughput_uses_bytes_transferred(tmp_path, monkeypatch):
    size = 500
    d = 2.0
    _memory_checkpoint(monkeypatch, size)

    bench = _Bench()
    params = _params(scenario="checkpoint_read")
    driver = _BytesTransferredReadDriver(
        durations=[d],
        bytes_transferred=10 * size,
        read_count=3,
    )

    checkpoint_case.run_checkpoint_case(
        bench,
        _Monitor(),
        params,
        driver,
        bucket_ctx=_local_bucket_ctx(tmp_path),
    )

    assert driver.read_count(params) == 3
    assert bench.extra_info["checkpoint_physical_size_bytes"] == size
    assert bench.extra_info[
        "checkpoint_read_throughput_mean_bytes_per_second"
    ] == pytest.approx(10 * size / d)


@pytest.mark.parametrize(
    "physical_size, fetched_total, expected_amp",
    [
        pytest.param(500, 1250, 2.5, id="nonzero-size"),
        pytest.param(0, 1250, 0.0, id="zero-size"),
    ],
)
def test_amplification_planned_published(
    tmp_path, monkeypatch, physical_size, fetched_total, expected_amp
):
    _memory_checkpoint(monkeypatch, physical_size)

    bench = _Bench()
    params = _params(scenario="checkpoint_read")
    driver = _BytesTransferredReadDriver(
        durations=[1.0],
        bytes_transferred=fetched_total,
        extra_columns={"dcp_read_bytes_fetched_total": fetched_total},
    )

    checkpoint_case.run_checkpoint_case(
        bench,
        _Monitor(),
        params,
        driver,
        bucket_ctx=_local_bucket_ctx(tmp_path),
    )

    assert bench.extra_info["checkpoint_physical_size_bytes"] == physical_size
    assert bench.extra_info["dcp_read_amplification_planned"] == pytest.approx(
        expected_amp
    )


def _write_pid_atomic(pid_file):
    tmp = f"{pid_file}.tmp"
    with open(tmp, "w") as f:
        f.write(str(os.getpid()))
    os.replace(tmp, pid_file)


def _sleep_and_record(pid_file):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    _write_pid_atomic(pid_file)
    time.sleep(600)


class _HangingSetupDriver(_FakeReadDriver):
    def __init__(self, pid_dir):
        super().__init__()
        self._pid_dir = str(pid_dir)

    def setup(self, prefix, params):
        _write_pid_atomic(os.path.join(self._pid_dir, "setup"))
        child = mp.get_context("spawn").Process(
            target=_sleep_and_record,
            args=(os.path.join(self._pid_dir, "child"),),
        )
        child.start()
        child.join()


class _Timeout(BaseException):
    pass


def test_setup_process_tree_is_reaped_when_interrupted(tmp_path, monkeypatch):
    monkeypatch.setattr(checkpoint_case, "assert_fsspec_gcsfs", lambda p: None)
    orig_terminate = checkpoint_case._terminate_process_tree
    monkeypatch.setattr(
        checkpoint_case,
        "_terminate_process_tree",
        lambda pid, grace_s=0.3, **kw: orig_terminate(pid, grace_s=grace_s, **kw),
    )
    pid_dir = tmp_path / "pids"
    pid_dir.mkdir()
    params = _params(scenario="checkpoint_read")

    def on_alarm(signum, frame):
        if (pid_dir / "setup").exists() and (pid_dir / "child").exists():
            signal.setitimer(signal.ITIMER_REAL, 0)
            raise _Timeout()

    old = signal.signal(signal.SIGALRM, on_alarm)
    signal.setitimer(signal.ITIMER_REAL, 0.2, 0.2)
    try:
        try:
            with pytest.raises(_Timeout):
                checkpoint_case.run_checkpoint_case(
                    _Bench(),
                    _Monitor(),
                    params,
                    _HangingSetupDriver(pid_dir),
                    bucket_ctx=_local_bucket_ctx(tmp_path / "bucket"),
                )
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old)

        pids = [int((pid_dir / n).read_text()) for n in ("setup", "child")]
        assert not any(
            psutil.pid_exists(p) and psutil.Process(p).status() != psutil.STATUS_ZOMBIE
            for p in pids
        )
        assert mp.active_children() == []
    finally:
        for n in ("child", "setup"):
            pfile = pid_dir / n
            if pfile.exists():
                with contextlib.suppress(ProcessLookupError, ValueError):
                    os.kill(int(pfile.read_text()), signal.SIGKILL)
