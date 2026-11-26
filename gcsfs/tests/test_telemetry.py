"""Unit tests for the gcsfs telemetry subsystem."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import copy
import inspect
import io
import logging
import os
import pickle
import sys
import threading
import time
import types
from unittest import mock

import aiohttp
import fsspec.asyn
import fsspec.spec
import grpc
import grpc.aio
import pytest
import pytest_asyncio
from google.auth.transport import DEFAULT_RETRYABLE_STATUS_CODES
from google.cloud import storage_control_v2
from google.longrunning import operations_pb2, operations_pb2_grpc

import gcsfs.telemetry.detector as environment_module
import gcsfs.telemetry.manager as manager_mod
from gcsfs import __version__ as version
from gcsfs.core import GCSFileSystem
from gcsfs.extended_gcsfs import ExtendedGcsFileSystem
from gcsfs.telemetry import schema as telemetry_schema
from gcsfs.telemetry.context import (
    GRPC_METADATA_KEY,
    HTTP_HEADER_KEY,
    Dimension,
    get_telemetry_context,
    has_telemetry_context,
    reset_telemetry_context,
    set_telemetry_context,
)
from gcsfs.telemetry.detector import (
    BaseDetector,
    EnvironmentDetector,
    FrameworkDetector,
)
from gcsfs.telemetry.features_proto import (
    GcsfsEnvironment,
    GcsfsFeatures,
    GcsfsFramework,
    GcsfsMethod,
)
from gcsfs.telemetry.grpc import ControlPlaneTelemetryInterceptor
from gcsfs.telemetry.manager import UsageMetricsTracker
from gcsfs.telemetry.wrapper import (
    _FILE_METHOD_TOKENS,
    _TRACKED_METHODS,
    _gcs_async_wrapper,
    _gcs_sync_wrapper,
    _telemetry_context,
    mirror_gcs_methods,
    with_telemetry_context,
    wrap_file_methods,
)

# Captured at import time, before the conftest autouse fixture stubs detect().
_REAL_ENVIRONMENT_DETECT = EnvironmentDetector.detect
_FW_ENV = frozenset({Dimension.FRAMEWORK, Dimension.ENVIRONMENT})


def _fw_env(tracker: UsageMetricsTracker) -> dict[str, str]:
    return {
        k: v
        for k, v in tracker.collect_tokens_map(_FW_ENV, call_scoped_only=False).items()
        if v
    }


# ============================================================================
# 1. Framework Detector Tests
# ============================================================================


def _create_mock_frame(module_name: str, back_frame=None):
    """Helper to create a fake execution frame for testing stack traversal."""
    frame = types.SimpleNamespace()
    frame.f_globals = {"__name__": module_name}
    frame.f_back = back_frame
    return frame


def test_framework_detector_known_frameworks_mapping():
    with pytest.raises(NotImplementedError):
        BaseDetector().detect()

    detector = FrameworkDetector()
    assert detector.KNOWN_FRAMEWORKS["pandas"] == "pandas"
    assert detector.KNOWN_FRAMEWORKS["polars"] == "polars"
    assert detector.KNOWN_FRAMEWORKS["dask"] == "dask"
    assert detector.KNOWN_FRAMEWORKS["lightning"] == "lightning"
    assert detector.KNOWN_FRAMEWORKS["pytorch_lightning"] == "lightning"


def test_framework_detector_top_to_bottom_traversal(monkeypatch):
    detector = FrameworkDetector()

    # Stack: root (__main__) -> dask.dataframe -> pandas.io -> pyarrow.parquet -> gcsfs.core
    f_root = _create_mock_frame("__main__")
    f_dask = _create_mock_frame("dask.dataframe.io.parquet", f_root)
    f_pandas = _create_mock_frame("pandas.io.parquet", f_dask)
    f_pyarrow = _create_mock_frame("pyarrow.parquet", f_pandas)
    f_gcsfs = _create_mock_frame("gcsfs.core", f_pyarrow)

    monkeypatch.setattr(sys, "_getframe", lambda: f_gcsfs)
    assert detector.detect() == "dask"


def test_framework_detector_ignores_internal_and_test_prefixes(monkeypatch):
    detector = FrameworkDetector()

    # Stack: root (__main__) -> pytest -> unittest -> asyncio -> fsspec -> gcsfs
    f_root = _create_mock_frame("__main__")
    f_pytest = _create_mock_frame("pytest.runner", f_root)
    f_asyncio = _create_mock_frame("asyncio.events", f_pytest)
    f_fsspec = _create_mock_frame("fsspec.asyn", f_asyncio)
    f_gcsfs = _create_mock_frame("gcsfs.core", f_fsspec)

    monkeypatch.setattr(sys, "_getframe", lambda: f_gcsfs)
    assert detector.detect() is None


@pytest.mark.parametrize(
    "module_names, expected",
    [
        # FsspecArtifactService I/O runs in a worker thread via `asyncio.to_thread`.
        (
            (
                "threading",
                "concurrent.futures.thread",
                "google.adk.integrations.fsspec._fsspec_artifact_service",
                "fsspec.spec",
                "gcsfs.core",
                "google.auth.transport.requests",
            ),
            "adk",
        ),
        # Outermost framework wins in both directions.
        (("google.adk.runners", "pandas.io.parquet", "gcsfs.core"), "adk"),
        (("ray.serve", "google.adk.runners", "gcsfs.core"), "ray"),
        # Other `google` packages and lookalikes are not frameworks.
        (("google", "gcsfs.core"), None),
        (("google.auth.transport.requests", "gcsfs.core"), None),
        (("google.cloud.storage.client", "gcsfs.core"), None),
        (("google.adkx.tools", "gcsfs.core"), None),
        (("google_adk", "gcsfs.core"), None),
    ],
)
def test_framework_detector_google_adk(monkeypatch, module_names, expected):
    frame = None
    for module_name in module_names:
        frame = _create_mock_frame(module_name, frame)

    monkeypatch.setattr(sys, "_getframe", lambda: frame)
    assert FrameworkDetector().detect() == expected


def test_framework_detector_handles_empty_and_invalid_frames(monkeypatch):
    detector = FrameworkDetector()

    monkeypatch.setattr(sys, "_getframe", lambda: None)
    assert detector.detect() is None

    # Frame with missing or empty __name__
    empty_frame = types.SimpleNamespace(f_globals={}, f_back=None)
    monkeypatch.setattr(sys, "_getframe", lambda: empty_frame)
    assert detector.detect() is None

    # sys._getframe raises exception on unsupported platforms / frames
    def mock_raises():
        raise ValueError("call stack is not deep enough")

    monkeypatch.setattr(sys, "_getframe", mock_raises)
    assert detector.detect() is None


def test_telemetry_tracker_opt_out(monkeypatch):
    class DummyEnvDetector(BaseDetector):
        name = "env"

        def detect(self):
            return "gke"

    tracker = UsageMetricsTracker(detectors=[DummyEnvDetector(), FrameworkDetector()])

    # Default: active
    assert tracker.is_enabled()
    decoded = _decode_features(tracker.encode_features())
    assert decoded.environment == GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE

    # With GCSFS_NO_TELEMETRY=1
    monkeypatch.setenv("GCSFS_NO_TELEMETRY", "1")
    assert not tracker.is_enabled()
    assert tracker.collect_tokens_map(call_scoped_only=False) == {}
    assert tracker.encode_features() is None

    # With GCSFS_NO_TELEMETRY=true
    monkeypatch.setenv("GCSFS_NO_TELEMETRY", "true")
    assert not tracker.is_enabled()
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)

    # With DO_NOT_TRACK=1
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    assert not tracker.is_enabled()
    assert tracker.collect_tokens_map(call_scoped_only=False) == {}
    assert tracker.encode_features() is None

    # With DO_NOT_TRACK=true
    monkeypatch.setenv("DO_NOT_TRACK", "true")
    assert not tracker.is_enabled()
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)


def test_deep_stack_traversal(monkeypatch):
    """Verify stack frame detector traverses deep stacks up to depth 64."""
    detector = FrameworkDetector(max_depth=64)

    # Build a 50-frame stack where the top frame (frame 50) is 'lightning'
    current_frame = _create_mock_frame("lightning.pytorch.trainer")
    for i in range(49):
        current_frame = _create_mock_frame(
            f"internal_module_{i}", back_frame=current_frame
        )

    monkeypatch.setattr(sys, "_getframe", lambda: current_frame)
    detected = detector.detect()
    assert detected == "lightning"


# ============================================================================
# 2. Context Management & Concurrency Tests
# ============================================================================


def test_set_and_get_telemetry_context():
    # Initial state
    assert get_telemetry_context().get(Dimension.FRAMEWORK) is None

    # Set caller framework
    token = set_telemetry_context(Dimension.FRAMEWORK, "custom-engine")
    assert get_telemetry_context().get(Dimension.FRAMEWORK) == "custom-engine"

    # Reset
    reset_telemetry_context(token)
    assert get_telemetry_context().get(Dimension.FRAMEWORK) is None


def test_multithreaded_context_isolation():
    """
    Verify that concurrent threads executing with distinct framework contexts
    remain completely isolated without cross-thread ContextVar leakage or overlap.
    """
    import concurrent.futures
    import threading
    import time

    barrier = threading.Barrier(4)
    errors = []

    def worker(framework_name: str):
        try:
            # 1. Verify clean initial state in this thread
            if get_telemetry_context().get(Dimension.FRAMEWORK) is not None:
                errors.append(f"{framework_name}: initial context not None")

            # 2. Set thread-local context
            token = set_telemetry_context(Dimension.FRAMEWORK, framework_name)
            try:
                # 3. Synchronize all threads so all 4 contexts are simultaneously active
                barrier.wait(timeout=5)

                # 4. Repeatedly verify context during concurrent overlap
                for _ in range(5):
                    current = get_telemetry_context().get(Dimension.FRAMEWORK)
                    if current != framework_name:
                        errors.append(
                            f"{framework_name}: expected {framework_name}, got {current}"
                        )
                    time.sleep(0.005)
            finally:
                reset_telemetry_context(token)

            # 5. Verify thread context is cleanly reset
            if get_telemetry_context().get(Dimension.FRAMEWORK) is not None:
                errors.append(f"{framework_name}: post-reset context not None")
        except Exception as e:
            errors.append(f"{framework_name} exception: {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(worker, name) for name in ["pandas", "torch", "dask", "ray"]
        ]
        concurrent.futures.wait(futures)

    assert errors == [], f"Thread isolation errors: {errors}"


def test_post_fork_telemetry_reset():
    """Verify os.register_at_fork automatically clears telemetry in a real forked child process."""
    if not hasattr(os, "fork"):
        pytest.skip("os.fork is only supported on Unix platforms")

    import multiprocessing

    token = set_telemetry_context(Dimension.FRAMEWORK, "parent-process")
    try:
        assert get_telemetry_context() == {"fw": "parent-process"}

        ctx = multiprocessing.get_context("fork")
        result_queue = ctx.Queue()

        def child_worker(q):
            # In the forked child, os.register_at_fork(after_in_child=...) automatically runs
            q.put(get_telemetry_context())

        p = ctx.Process(target=child_worker, args=(result_queue,))
        p.start()
        p.join(timeout=5)

        child_result = result_queue.get(timeout=5)
        # Child process must have an empty telemetry context
        assert child_result == {}, f"Child telemetry was not reset! Got: {child_result}"

        # Parent process still retains its original telemetry context
        assert get_telemetry_context().get(Dimension.FRAMEWORK) == "parent-process"

        # Also directly exercise the registered at_fork callback for coverage tracing
        from gcsfs.telemetry.context import _reset_telemetry_in_child

        _reset_telemetry_in_child()
        assert get_telemetry_context() == {}
    finally:
        reset_telemetry_context(token)


# ============================================================================
# 3. UsageMetricsTracker Tests
# ============================================================================


def test_multidimensional_telemetry_registration():
    class DummyEnvDetector(BaseDetector):
        name = "env"

        def detect(self):
            return "gke"

    tracker = UsageMetricsTracker(detectors=[DummyEnvDetector(), FrameworkDetector()])
    assert (
        _decode_features(tracker.encode_features()).environment
        == GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE
    )

    # When framework is active in context
    token = set_telemetry_context(Dimension.FRAMEWORK, "pandas")
    try:
        decoded = _decode_features(tracker.encode_features())
        assert decoded.environment == GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE
        assert decoded.framework == GcsfsFramework.GCSFS_FRAMEWORK_PANDAS
        assert tracker.get_dimension(Dimension.FRAMEWORK) == "pandas"
    finally:
        reset_telemetry_context(token)


def test_unrecognized_and_malicious_context_values_never_reach_headers(monkeypatch):
    """Verify arbitrary, malformed, and CRLF context values are never emitted in HTTP headers."""
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)
    tracker = UsageMetricsTracker(detectors=[])
    monkeypatch.setattr(manager_mod, "default_usage_tracker", tracker)
    fs = GCSFileSystem(token="anon", skip_instance_cache=True)

    for invalid in (
        "pandas\r\nInjected-Header: bad",
        "my/custom/lib",
        "///",
        "a" * 100,
    ):
        token = set_telemetry_context(Dimension.FRAMEWORK, invalid)
        try:
            assert tracker.encode_features() is None
            headers = fs._get_headers(None)
            assert headers["User-Agent"] == f"python-gcsfs/{version}"
            assert HTTP_HEADER_KEY not in headers
        finally:
            reset_telemetry_context(token)

    # Whitespace/case-normalized valid enum values still encode into the feature header only.
    token = set_telemetry_context(Dimension.FRAMEWORK, "  PANDAS ")
    try:
        headers = fs._get_headers(None)
        assert headers["User-Agent"] == f"python-gcsfs/{version}"
        decoded = _decode_features(headers[HTTP_HEADER_KEY])
        assert decoded.framework == GcsfsFramework.GCSFS_FRAMEWORK_PANDAS
    finally:
        reset_telemetry_context(token)


def test_get_dimension_strips_whitespace_and_rejects_non_strings():
    """Verify get_dimension strips surrounding whitespace and returns None for blank/non-string values."""

    class _RawValueDetector(BaseDetector):
        name = Dimension.FRAMEWORK

        def __init__(self, value):
            self.value = value

        def detect(self):
            return self.value

    assert (
        UsageMetricsTracker(detectors=[_RawValueDetector("  pandas  ")]).get_dimension(
            Dimension.FRAMEWORK
        )
        == "pandas"
    )
    assert (
        UsageMetricsTracker(detectors=[_RawValueDetector("   ")]).get_dimension(
            Dimension.FRAMEWORK
        )
        is None
    )
    assert (
        UsageMetricsTracker(detectors=[_RawValueDetector(123)]).get_dimension(
            Dimension.FRAMEWORK
        )
        is None
    )


def test_collect_tokens_map_caches_detector_exception():
    """Verify that if a detector raises an exception, it caches empty string to prevent repeated calls."""
    call_count = 0

    class FailingDetector(BaseDetector):
        name = Dimension.FRAMEWORK

        def detect(self):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("Frame traversal failed unexpectedly")

    tracker = UsageMetricsTracker(detectors=[FailingDetector()])

    tokens1 = tracker.collect_tokens_map(call_scoped_only=False)
    assert tokens1 == {Dimension.FRAMEWORK.value: ""}
    assert call_count == 1

    # Second call
    t = set_telemetry_context(tokens1)
    try:
        tokens2 = tracker.collect_tokens_map(call_scoped_only=False)
    finally:
        reset_telemetry_context(t)
    assert tokens2 == {Dimension.FRAMEWORK.value: ""}
    assert call_count == 1


def test_warm_up_and_get_dimension_handle_failing_and_missing_detectors():
    calls = 0

    class FailingBackgroundDetector(BaseDetector):
        name = Dimension.ENVIRONMENT
        call_scoped = False

        def detect(self):
            nonlocal calls
            calls += 1
            raise RuntimeError("probe init failed")

    tracker = UsageMetricsTracker(detectors=[FailingBackgroundDetector()])
    tracker.warm_up()
    assert calls == 1
    assert tracker.get_dimension(Dimension.ENVIRONMENT) is None
    assert calls == 2
    assert tracker.get_dimension(Dimension.FRAMEWORK) is None


def test_collect_tokens_map_records_empty_for_none_detection():
    """Verify that collect_tokens_map records empty string for None detection and avoids redundant checks."""
    call_count = 0

    class DummyNoneDetector(BaseDetector):
        name = Dimension.FRAMEWORK

        def detect(self):
            nonlocal call_count
            call_count += 1
            return None

    tracker = UsageMetricsTracker(detectors=[DummyNoneDetector()])
    tokens_map = tracker.collect_tokens_map(call_scoped_only=False)
    assert tokens_map == {"fw": ""}
    assert call_count == 1

    # When the captured tokens_map is bridged into ContextVar:
    token = set_telemetry_context(tokens_map)
    try:
        # All downstream operations on event loop (encode_features, get_dimension, collect_tokens_map)
        # must now be O(1) without re-running detect()
        assert tracker.encode_features() is None
        assert tracker.get_dimension(Dimension.FRAMEWORK) is None
        tokens_map_2 = tracker.collect_tokens_map(call_scoped_only=False)
        assert tokens_map_2 == {"fw": ""}
        assert call_count == 1  # Still 1, no additional detect() calls!
    finally:
        reset_telemetry_context(token)


class _CallScopedFrameworkDetector(BaseDetector):
    name = Dimension.FRAMEWORK

    def detect(self):
        return "pandas"


class _ProcessScopedEnvDetector(BaseDetector):
    """Process-wide detector whose value can change mid-call, like an in-flight probe."""

    name = Dimension.ENVIRONMENT
    call_scoped = False

    def __init__(self, value="gke"):
        self.value = value
        self.calls = 0

    def detect(self):
        self.calls += 1
        return self.value


def _tracker_with(env_detector):
    return UsageMetricsTracker(detectors=[_CallScopedFrameworkDetector(), env_detector])


def test_collect_tokens_map_requires_call_scoped_only():
    tracker = _tracker_with(_ProcessScopedEnvDetector())

    with pytest.raises(TypeError):
        tracker.collect_tokens_map()


def test_collect_tokens_map_call_scoped_only_skips_process_scoped_detectors():
    env_detector = _ProcessScopedEnvDetector()
    tracker = _tracker_with(env_detector)

    assert tracker.collect_tokens_map(call_scoped_only=True) == {"fw": "pandas"}
    assert env_detector.calls == 0

    assert tracker.collect_tokens_map(call_scoped_only=False) == {
        "fw": "pandas",
        "env": "gke",
    }
    assert env_detector.calls == 1


def test_encode_features_reads_process_scoped_detectors_every_request():
    """Encoded features include env and re-read it per call, even inside a per-call context."""
    env_detector = _ProcessScopedEnvDetector()
    tracker = _tracker_with(env_detector)

    first = _decode_features(tracker.encode_features())
    assert (first.framework, first.environment) == (
        GcsfsFramework.GCSFS_FRAMEWORK_PANDAS,
        GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE,
    )

    token = set_telemetry_context(tracker.collect_tokens_map(call_scoped_only=True))
    try:
        assert get_telemetry_context() == {"fw": "pandas"}
        for _ in range(2):
            decoded = _decode_features(tracker.encode_features())
            assert (decoded.framework, decoded.environment) == (
                GcsfsFramework.GCSFS_FRAMEWORK_PANDAS,
                GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE,
            )
    finally:
        reset_telemetry_context(token)
    assert env_detector.calls == 3


def test_explicit_context_value_wins_over_process_scoped_detector():
    env_detector = _ProcessScopedEnvDetector()
    tracker = _tracker_with(env_detector)

    token = set_telemetry_context({"env": "gce"})
    try:
        assert tracker.collect_tokens_map(call_scoped_only=True) == {
            "env": "gce",
            "fw": "pandas",
        }
        decoded = _decode_features(tracker.encode_features())
        assert (decoded.framework, decoded.environment) == (
            GcsfsFramework.GCSFS_FRAMEWORK_PANDAS,
            GcsfsEnvironment.GCSFS_ENVIRONMENT_GCE,
        )
    finally:
        reset_telemetry_context(token)
    assert env_detector.calls == 0


def _check_env_resolves_mid_call(tracker, env_detector):
    # The per-call snapshot holds only call-scoped dimensions while env is unresolved.
    ctx = get_telemetry_context()
    assert Dimension.ENVIRONMENT.value not in ctx
    assert ctx.get(Dimension.FRAMEWORK.value) == "pandas"
    m = ctx.get(Dimension.METHOD.value, "")
    expected_method = (
        GcsfsMethod[f"GCSFS_METHOD_{m.upper()}"]
        if m
        else GcsfsMethod.GCSFS_METHOD_UNSPECIFIED
    )
    before = _decode_features(tracker.encode_features())
    assert (before.framework, before.environment, before.method) == (
        GcsfsFramework.GCSFS_FRAMEWORK_PANDAS,
        GcsfsEnvironment.GCSFS_ENVIRONMENT_UNSPECIFIED,
        expected_method,
    )
    # The background probe resolves mid-call; later requests in the same call pick it up.
    env_detector.value = "gke"
    after = _decode_features(tracker.encode_features())
    assert (after.framework, after.environment, after.method) == (
        GcsfsFramework.GCSFS_FRAMEWORK_PANDAS,
        GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE,
        expected_method,
    )


def _run_in_telemetry_context(check):
    with_telemetry_context(check)()


def _run_in_native_async_gen(check):
    async def steps():
        check()
        yield None

    async def consume():
        return [item async for item in _gcs_async_wrapper(steps)()]

    asyncio.run(consume())


def _run_in_sync_gen(check):
    class GenFS(fsspec.asyn.AsyncFileSystem):
        async def _steps(self):
            check()
            yield None

        steps = _gcs_sync_wrapper(_steps)

    list(GenFS(skip_instance_cache=True).steps())


def _run_in_file_context(check):
    class SnapshotFile:
        def __init__(self):
            self.caller_framework = "pandas"
            self.caller_method = "open"

        def read(self):
            check()

    wrap_file_methods(SnapshotFile)
    SnapshotFile().read()


@pytest.mark.parametrize(
    "run_call",
    [
        _run_in_telemetry_context,
        _run_in_native_async_gen,
        _run_in_sync_gen,
        _run_in_file_context,
    ],
    ids=["telemetry_context", "native_async_gen", "sync_gen", "file_context"],
)
def test_per_call_snapshot_never_freezes_unresolved_env(monkeypatch, run_call):
    """Each wrapper snapshot site leaves env out, so an env that resolves mid-call is sent."""
    env_detector = _ProcessScopedEnvDetector(value=None)
    tracker = _tracker_with(env_detector)
    monkeypatch.setattr(manager_mod, "default_usage_tracker", tracker)

    run_call(lambda: _check_env_resolves_mid_call(tracker, env_detector))

    assert env_detector.value == "gke"
    assert get_telemetry_context() == {}


# ============================================================================
# 4. Method Wrappers & Context Bridging Tests
# ============================================================================


class _CountingDetector(BaseDetector):
    """Framework detector that returns `value` and records the thread of every call."""

    name = Dimension.FRAMEWORK

    def __init__(self, value="pandas"):
        self.value = value
        self.calls = []

    def detect(self):
        self.calls.append(threading.get_ident())
        return self.value


@pytest.fixture
def detector(monkeypatch):
    """Install a counting framework detector on the default tracker for one test."""
    counting = _CountingDetector()
    monkeypatch.setattr(
        manager_mod, "default_usage_tracker", UsageMetricsTracker(detectors=[counting])
    )
    return counting


class _CountingEnvDetector(BaseDetector):
    """Environment detector that counts invocations and returns `value`."""

    name = Dimension.ENVIRONMENT
    call_scoped = False

    def __init__(self, value="gke"):
        self.value = value
        self.calls = 0

    def detect(self):
        self.calls += 1
        return self.value


@contextlib.contextmanager
def _scoped_ctx(tokens, value=None):
    token = set_telemetry_context(tokens, value)
    try:
        yield
    finally:
        reset_telemetry_context(token)


class _DummyFile(fsspec.spec.AbstractBufferedFile):
    def __init__(self, *args, **kwargs):
        self.caller_framework = (
            manager_mod.default_usage_tracker.get_dimension(Dimension.FRAMEWORK) or ""
        )
        super().__init__(*args, **kwargs)

    def _fetch_range(self, start, end):
        return self.fs.cat_file(self.path, start=start, end=end)


wrap_file_methods(_DummyFile)


class _DummyFS(fsspec.asyn.AsyncFileSystem):
    """Fake async filesystem whose leaf calls record (thread, fw/env map, method)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen, self.seen_telemetry = [], []

    def clear(self):
        self.seen.clear()
        self.seen_telemetry.clear()

    def _record(self):
        tracker = manager_mod.default_usage_tracker
        self.seen.append((threading.get_ident(), _fw_env(tracker)))
        self.seen_telemetry.append(tracker.get_dimension(Dimension.METHOD))

    async def _ls(self, path, detail=False, prefix="", **kwargs):
        self._record()
        return (
            [{"name": f"{path}/a.txt", "size": 3, "type": "file"}] if detail else [path]
        )

    async def _info(self, path, **kwargs):
        self._record()
        return {"name": path, "size": 3, "type": "file"}

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        self._record()
        return b"abc"[start:end]

    async def _rm_file(self, path, **kwargs):
        self._record()

    async def _cp_file(self, path1, path2, **kwargs):
        self._record()

    async def _list_objects(self, path, **kwargs):
        self._record()
        return [{"name": f"{path}/a.txt", "size": 3, "type": "file"}]

    async def open_async(self, path, mode="rb", **kwargs):
        self._record()
        return b"async"

    def created(self, path):
        return self.info(path)["name"]

    def modified(self, path):
        return self.info(path)["name"]

    def sign(self, path, expiration=100, **kwargs):
        self._record()
        return f"https://signed/{path}"

    def _open(self, path, mode="rb", **kwargs):
        self._record()
        return _DummyFile(self, path, mode=mode, size=3, cache_type="none")

    async def _walk(self, path, maxdepth=None, **kwargs):
        for i in range(4):
            await self._ls(f"{path}/dir_{i}", detail=True)
            yield (f"{path}/dir_{i}", [], [f"file_{i}.txt"])


mirror_gcs_methods(_DummyFS)


@pytest.mark.asyncio
async def test_gcs_async_wrapper_scopes_context(detector):
    """Verify that _gcs_async_wrapper scopes context so inner HTTP requests skip stack detection."""

    # 1. Simulate an async file operation (like _cat_file) making 5 inner HTTP requests
    async def simulated_cat_file():
        for _ in range(5):
            # Each HTTP request calls encode_features() for X-Goog-Storage-Gcsfs-Features
            assert manager_mod.default_usage_tracker.encode_features() == "EAE="
        return "done"

    # 2. Wrap with _gcs_async_wrapper
    wrapped_cat_file = _gcs_async_wrapper(simulated_cat_file)

    # 3. Run the operation
    assert await wrapped_cat_file() == "done"

    # 4. Check: detect() ran ONCE at entry (instead of 5 times for 5 HTTP requests)
    assert len(detector.calls) == 1

    # 5. Check: ContextVar is cleanly reset after the operation finishes
    assert get_telemetry_context() == {}


@pytest.mark.asyncio
async def test_consecutive_native_async_calls_isolated(detector):
    """Verify that consecutive native async calls under different frameworks are 100% isolated."""
    captured = []

    async def file_operation():
        captured.append(get_telemetry_context().get("fw"))

    wrapped = _gcs_async_wrapper(file_operation)

    detector.value = "pandas"
    await wrapped()
    assert captured[-1] == "pandas"
    assert get_telemetry_context() == {}

    detector.value = "torch"
    await wrapped()
    assert captured[-1] == "torch"
    assert get_telemetry_context() == {}


def test_async_gen_wrapper_aclose_on_early_exit(detector):
    """Verify that _gcs_sync_wrapper closes the underlying async generator on early exit, with telemetry."""
    closed_with = []

    class GenFS(fsspec.asyn.AsyncFileSystem):
        async def _sample_async_gen(self):
            try:
                yield 1
                yield 2
                yield 3
            finally:
                # Has telemetry only when closed by the wrapper, not by garbage collection
                closed_with.append(get_telemetry_context())

        sample_async_gen = _gcs_sync_wrapper(_sample_async_gen)

    gen_instance = GenFS(skip_instance_cache=True).sample_async_gen()
    val = next(gen_instance)
    assert val == 1
    assert closed_with == []
    # Close the sync generator early (simulating break in for loop)
    gen_instance.close()
    assert closed_with == [{"fw": "pandas", "m": ""}]


@pytest.mark.asyncio
async def test_async_gen_detects_framework_only_once(detector):
    """Verify sync walk and native `async for _walk` detect framework only once across all yields."""
    fs = _DummyFS(skip_instance_cache=True)

    # 1. Sync walk across 4 yielded directories -> detect() must run strictly ONCE
    assert len(list(fs.walk("bucket"))) == 4
    assert len(detector.calls) == 1
    assert [tokens for _, tokens in fs.seen] == [{"fw": "pandas"}] * 4
    assert fs.seen_telemetry == ["walk"] * 4

    # 2. Native async _walk across 4 yielded directories -> detect() must run strictly ONCE
    fs.clear()
    detector.calls.clear()
    assert len([item async for item in fs._walk("bucket")]) == 4
    assert len(detector.calls) == 1

    # Every inner _ls call carried the framework and top-level walk method, and nothing leaked afterwards
    assert [tokens for _, tokens in fs.seen] == [{"fw": "pandas"}] * 4
    assert fs.seen_telemetry == ["walk"] * 4
    assert get_telemetry_context() == {}


@pytest.mark.asyncio
async def test_native_async_gen_scopes_context_per_step_and_closes(detector):
    """Verify native async gen wrapper scopes context to each step (no leak to consumer) and closes on early exit."""
    closed_with = []
    seen_inside = []

    async def sample_async_gen():
        try:
            for i in range(3):
                seen_inside.append(get_telemetry_context())
                yield i
        finally:
            closed_with.append(get_telemetry_context())

    agen = _gcs_async_wrapper(sample_async_gen)()
    assert await agen.__anext__() == 0
    # Context is active inside the generator step but never leaks to the consumer between yields
    assert seen_inside == [{"fw": "pandas", "m": ""}]
    assert get_telemetry_context() == {}

    # Early exit (like `break` in `async for`) closes the underlying generator with context active
    await agen.aclose()
    assert closed_with == [{"fw": "pandas", "m": ""}]


def test_file_inherited_methods_propagate_caller_framework_across_threads(detector):
    """
    Verify inherited AbstractBufferedFile methods (e.g. read) called on another thread
    (pyarrow IO thread, dask worker) carry the file's caller_framework without re-detecting,
    including ZonalFile-style overrides that call raw fsspec.asyn.sync.
    """

    class LeafFile(fsspec.spec.AbstractBufferedFile):
        def _fetch_range(self, start, end):
            return self.fs.cat_file(self.path, start=start, end=end)

    class ZonalLikeFile(LeafFile):
        def _fetch_range(self, start, end):
            # Same pattern as ZonalFile: raw asyn.sync instead of a wrapped fs method
            return fsspec.asyn.sync(
                self.fs.loop, self.fs._cat_file, self.path, start, end
            )

    wrap_file_methods(LeafFile)
    wrap_file_methods(ZonalLikeFile)

    for file_cls in (LeafFile, ZonalLikeFile):
        fs = _DummyFS(skip_instance_cache=True)
        with file_cls(fs, "bucket/f.txt", mode="rb", size=3, cache_type="none") as f:
            # Differs from the detector's "pandas", so a re-detection would show up
            f.caller_framework = "torch"
            out = []
            t = threading.Thread(target=lambda: out.append(f.read()))
            t.start()
            t.join()
        assert out == [b"abc"], file_cls.__name__
        assert [tokens for _, tokens in fs.seen] == [{"fw": "torch"}], file_cls.__name__
        assert fs.seen_telemetry == ["file_read"], file_cls.__name__
    assert detector.calls == []


@pytest.mark.asyncio
async def test_file_telemetry_wrapper_and_wrap_file_methods():
    """Verify wrap_file_methods and _file_telemetry_wrapper cover sync and async methods."""

    class DummyFile:
        def __init__(self, caller_framework: str):
            self.caller_framework = caller_framework

        def sync_op(self):
            return (
                get_telemetry_context().get(Dimension.FRAMEWORK),
                get_telemetry_context().get(Dimension.METHOD),
            )

        async def async_op(self):
            return (
                get_telemetry_context().get(Dimension.FRAMEWORK),
                get_telemetry_context().get(Dimension.METHOD),
            )

        @property
        def details(self):
            return (
                get_telemetry_context().get(Dimension.FRAMEWORK),
                get_telemetry_context().get(Dimension.METHOD),
            )

        @staticmethod
        def static_helper(x):
            return x * 2

        @classmethod
        def class_helper(cls):
            return cls.__name__

    wrap_file_methods(DummyFile)

    f = DummyFile("pytorch")
    assert f.sync_op() == ("pytorch", None)
    assert f.sync_op() == ("pytorch", None)
    assert get_telemetry_context() == {}

    with _scoped_ctx(
        {Dimension.FRAMEWORK.value: "pytorch", Dimension.METHOD.value: "find"}
    ):
        assert f.sync_op() == ("pytorch", "find")
    with _scoped_ctx(Dimension.FRAMEWORK, "pytorch"):
        assert f.sync_op() == ("pytorch", None)

    assert f.details == ("pytorch", "file_details")
    assert get_telemetry_context() == {}

    assert await f.async_op() == ("pytorch", None)
    assert get_telemetry_context() == {}

    # Called through the instance: a wrapped staticmethod would receive `self` and fail
    assert f.static_helper(3) == 6
    assert f.class_helper() == "DummyFile"

    # Uninitialized instance without caller_framework/caller_method is a clean no-op
    bare = DummyFile.__new__(DummyFile)
    assert bare.sync_op() == (None, None)


@pytest.mark.asyncio
async def test_mirror_gcs_methods_and_subclass_polymorphism(monkeypatch, detector):
    """
    Verify mirror_gcs_methods binds sync/async/gen wrappers, wraps inherited batch
    coroutines, and preserves subclass overrides & mocks.
    """

    class SubDummyFS(_DummyFS):
        async def _ls(self, path, detail=False, **kwargs):
            return [f"sub:{path}"]

        async def _walk(self, path, maxdepth=None, **kwargs):
            yield (f"sub_walk:{path}", [], ["b.txt"])

        async def _internal_helper(self):
            return "internal"

        def info(self, path, **kwargs):
            self._record()
            return {"name": f"sub_sync:{path}", "size": 3, "type": "file"}

    mirror_gcs_methods(SubDummyFS)

    # Private internal helpers (not on AsyncFileSystem and no public sync counterpart) must not be wrapped,
    # whereas AsyncFileSystem async-only methods (_process_limits) are async-wrapped without a sync mirror.
    assert not getattr(SubDummyFS._internal_helper, "_is_gcs_async_wrapped", False)
    assert not hasattr(SubDummyFS, "internal_helper")
    assert getattr(_DummyFS._process_limits, "_is_gcs_async_wrapped", False)
    assert not hasattr(_DummyFS, "process_limits")

    sub_fs = SubDummyFS(skip_instance_cache=True)
    assert sub_fs.ls("bucket") == ["sub:bucket"]
    assert sub_fs.info("bucket/a")["name"] == "sub_sync:bucket/a"
    assert sub_fs.seen_telemetry[-1] == "info"
    assert list(sub_fs.walk("bucket")) == [("sub_walk:bucket", [], ["b.txt"])]

    # A plain sync override on an intermediate class survives mirroring a subclass.
    class ParentFS(_DummyFS):
        def info(self, path, **kwargs):
            self._record()
            return {"name": f"parent_sync:{path}", "size": 3, "type": "file"}

    class ChildFS(ParentFS):
        pass

    mirror_gcs_methods(ChildFS)
    child_fs = ChildFS(skip_instance_cache=True)
    assert child_fs.info("bucket/a")["name"] == "parent_sync:bucket/a"
    assert child_fs.seen_telemetry[-1] == "info"

    base_fs = _DummyFS(skip_instance_cache=True)

    # Inherited batch coroutine (_cat from AsyncFileSystem) detects framework ONCE across multiple files
    detector.calls.clear()
    batch_res = await base_fs._cat(["bucket/f1.txt", "bucket/f2.txt", "bucket/f3.txt"])
    assert len(batch_res) == 3
    assert len(detector.calls) == 1
    assert get_telemetry_context() == {}

    async def mocked_cat_file(self, path, start=None, end=None, **kwargs):
        return f"mocked:{path}".encode()

    monkeypatch.setattr(_DummyFS, "_cat_file", mocked_cat_file)
    assert base_fs.cat_file("bucket/file.txt") == b"mocked:bucket/file.txt"
    # SubDummyFS did not override _cat_file, so it inherits the monkeypatch on _DummyFS
    assert sub_fs.cat_file("bucket/file.txt") == b"mocked:bucket/file.txt"


def test_sync_wrapper_propagates_context_to_background_loop_thread(detector):
    """Verify sync wrappers capture context on caller thread and propagate it to fsspecIO thread."""
    caller_thread_id = threading.get_ident()
    fs = _DummyFS(skip_instance_cache=True)

    assert fs.ls("bucket") == ["bucket"]
    [(thread_id, tokens)] = fs.seen
    # Ran on the background fsspecIO thread, with the caller thread's framework and method
    assert thread_id != caller_thread_id
    assert tokens == {"fw": "pandas"}
    assert fs.seen_telemetry == ["ls"]
    # Detection ran once, on the caller thread: the loop thread's stack has no framework frames
    assert detector.calls == [caller_thread_id]
    assert get_telemetry_context() == {}


@pytest.mark.parametrize(
    "invoke, expected_method, expected_count",
    [
        pytest.param(
            lambda fs: fs.cat(["bucket/f1.txt", "bucket/f2.txt", "bucket/f3.txt"]),
            "cat",
            3,
            id="cat",
        ),
        pytest.param(
            lambda fs: fs.move("bucket/a.txt", "bucket/b.txt"), "move", 4, id="move"
        ),
        pytest.param(
            lambda fs: fs.mv("bucket/a.txt", "bucket/b.txt"), "mv", 4, id="mv"
        ),
        pytest.param(lambda fs: fs.created("bucket/a.txt"), "created", 1, id="created"),
        pytest.param(
            lambda fs: fs.modified("bucket/a.txt"), "modified", 1, id="modified"
        ),
        pytest.param(lambda fs: fs.sign("bucket/a.txt"), "sign", 1, id="sign"),
        pytest.param(lambda fs: fs.head("bucket/a.txt", size=2), "head", 2, id="head"),
        pytest.param(
            lambda fs: fs.read_text("bucket/a.txt"), "read_text", 2, id="read_text"
        ),
        pytest.param(lambda fs: fs.listdir("bucket"), "listdir", 1, id="listdir"),
        pytest.param(
            lambda fs: fs.read_bytes("bucket/a.txt"), "read_bytes", 1, id="read_bytes"
        ),
        pytest.param(lambda fs: fs.stat("bucket/a.txt"), "stat", 1, id="stat"),
        pytest.param(lambda fs: fs.lexists("bucket/a.txt"), "lexists", 1, id="lexists"),
        pytest.param(
            lambda fs: fs.rename("bucket/a.txt", "bucket/b.txt"),
            "rename",
            4,
            id="rename",
        ),
        pytest.param(lambda fs: fs.delete("bucket/a.txt"), "delete", 1, id="delete"),
    ],
)
def test_method_detection_top_level_wins_and_public_entrypoints(
    detector, invoke, expected_method, expected_count
):
    """Verify outermost public methods retain their own name over inner helpers."""
    fs = _DummyFS(skip_instance_cache=True)

    invoke(fs)

    assert fs.seen_telemetry == [expected_method] * expected_count
    assert [tokens for _, tokens in fs.seen] == [{"fw": "pandas"}] * expected_count
    assert len(detector.calls) == 1
    assert get_telemetry_context() == {}


@pytest.mark.asyncio
async def test_native_async_entrypoints_and_unwrapped_private_helper(detector):
    """Verify native async `_cat`, `open_async`, and unwrapped `_list_objects` behavior."""
    fs = _DummyFS(skip_instance_cache=True)

    await fs._cat(["bucket/f1.txt", "bucket/f2.txt", "bucket/f3.txt"])

    assert [tokens for _, tokens in fs.seen] == [{"fw": "pandas"}] * 3
    assert fs.seen_telemetry == ["cat"] * 3
    assert len(detector.calls) == 1

    fs.clear()
    await fs.open_async("bucket/a.txt")

    assert fs.seen_telemetry == ["open_async"]

    fs.clear()
    await fs._list_objects("bucket")

    assert fs.seen_telemetry == [None]

    fs.clear()
    with _scoped_ctx({Dimension.FRAMEWORK.value: "jax", Dimension.METHOD.value: "ls"}):
        await fs._list_objects("bucket")

    assert fs.seen_telemetry == ["ls"]
    assert get_telemetry_context() == {}


def test_fs_open_records_open_then_file_methods_without_leaking(detector):
    """Verify `fs.open` records `open` on init and `file_read` on subsequent reads."""
    fs = _DummyFS(skip_instance_cache=True)

    with fs.open("bucket/a.txt", "rb") as f:
        assert get_telemetry_context() == {}
        assert f.read() == b"abc"

    assert [tokens for _, tokens in fs.seen] == [{"fw": "pandas"}] * 2
    assert fs.seen_telemetry == ["open", "file_read"]


def test_with_telemetry_context_tracks_open_and_preserves_outer_method(detector):
    """Verify `@with_telemetry_context` tracks `_open`, skips `_helper`, and cleans up on error."""

    class _DirectContextFS(_DummyFS):
        @with_telemetry_context
        def _open(self, path, mode="rb", **kwargs):
            self._record()
            if path == "bucket/fail":
                raise RuntimeError("open failed")
            return path

        @with_telemetry_context
        def _helper(self):
            self._record()
            return "ok"

    direct_fs = _DirectContextFS(skip_instance_cache=True)

    direct_fs._open("bucket/a.txt")
    direct_fs._helper()
    with _scoped_ctx(
        {Dimension.FRAMEWORK.value: "jax", Dimension.METHOD.value: "find"}
    ):
        direct_fs._helper()

    assert direct_fs.seen_telemetry == ["open", None, "find"]

    with pytest.raises(RuntimeError, match="open failed"):
        direct_fs._open("bucket/fail")

    assert get_telemetry_context() == {}


@pytest.mark.asyncio
async def test_preset_framework_skips_detection_while_recording_method(detector):
    """Verify partial preset framework reuses its token without stack walking while capturing method."""
    fs = _DummyFS(skip_instance_cache=True)

    with _scoped_ctx(Dimension.FRAMEWORK, "jax"):
        await fs._cat(["bucket/f1.txt", "bucket/f2.txt"])
        assert [tokens for _, tokens in fs.seen] == [{"fw": "jax"}] * 2
        assert fs.seen_telemetry == ["cat", "cat"]

        fs.clear()
        assert fs.head("bucket/a.txt", size=2) == b"ab"
        assert fs.seen_telemetry == ["head", "head"]

        fs.clear()
        assert fs.read_text("bucket/a.txt") == "abc"
        assert fs.seen_telemetry == ["read_text", "read_text"]

        fs.clear()
        assert len(list(fs.walk("bucket"))) == 4
        assert fs.seen_telemetry == ["walk"] * 4

        assert detector.calls == []
        assert get_telemetry_context() == {Dimension.FRAMEWORK.value: "jax"}

    assert get_telemetry_context() == {}


def test_has_telemetry_context_and_wrapper_exception_cleanup(detector):
    """Verify `has_telemetry_context` states and ContextVar cleanup on wrapper/init exceptions."""
    from gcsfs.core import GCSFile, GCSFileSystem

    assert not has_telemetry_context()
    assert not has_telemetry_context(Dimension.METHOD)

    with _scoped_ctx(Dimension.FRAMEWORK, "pandas"):
        assert has_telemetry_context()
        assert has_telemetry_context(Dimension.FRAMEWORK)
        assert not has_telemetry_context(Dimension.METHOD)

    with _scoped_ctx(Dimension.METHOD, ""):
        assert has_telemetry_context()
        assert has_telemetry_context(Dimension.METHOD)

    class _FailingFS(_DummyFS):
        async def _cat_file(self, path, start=None, end=None, **kwargs):
            raise RuntimeError("cat failed")

    failing_fs = _FailingFS(skip_instance_cache=True)
    with pytest.raises(RuntimeError, match="cat failed"):
        failing_fs.cat_file("bucket/f.txt")
    assert get_telemetry_context() == {}

    real_fs = GCSFileSystem(token="anon", skip_instance_cache=True)
    with pytest.raises(NotImplementedError):
        GCSFile(real_fs, "bucket/f.txt", mode="invalid")
    assert get_telemetry_context() == {}


def test_http_headers_exclude_telemetry_from_user_agent_and_respect_opt_out(
    monkeypatch,
):
    """Verify User-Agent excludes all telemetry, feature header carries all dimensions, and opt-out holds."""
    tracker = UsageMetricsTracker()
    monkeypatch.setattr(manager_mod, "default_usage_tracker", tracker)
    fs = GCSFileSystem(token="anon", skip_instance_cache=True)
    with _scoped_ctx(
        {
            Dimension.FRAMEWORK.value: "pandas",
            Dimension.ENVIRONMENT.value: "gke",
            Dimension.METHOD.value: "find",
        }
    ):
        assert has_telemetry_context(Dimension.METHOD)
        assert tracker.collect_tokens_map(_FW_ENV, call_scoped_only=False) == {
            "fw": "pandas",
            "env": "gke",
        }
        assert tracker.get_dimension(Dimension.METHOD) == "find"
        headers = fs._get_headers(None)
        decoded = _decode_features(headers[HTTP_HEADER_KEY])
        assert headers["User-Agent"] == f"python-gcsfs/{version}"
        assert (decoded.method, decoded.framework, decoded.environment) == (
            GcsfsMethod.GCSFS_METHOD_FIND,
            GcsfsFramework.GCSFS_FRAMEWORK_PANDAS,
            GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE,
        )

        for env_var, env_val in (
            ("DO_NOT_TRACK", "1"),
            ("DO_NOT_TRACK", " TRUE "),
            ("GCSFS_NO_TELEMETRY", "yes"),
        ):
            with monkeypatch.context() as mp:
                mp.setenv(env_var, env_val)
                assert tracker.encode_features() is None
                assert tracker.get_dimension(Dimension.METHOD) is None
                assert tracker.collect_tokens_map(call_scoped_only=False) == {}
                disabled_headers = fs._get_headers(None)
                assert disabled_headers["User-Agent"] == f"python-gcsfs/{version}"
                assert HTTP_HEADER_KEY not in disabled_headers


def test_custom_detector_and_dimension_filtering(detector, monkeypatch):
    """Verify custom detectors respect dimension filtering and file handles cache missing detectors."""

    class _CustomAppDetector(BaseDetector):
        name = "app"

        def detect(self):
            return "myapp"

    custom_tracker = UsageMetricsTracker(detectors=[_CustomAppDetector()])
    assert custom_tracker.collect_tokens_map(
        frozenset({"app"}), call_scoped_only=False
    ) == {"app": "myapp"}
    with _scoped_ctx({Dimension.FRAMEWORK: "jax", "region": "us"}):
        assert custom_tracker.collect_tokens_map(_FW_ENV, call_scoped_only=False) == {
            "fw": "jax"
        }
        assert custom_tracker.collect_tokens_map(
            frozenset(Dimension), call_scoped_only=False
        ) == {"fw": "jax"}
        assert custom_tracker.collect_tokens_map(call_scoped_only=False) == {
            "fw": "jax",
            "region": "us",
            "app": "myapp",
        }
        decoded = _decode_features(custom_tracker.encode_features())
        assert decoded.framework == GcsfsFramework.GCSFS_FRAMEWORK_JAX

    # Empty call-scoped snapshot (only process-wide detectors registered) keeps env active.
    gke_only_tracker = UsageMetricsTracker(detectors=[_CountingEnvDetector("gke")])
    with monkeypatch.context() as mp:
        mp.setattr(manager_mod, "default_usage_tracker", gke_only_tracker)
        with _telemetry_context({}):
            assert get_telemetry_context() == {}
            assert (
                _decode_features(gke_only_tracker.encode_features()).environment
                == GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE
            )

    # Dimension-filtered queries skip unrelated detectors, and file handles skip stack re-walks.
    env_det = _CountingEnvDetector(None)
    multi_tracker = UsageMetricsTracker(detectors=[detector, env_det])
    with monkeypatch.context() as mp:
        mp.setattr(manager_mod, "default_usage_tracker", multi_tracker)
        detector.calls.clear()
        assert multi_tracker.get_dimension(Dimension.FRAMEWORK) == "pandas"
        assert len(detector.calls) == 1
        assert env_det.calls == 0

        class _ChunkFile:
            def __init__(self):
                self.caller_framework = "pandas"
                self.caller_method = "open"

            def read_chunks(self):
                return [_fw_env(multi_tracker) for _ in range(5)]

        wrap_file_methods(_ChunkFile)
        detector.calls.clear()
        assert _ChunkFile().read_chunks() == [{"fw": "pandas"}] * 5
        assert detector.calls == []
        assert env_det.calls == 5


def test_opted_out_fs_and_open_file_suppress_telemetry(
    detector, spied_gcs_harness, monkeypatch
):
    """Verify opted-out FS calls and open handles suppress telemetry dynamically and resume when unset."""
    gke_env = _CountingEnvDetector()
    monkeypatch.setattr(
        manager_mod,
        "default_usage_tracker",
        UsageMetricsTracker(detectors=[detector, gke_env]),
    )
    fs = _DummyFS(skip_instance_cache=True)
    core_fs = spied_gcs_harness.fs
    detector.calls.clear()

    with contextlib.ExitStack() as stack:
        with monkeypatch.context() as mp:
            mp.setenv("DO_NOT_TRACK", "TRUE")
            assert fs.ls("bucket") == ["bucket"]
            assert [tokens for _, tokens in fs.seen] == [{}]
            assert fs.seen_telemetry == [None]
            assert detector.calls == []
            assert gke_env.calls == 0

            spied_gcs_harness.clear()
            opted_out_file = stack.enter_context(
                core_fs.open(
                    "test-bucket/file.txt", "rb", block_size=2, cache_type="blockcache"
                )
            )
            assert opted_out_file.caller_framework == ""
            assert opted_out_file.caller_method == "open"
            assert len(opted_out_file.read(2)) == 2
            with _scoped_ctx(
                {
                    Dimension.FRAMEWORK.value: "ray",
                    Dimension.METHOD.value: "find",
                    "region": "us",
                }
            ):
                assert len(opted_out_file.read(2)) == 2
                opted_out_file._details = None
                assert int(opted_out_file.info()["size"]) > 0
            assert len(spied_gcs_harness.seen) >= 3
            assert all(entry == ({}, None) for entry in spied_gcs_harness.seen)
            assert spied_gcs_harness.feature_headers == [""] * len(
                spied_gcs_harness.seen
            )
            assert detector.calls == []
            assert gke_env.calls == 0

        spied_gcs_harness.clear()
        detector.calls.clear()
        assert get_telemetry_context() == {}
        assert len(opted_out_file.read(2)) == 2
        opted_out_file._details = None
        assert int(opted_out_file.info()["size"]) > 0
        assert len(spied_gcs_harness.seen) >= 2
        assert spied_gcs_harness.seen[0] == ({"env": "gke"}, "file_read")
        assert all(
            entry == ({"env": "gke"}, "file_info")
            for entry in spied_gcs_harness.seen[1:]
        )
        decoded_headers = [
            _decode_features(h) for h in spied_gcs_harness.feature_headers
        ]
        assert len(decoded_headers) == len(spied_gcs_harness.seen)
        assert (decoded_headers[0].environment, decoded_headers[0].method) == (
            GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE,
            GcsfsMethod.GCSFS_METHOD_FILE_READ,
        )
        assert all(
            (d.environment, d.method)
            == (
                GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE,
                GcsfsMethod.GCSFS_METHOD_FILE_INFO,
            )
            for d in decoded_headers[1:]
        )
        assert detector.calls == []
        assert gke_env.calls >= 2
    assert get_telemetry_context() == {}


@pytest.mark.parametrize(
    "outer_ctx",
    [None, {Dimension.FRAMEWORK.value: "jax", Dimension.METHOD.value: "find"}],
    ids=["no_outer_ctx", "with_outer_ctx"],
)
@pytest.mark.asyncio
async def test_opted_out_generators_and_nested_calls_suppress_telemetry(
    detector, monkeypatch, outer_ctx
):
    """Verify opted-out sync/async generators and nested calls negative-cache all detectors."""
    gke_env = _CountingEnvDetector()
    monkeypatch.setattr(
        manager_mod,
        "default_usage_tracker",
        UsageMetricsTracker(detectors=[detector, gke_env]),
    )
    fs = _DummyFS(skip_instance_cache=True)

    scope_cm = (
        _scoped_ctx(outer_ctx) if outer_ctx is not None else contextlib.nullcontext()
    )
    with scope_cm:
        with monkeypatch.context() as mp:
            mp.setenv("DO_NOT_TRACK", "1")
            sync_gen = fs.walk("bucket")
            sync_early = fs.walk("bucket")
            async_gen = fs._walk("bucket")
            async_early = fs._walk("bucket")

            assert next(sync_gen)[0] == "bucket/dir_0"
            assert next(sync_early)[0] == "bucket/dir_0"
            assert (await async_gen.__anext__())[0] == "bucket/dir_0"
            assert (await async_early.__anext__())[0] == "bucket/dir_0"
            assert fs.cat("bucket/a.txt") == b"abc"
            assert await fs._cat("bucket/b.txt") == b"abc"
            sync_early.close()
            await async_early.aclose()

            assert fs.seen
            assert all(tokens == {} for _, tokens in fs.seen)
            assert fs.seen_telemetry == [None] * len(fs.seen)
            assert detector.calls == []
            assert gke_env.calls == 0

        fs.clear()
        assert len(list(sync_gen)) == 3
        assert len([item async for item in async_gen]) == 3

        expected_ctx = dict(outer_ctx) if outer_ctx is not None else {}
        assert get_telemetry_context() == expected_ctx

    expected_method = outer_ctx[Dimension.METHOD.value] if outer_ctx else "walk"
    expected_fw = outer_ctx[Dimension.FRAMEWORK.value] if outer_ctx else "pandas"
    assert [tokens for _, tokens in fs.seen] == [{"fw": expected_fw, "env": "gke"}] * 6
    assert fs.seen_telemetry == [expected_method] * 6
    assert get_telemetry_context() == {}


# ============================================================================
# 5. GCSFileSystem & GCSFile Integration Tests
# ============================================================================


def _write_file_helper(fs, tmp_path, monkeypatch):
    with fs.open("test-bucket/file.txt", "wb") as f:
        f.write(b"data\n")
        f.writelines([b"data2\n"])
        f.flush()


def _get_file_concurrent_helper(fs, tmp_path, monkeypatch):
    monkeypatch.setattr(fs, "MIN_CHUNK_SIZE_FOR_CONCURRENCY", 1)

    async def _zero_threshold(bucket):
        return 0

    monkeypatch.setattr(fs, "_get_threshold_for_disk_reads", _zero_threshold)
    fs.get_file(
        "test-bucket/file.txt",
        str(tmp_path / "downloaded.bin"),
        concurrency=2,
        chunk_size=10,
    )


def _read_file_helper(fs, tmp_path, monkeypatch):
    with fs.open("test-bucket/file.txt", "rb") as f:
        return f.read(5)


class _CallerThreadOnlyDetector(BaseDetector):
    """
    Framework detector that only "sees" a framework on the thread that created it,
    mirroring real stack-walking: the fsspecIO loop thread never has user frames.
    Records the thread name of every call so tests can assert detection never ran there.
    """

    name = Dimension.FRAMEWORK

    def __init__(self, value):
        self.value = value
        self.caller_ident = threading.get_ident()
        self.thread_names = []

    def detect(self):
        self.thread_names.append(threading.current_thread().name)
        return self.value if threading.get_ident() == self.caller_ident else None


_COMMON_FS_OPS = [
    ("ls", lambda fs, tmp_path, monkeypatch: fs.ls("test-bucket"), "ls"),
    ("info", lambda fs, tmp_path, monkeypatch: fs.info("test-bucket/file.txt"), "info"),
    (
        "exists",
        lambda fs, tmp_path, monkeypatch: fs.exists("test-bucket/file.txt"),
        "exists",
    ),
    ("cat", lambda fs, tmp_path, monkeypatch: fs.cat("test-bucket/file.txt"), "cat"),
    (
        "cat_file",
        lambda fs, tmp_path, monkeypatch: fs.cat_file("test-bucket/file.txt"),
        "cat_file",
    ),
    (
        "head",
        lambda fs, tmp_path, monkeypatch: fs.head("test-bucket/file.txt", size=5),
        "head",
    ),
    (
        "tail",
        lambda fs, tmp_path, monkeypatch: fs.tail("test-bucket/file.txt", size=5),
        "tail",
    ),
    ("find", lambda fs, tmp_path, monkeypatch: fs.find("test-bucket"), "find"),
    (
        "walk",
        lambda fs, tmp_path, monkeypatch: list(fs.walk("test-bucket")),
        "walk",
    ),
    ("glob", lambda fs, tmp_path, monkeypatch: fs.glob("test-bucket/*"), "glob"),
    ("du", lambda fs, tmp_path, monkeypatch: fs.du("test-bucket"), "du"),
    ("size", lambda fs, tmp_path, monkeypatch: fs.size("test-bucket/file.txt"), "size"),
    (
        "read_bytes",
        lambda fs, tmp_path, monkeypatch: fs.read_bytes("test-bucket/file.txt"),
        {"read_bytes"},
    ),
    (
        "read_text",
        lambda fs, tmp_path, monkeypatch: fs.read_text("test-bucket/file.txt"),
        {"read_text"},
    ),
    (
        "write_bytes",
        lambda fs, tmp_path, monkeypatch: fs.write_bytes(
            "test-bucket/file.txt", b"data"
        ),
        {"write_bytes"},
    ),
    (
        "write_text",
        lambda fs, tmp_path, monkeypatch: fs.write_text("test-bucket/file.txt", "data"),
        {"write_text"},
    ),
    (
        "pipe_file",
        lambda fs, tmp_path, monkeypatch: fs.pipe_file("test-bucket/file.txt", b"x"),
        {"pipe_file"},
    ),
    ("rm", lambda fs, tmp_path, monkeypatch: fs.rm("test-bucket/file.txt"), {"rm"}),
    ("rmdir", lambda fs, tmp_path, monkeypatch: fs.rmdir("test-bucket"), {"rmdir"}),
    (
        "merge",
        lambda fs, tmp_path, monkeypatch: fs.merge(
            "test-bucket/merged.txt", ["test-bucket/a.txt", "test-bucket/b.txt"]
        ),
        {"merge"},
    ),
    ("buckets", lambda fs, tmp_path, monkeypatch: fs.buckets, {"buckets"}),
    ("open_read", _read_file_helper, {"open", "file_read"}),
    ("open_write", _write_file_helper, {"file_close"}),
    ("get_file_concurrent", _get_file_concurrent_helper, {"get_file"}),
]


@pytest.mark.parametrize(
    "mock_gcs_harness", ["core", "extended"], indirect=True, ids=["core", "extended"]
)
@pytest.mark.parametrize(
    "name,op,expected_method",
    _COMMON_FS_OPS,
    ids=[name for name, _, _ in _COMMON_FS_OPS],
)
def test_all_parent_methods_telemetry_coverage(
    spied_gcs_harness, tmp_path, monkeypatch, name, op, expected_method
):
    """
    Verify that inherited and overridden FileSystem and File methods (on both
    GCSFileSystem and ExtendedGcsFileSystem) detect the caller framework on the
    caller thread and propagate framework and method telemetry to every outgoing call.
    """
    fs = spied_gcs_harness.fs
    user_agents = spied_gcs_harness.user_agents
    feature_headers = spied_gcs_harness.feature_headers
    fs.dircache.clear()

    valid_frameworks = [
        m for m in GcsfsFramework if m != GcsfsFramework.GCSFS_FRAMEWORK_UNSPECIFIED
    ]
    idx = [n for n, _, _ in _COMMON_FS_OPS].index(name)
    expected_fw = valid_frameworks[idx % len(valid_frameworks)]
    fw_name = expected_fw.name.removeprefix("GCSFS_FRAMEWORK_").lower()
    detector = _CallerThreadOnlyDetector(fw_name)
    monkeypatch.setattr(
        manager_mod,
        "default_usage_tracker",
        UsageMetricsTracker(detectors=[detector]),
    )
    spied_gcs_harness.clear()

    op(fs, tmp_path, monkeypatch)

    assert len(user_agents) > 0, f"operation {name} did not execute any network calls"
    assert len(feature_headers) == len(user_agents)
    assert len(spied_gcs_harness.seen) > 0
    expected_set = (
        expected_method if isinstance(expected_method, set) else {expected_method}
    )
    expected_method_enums = {
        GcsfsMethod[f"GCSFS_METHOD_{m.upper()}"] for m in expected_set
    }
    for h in feature_headers:
        assert h, f"operation {name} had request missing {HTTP_HEADER_KEY}"
        decoded = _decode_features(h)
        assert (
            decoded.framework == expected_fw
        ), f"operation {name} expected {expected_fw.name}, got {decoded.framework.name}"
        assert (
            decoded.method in expected_method_enums
        ), f"operation {name} recorded unexpected method {decoded.method.name}"
    assert {
        m for _, m in spied_gcs_harness.seen
    } == expected_set, (
        f"operation {name} recorded unexpected methods: {spied_gcs_harness.seen}"
    )
    # Regression guard: stack-walking must never run on the fsspec loop thread,
    # where it can't find user frames and would add per-request overhead.
    loop_thread_name = fsspec.asyn.iothread[0].name
    assert (
        loop_thread_name not in detector.thread_names
    ), f"operation {name} ran framework detection on the loop thread: {detector.thread_names}"
    assert get_telemetry_context() == {}


def _is_async_fn(fn) -> bool:
    return inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn)


@pytest.mark.parametrize(
    "fs_cls_path",
    ["gcsfs.core.GCSFileSystem", "gcsfs.extended_gcsfs.ExtendedGcsFileSystem"],
    ids=["core", "extended"],
)
def test_filesystem_methods_are_telemetry_wrapped(fs_cls_path):
    """
    Verify, without a hardcoded method list, that every sync bridge and eligible native
    async method defined on the filesystem class carries a telemetry wrapper.

    - Any sync function defined on the class that wraps a coroutine (e.g. a leftover
      `foo = asyn.sync_wrapper(_foo)`, detected via `functools.wraps`' `__wrapped__`)
      must be a `_gcs_sync_wrapper`; otherwise it would skip caller-thread detection.
    - Any native async method that `mirror_gcs_methods` targets (public sync counterpart
      or AsyncFileSystem method) must be async-wrapped, and its public sync
      counterpart (if any) must be sync-wrapped.
    """
    import importlib

    module_name, cls_name = fs_cls_path.rsplit(".", 1)
    fs_cls = getattr(importlib.import_module(module_name), cls_name)

    unwrapped = set()
    for name, attr in fs_cls.__dict__.items():
        if name.startswith("__") or not inspect.isfunction(attr):
            continue

        # Sync bridges over coroutines (fsspec's sync_wrapper or ours)
        if not _is_async_fn(attr) and _is_async_fn(inspect.unwrap(attr)):
            if not getattr(attr, "_is_gcs_sync_wrapped", False):
                unwrapped.add(name)
            continue

        # Native async methods targeted by mirror_gcs_methods
        if name.startswith("_") and _is_async_fn(attr):
            smethod = name[1:]
            has_public_sync = hasattr(fsspec.spec.AbstractFileSystem, smethod) or (
                hasattr(fs_cls, smethod)
            )
            if not (has_public_sync or hasattr(fsspec.asyn.AsyncFileSystem, name)):
                continue
            if not getattr(attr, "_is_gcs_async_wrapped", False):
                unwrapped.add(name)
            public = getattr(fs_cls, smethod, None)
            if has_public_sync and not getattr(public, "_is_gcs_sync_wrapped", False):
                unwrapped.add(smethod)

    for name in _TRACKED_METHODS:
        raw_attr = inspect.getattr_static(fs_cls, name, None)
        if isinstance(raw_attr, property):
            if not getattr(raw_attr.fget, "_is_gcs_sync_wrapped", False):
                unwrapped.add(name)
            continue
        attr = getattr(fs_cls, name, None)
        expected_marker = (
            "_is_gcs_async_wrapped" if _is_async_fn(attr) else "_is_gcs_sync_wrapped"
        )
        if not getattr(attr, expected_marker, False):
            unwrapped.add(name)

    assert (
        not unwrapped
    ), f"{cls_name} methods missing telemetry wrappers: {sorted(unwrapped)}"


@pytest.fixture
def zonal_fs_harness(monkeypatch):
    """Patch Zonal/Extended GCS hooks and record telemetry across HTTP and gRPC ops."""
    from gcsfs import zb_hns_utils
    from gcsfs.core import GCSFileSystem as CoreGCSFileSystem
    from gcsfs.extended_gcsfs import ExtendedGcsFileSystem
    from gcsfs.tests import layouts

    data = b"0123456789"
    user_agents, feature_headers, grpc_ops, captured, mrd_md, aaow_md = (
        [],
        [],
        [],
        [],
        [],
        [],
    )

    def _rec(op_name):
        tracker = manager_mod.default_usage_tracker
        captured.append(
            (
                op_name,
                _fw_env(tracker),
                tracker.get_dimension(Dimension.METHOD),
            )
        )

    async def fake_call(self, method, path, *args, **kwargs):
        _rec("http_info")
        resolved = self._get_headers(kwargs.get("headers"))
        user_agents.append(resolved["User-Agent"])
        feature_headers.append(resolved.get(HTTP_HEADER_KEY, ""))
        return {
            "kind": "storage#object",
            "name": "obj",
            "bucket": "zonal-bucket",
            "size": str(len(data)),
            "generation": "1",
            "timeFinalized": "2026-01-01T00:00:00.000Z",
        }

    async def fake_layout(self, bucket):
        _rec("layout")
        return layouts.ZONAL_HIERARCHICAL

    async def fake_get_grpc_client(self):
        self._grpc_client = object()
        return self._grpc_client

    async def fake_create_mrd(
        grpc_client, bucket_name, object_name, generation=None, **kwargs
    ):
        mrd_md.append(kwargs.get("metadata"))
        return types.SimpleNamespace(persisted_size=len(data), close=mock.AsyncMock())

    orig_mrd_pool_close = zb_hns_utils.MRDPool.close

    async def fake_mrd_pool_close(self):
        _rec("mrd_close")
        grpc_ops.append(("mrd_pool.close", get_telemetry_context().copy()))
        await orig_mrd_pool_close(self)

    async def fake_cat_file(self, path, start=None, end=None, **kwargs):
        _rec("cat_file")
        grpc_ops.append(("_cat_file", get_telemetry_context().copy()))
        return data[start:end]

    async def fake_aaow_open(**kwargs):
        aaow_md.append(kwargs.get("metadata"))

    fake_writer = types.SimpleNamespace(
        open=fake_aaow_open,
        flush=mock.AsyncMock(),
        close=mock.AsyncMock(),
        _is_stream_open=True,
    )

    monkeypatch.setattr(CoreGCSFileSystem, "_call", fake_call)
    monkeypatch.setattr(ExtendedGcsFileSystem, "_lookup_bucket_layout", fake_layout)
    monkeypatch.setattr(ExtendedGcsFileSystem, "_get_grpc_client", fake_get_grpc_client)
    monkeypatch.setattr(
        zb_hns_utils.AsyncMultiRangeDownloader, "create_mrd", fake_create_mrd
    )
    monkeypatch.setattr(
        zb_hns_utils, "AsyncAppendableObjectWriter", lambda **kwargs: fake_writer
    )
    monkeypatch.setattr(ExtendedGcsFileSystem, "_cat_file", fake_cat_file)
    monkeypatch.setattr(zb_hns_utils.MRDPool, "close", fake_mrd_pool_close)

    ExtendedGcsFileSystem.clear_instance_cache()
    fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)
    try:
        yield types.SimpleNamespace(
            fs=fs,
            data=data,
            user_agents=user_agents,
            feature_headers=feature_headers,
            grpc_ops=grpc_ops,
            captured=captured,
            mrd_md=mrd_md,
            aaow_md=aaow_md,
            clear=lambda: (
                user_agents.clear(),
                feature_headers.clear(),
                grpc_ops.clear(),
                captured.clear(),
                mrd_md.clear(),
                aaow_md.clear(),
            ),
        )
    finally:
        ExtendedGcsFileSystem.clear_instance_cache()


def test_zonal_file_lifecycle_detects_framework_once(monkeypatch, zonal_fs_harness):
    """Verify Rapid (zonal) file lifecycle detects framework once and attaches stream-open metadata."""
    fw = "pandas"
    detector = _CallerThreadOnlyDetector(fw)
    monkeypatch.setattr(
        manager_mod, "default_usage_tracker", UsageMetricsTracker(detectors=[detector])
    )
    fs, data = zonal_fs_harness.fs, zonal_fs_harness.data

    with fs.open("zonal-bucket/obj", "rb", cache_type="none") as f:
        assert f.caller_framework == fw
        assert f.caller_method == "open"
        assert zonal_fs_harness.user_agents
        assert detector.thread_names == [threading.current_thread().name]
        assert f.read(4) == data[:4]
        f.seek(0)
        assert f.read(2) == data[:2]
        assert f.info()["size"] == len(data)
        f._details = None
        assert f.info()["size"] == len(data)

    assert detector.thread_names == [threading.current_thread().name]
    assert (
        len(zonal_fs_harness.feature_headers) == len(zonal_fs_harness.user_agents) > 0
    )
    assert all(
        _decode_features(h).framework == GcsfsFramework.GCSFS_FRAMEWORK_PANDAS
        for h in zonal_fs_harness.feature_headers
    )
    assert [op for op, _ in zonal_fs_harness.grpc_ops].count("_cat_file") == 2
    assert ("mrd_pool.close", fw) in [
        (op, ctx.get("fw")) for op, ctx in zonal_fs_harness.grpc_ops
    ]
    assert [
        (op, ctx.get("fw"), ctx.get("m")) for op, ctx in zonal_fs_harness.grpc_ops
    ] == [
        ("_cat_file", fw, "file_read"),
        ("_cat_file", fw, "file_read"),
        ("mrd_pool.close", fw, "file_close"),
    ]
    assert [(op, m) for op, _, m in zonal_fs_harness.captured] == [
        ("layout", "open"),
        ("http_info", "open"),
        ("layout", "open"),
        ("cat_file", "file_read"),
        ("cat_file", "file_read"),
        ("http_info", "file_info"),
        ("layout", "file_info"),
        ("mrd_close", "file_close"),
    ]

    with fs.open("zonal-bucket/obj", "wb"):
        pass

    assert detector.thread_names == [threading.current_thread().name] * 2
    assert len(zonal_fs_harness.mrd_md) == 1
    mrd_headers = dict(zonal_fs_harness.mrd_md[0])
    assert mrd_headers["x-goog-api-client"] == "cache_type/none:e"
    mrd_feat = _decode_features(mrd_headers[GRPC_METADATA_KEY])
    assert (mrd_feat.method, mrd_feat.framework) == (
        GcsfsMethod.GCSFS_METHOD_OPEN,
        GcsfsFramework.GCSFS_FRAMEWORK_PANDAS,
    )
    assert len(zonal_fs_harness.aaow_md) == 1
    aaow_feat = _decode_features(dict(zonal_fs_harness.aaow_md[0])[GRPC_METADATA_KEY])
    assert (aaow_feat.method, aaow_feat.framework) == (
        GcsfsMethod.GCSFS_METHOD_FILE_CLOSE,
        GcsfsFramework.GCSFS_FRAMEWORK_PANDAS,
    )


def test_zonal_file_pickle_reopen_detects_worker_framework_and_honors_opt_out(
    monkeypatch, zonal_fs_harness
):
    """Verify ZonalFile pickle reopen detects worker framework on plain-driver handles and honors worker opt-out."""
    fs, data = zonal_fs_harness.fs, zonal_fs_harness.data

    class _GkeEnvDetector(BaseDetector):
        name = Dimension.ENVIRONMENT
        call_scoped = False

        def __init__(self):
            self.calls = 0

        def detect(self):
            self.calls += 1
            return "gke"

    detector = _CallerThreadOnlyDetector(None)
    gke_env = _GkeEnvDetector()
    monkeypatch.setattr(
        manager_mod,
        "default_usage_tracker",
        UsageMetricsTracker(detectors=[detector, gke_env]),
    )

    with fs.open("zonal-bucket/obj", "rb", cache_type="none") as f:
        assert f.caller_framework == ""
        payload = pickle.dumps(f)

    detector.value = "ray"
    detector.thread_names.clear()
    gke_env.calls = 0
    zonal_fs_harness.clear()
    with pickle.loads(payload) as reopened:
        assert reopened.caller_framework == "ray"
        assert reopened.caller_method == "open"
        assert reopened.read(4) == data[:4]
    assert detector.thread_names == [threading.current_thread().name]
    assert zonal_fs_harness.captured == [
        ("layout", {"fw": "ray", "env": "gke"}, "open"),
        ("http_info", {"fw": "ray", "env": "gke"}, "open"),
        ("layout", {"fw": "ray", "env": "gke"}, "open"),
        ("cat_file", {"fw": "ray", "env": "gke"}, "file_read"),
        ("mrd_close", {"fw": "ray", "env": "gke"}, "file_close"),
    ]
    assert len(zonal_fs_harness.feature_headers) == 1
    decoded_zonal = _decode_features(zonal_fs_harness.feature_headers[0])
    assert (
        decoded_zonal.framework,
        decoded_zonal.environment,
        decoded_zonal.method,
    ) == (
        GcsfsFramework.GCSFS_FRAMEWORK_RAY,
        GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE,
        GcsfsMethod.GCSFS_METHOD_OPEN,
    )

    detector.thread_names.clear()
    zonal_fs_harness.clear()
    gke_env.calls = 0
    with monkeypatch.context() as mp:
        mp.setenv("DO_NOT_TRACK", "1")
        with pickle.loads(payload) as reopened_opted_out:
            with _scoped_ctx(
                {Dimension.FRAMEWORK.value: "ray", Dimension.METHOD.value: "find"}
            ):
                assert reopened_opted_out.read(4) == data[:4]
    assert zonal_fs_harness.captured
    assert all((fw_env, m) == ({}, None) for _, fw_env, m in zonal_fs_harness.captured)
    assert zonal_fs_harness.feature_headers == [""] * len(zonal_fs_harness.user_agents)
    assert detector.thread_names == []
    assert gke_env.calls == 0
    assert get_telemetry_context() == {}


def test_zonal_file_methods_are_wrapped():
    """Verify every ZonalFile override carries the file telemetry wrapper."""
    from gcsfs.zonal_file import ZonalFile

    for name, attr in ZonalFile.__dict__.items():
        if name.startswith("__") or isinstance(attr, (classmethod, staticmethod)):
            continue
        func = attr.fget if isinstance(attr, property) else attr
        if callable(func):
            assert getattr(
                func, "_is_telemetry_wrapped", False
            ), f"ZonalFile.{name} is not telemetry-wrapped"


@pytest.mark.asyncio
async def test_zonal_file_async_fetch_range_propagates_caller_framework():
    """Verify ZonalFile's own background prefetch coroutine inherits the file's framework."""
    import unittest.mock as mock

    from gcsfs.zonal_file import ZonalFile

    captured_context = {}

    async def fake_concurrent_mrd_fetch(*args, **kwargs):
        captured_context.update(get_telemetry_context())
        return b"data"

    f = ZonalFile.__new__(ZonalFile)
    f.closed = True  # Skip finalizer cleanup for this partially-initialized file
    f.caller_framework = "jax"
    f.mrd_pool = None
    f.gcsfs = mock.MagicMock()
    f.gcsfs._concurrent_mrd_fetch = mock.AsyncMock(
        side_effect=fake_concurrent_mrd_fetch
    )

    assert await f._async_fetch_range(0, 10) == b"data"
    assert captured_context.get(Dimension.FRAMEWORK.value) == "jax"
    assert captured_context.get(Dimension.METHOD.value) == "file_read"
    assert get_telemetry_context() == {}


def test_zonal_file_write_propagates_caller_framework():
    """Verify ZonalFile.write and flush record `file_write` and `file_flush` with caller framework."""
    import unittest.mock as mock

    from gcsfs.zonal_file import ZonalFile

    captured_context = {}

    async def fake_append(data):
        captured_context.update(get_telemetry_context())

    f = ZonalFile.__new__(ZonalFile)
    f.closed = False
    f.mode = "wb"
    f.forced = False
    f.loc = 0
    f.caller_framework = "ray"
    f.aaow = mock.MagicMock()
    f.aaow.append = fake_append
    f.gcsfs = mock.MagicMock()
    f.gcsfs.loop = fsspec.asyn.get_loop()
    f._ensure_aaow = lambda: None
    try:
        assert f.write(b"abc") == 3
    finally:
        f.closed = True
    assert captured_context.get(Dimension.FRAMEWORK.value) == "ray"
    assert captured_context.get(Dimension.METHOD.value) == "file_write"
    assert get_telemetry_context() == {}

    flush_context = {}

    async def fake_flush():
        flush_context.update(get_telemetry_context())

    f.closed = False
    f.finalized = False
    f.aaow.flush = fake_flush
    try:
        f.flush()
    finally:
        f.closed = True
    assert flush_context.get(Dimension.FRAMEWORK.value) == "ray"
    assert flush_context.get(Dimension.METHOD.value) == "file_flush"
    assert get_telemetry_context() == {}


def test_gcsfile_caller_framework_caches_empty_and_avoids_repeated_detect(monkeypatch):
    """Verify GCSFile.caller_framework caches "" when undetected so file methods never re-detect."""
    from gcsfs.core import GCSFile, GCSFileSystem

    detect_count = 0

    class DummyNoneDetector(BaseDetector):
        name = Dimension.FRAMEWORK

        def detect(self):
            nonlocal detect_count
            detect_count += 1
            return None

    tracker = UsageMetricsTracker(detectors=[DummyNoneDetector()])
    monkeypatch.setattr(manager_mod, "default_usage_tracker", tracker)

    fs = GCSFileSystem(token="anon", skip_instance_cache=True)
    f = GCSFile(fs, "bucket/test.txt", mode="wb")

    # Detected once during GCSFile.__init__ and cached as the "" negative sentinel
    assert detect_count == 1
    assert f.caller_framework == ""

    # Wrapped file methods reuse the cached value without re-running detection or env-var checks
    enabled_spy = mock.create_autospec(tracker.is_enabled, wraps=tracker.is_enabled)
    monkeypatch.setattr(tracker, "is_enabled", enabled_spy)
    f._details = {"size": 4}
    f.write(b"data")
    f.tell()
    assert f.details == {"size": 4}
    assert f.info() == {"size": 4}
    assert "/b/bucket/o/test.txt" in f.url()
    assert detect_count == 1
    assert enabled_spy.call_count == 0
    f.closed = True


@pytest.mark.asyncio
async def test_async_fetch_range_propagates_caller_framework():
    """Verify that background prefetch coroutines inherit the framework context."""
    import unittest.mock as mock

    import gcsfs

    captured_context = {}

    async def mock_cat_file_concurrent(*args, **kwargs):
        captured_context.update(get_telemetry_context())
        return b"data"

    fs = mock.MagicMock()
    fs._cat_file_concurrent = mock.AsyncMock(side_effect=mock_cat_file_concurrent)
    fs.split_path.return_value = ("bucket", "test.parquet", None)

    with mock.patch.object(gcsfs.core.GCSFile, "info", return_value={"size": 100}):
        f = gcsfs.core.GCSFile(fs, "gs://bucket/test.parquet", "rb", cache_type="none")

    f.caller_framework = "pandas"
    await f._async_fetch_range(0, 10)
    f.closed = True
    assert captured_context.get(Dimension.FRAMEWORK.value) == "pandas"
    assert captured_context.get(Dimension.METHOD.value) == "file_read"


def test_defer_close_propagates_caller_framework():
    """Verify that background deferred close worker thread inherits the file's framework context."""
    import unittest.mock as mock

    import gcsfs
    from gcsfs.core import _defer_close

    done = threading.Event()
    captured_context = {}

    fs = mock.MagicMock()
    fs.split_path.return_value = ("bucket", "test.parquet", None)
    with mock.patch.object(gcsfs.core.GCSFile, "info", return_value={"size": 100}):
        f = gcsfs.core.GCSFile(fs, "gs://bucket/test.parquet", "wb")

    f.caller_framework = "ray"
    f.buffer.write(b"x")

    def mock_upload_chunk(final=False):
        captured_context.update(get_telemetry_context())
        done.set()
        return True

    f._initiate_upload = lambda: None
    f._upload_chunk = mock_upload_chunk
    _defer_close(f)

    assert done.wait(timeout=5.0)
    assert captured_context.get(Dimension.FRAMEWORK.value) == "ray"
    assert captured_context.get(Dimension.METHOD.value) == "file_close"


@pytest.fixture
def spied_gcs_harness(mock_gcs_harness, monkeypatch):
    """Wrap mock_gcs_harness to capture active fw/env map and method per call."""
    from gcsfs.core import GCSFileSystem as CoreGCSFileSystem

    orig_call = CoreGCSFileSystem._call
    seen = []

    async def spy_call(self, method, path, *args, **kwargs):
        tracker = manager_mod.default_usage_tracker
        seen.append(
            (
                _fw_env(tracker),
                tracker.get_dimension(Dimension.METHOD),
            )
        )
        res = await orig_call(self, method, path, *args, **kwargs)
        req_headers = kwargs.get("headers") or {}
        content_range = req_headers.get("Content-Range", "")
        if (
            method == "POST"
            and "http://mock-gcs/upload" in str(path)
            and content_range.endswith("/*")
        ):
            end_byte = content_range.removeprefix("bytes ").split("/")[0].split("-")[1]
            return {"Range": f"bytes=0-{end_byte}"}, b""
        if isinstance(res, dict) and "size" in res:
            res.setdefault("metadata", {"k": "v"})
        elif method == "GET" and "download" in str(path) and isinstance(res, tuple):
            rng = (kwargs.get("headers") or {}).get("Range", "")
            if rng.startswith("bytes="):
                s_str, e_str = rng.removeprefix("bytes=").split("-")
                return res[0], res[1][int(s_str) : int(e_str) + 1]
        return res

    monkeypatch.setattr(CoreGCSFileSystem, "_call", spy_call)

    def _clear():
        mock_gcs_harness.clear()
        seen.clear()

    return types.SimpleNamespace(
        fs=mock_gcs_harness.fs,
        user_agents=mock_gcs_harness.user_agents,
        feature_headers=mock_gcs_harness.feature_headers,
        seen=seen,
        clear=_clear,
    )


@pytest.mark.asyncio
async def test_gcsfs_native_async_telemetry_propagation(spied_gcs_harness):
    """Verify native async GCSFileSystem methods propagate telemetry context."""
    fs = spied_gcs_harness.fs
    spied_gcs_harness.clear()

    with _scoped_ctx(Dimension.FRAMEWORK, "jax"):
        res = await fs._cat(["test-bucket/f1.txt", "test-bucket/f2.txt"])
        assert len(res) == 2
        assert len(spied_gcs_harness.user_agents) >= 2
        assert len(spied_gcs_harness.feature_headers) == len(
            spied_gcs_harness.user_agents
        )
        assert all(
            (d.framework, d.method)
            == (GcsfsFramework.GCSFS_FRAMEWORK_JAX, GcsfsMethod.GCSFS_METHOD_CAT)
            for d in (_decode_features(h) for h in spied_gcs_harness.feature_headers)
        )
        assert len(spied_gcs_harness.seen) >= 2
        assert all(m == "cat" for _, m in spied_gcs_harness.seen)
        assert get_telemetry_context() == {Dimension.FRAMEWORK.value: "jax"}


@pytest.mark.asyncio
async def test_gcsfs_async_wrapper_super_resolution():
    """Verify super() resolution works on inherited AsyncFileSystem methods wrapped by telemetry."""
    from unittest import mock

    from gcsfs.core import GCSFileSystem

    class SubGCSFS(GCSFileSystem):
        async def _cat_ranges(self, paths, starts, ends, **kwargs):
            return await super()._cat_ranges(paths, starts, ends, **kwargs)

    sub_fs = SubGCSFS(token="anon", project="test-project", skip_instance_cache=True)
    with mock.patch(
        "fsspec.asyn.AsyncFileSystem._cat_ranges",
        new_callable=mock.AsyncMock,
        return_value=[b"patched"],
    ) as mock_super:
        out = await sub_fs._cat_ranges(["test-bucket/f1.txt"], [0], [5])
        assert out == [b"patched"]
        mock_super.assert_awaited_once()


@pytest.mark.parametrize(
    "mock_gcs_harness", ["core", "extended"], indirect=True, ids=["core", "extended"]
)
@pytest.mark.parametrize(
    "invoke, expected_method, check",
    [
        pytest.param(
            lambda fs, tmp_path: fs.listdir("test-bucket"),
            "listdir",
            lambda res: len(res) == 1,
            id="listdir",
        ),
        pytest.param(
            lambda fs, tmp_path: fs.stat("test-bucket/file.txt"),
            "stat",
            lambda res: int(res["size"]) > 0,
            id="stat",
        ),
        pytest.param(
            lambda fs, tmp_path: fs.lexists("test-bucket/file.txt"),
            "lexists",
            lambda res: res is True,
            id="lexists",
        ),
        pytest.param(
            lambda fs, tmp_path: fs.makedir("test-bucket/newdir"),
            "makedir",
            lambda res: res is None,
            id="makedir",
        ),
        pytest.param(
            lambda fs, tmp_path: (
                (tmp_path / "up.bin").write_bytes(b"up"),
                fs.upload(str(tmp_path / "up.bin"), "test-bucket/up.bin"),
            )[1],
            "upload",
            lambda res: res == [None],
            id="upload",
        ),
        pytest.param(
            lambda fs, tmp_path: fs.download(
                "test-bucket/file.txt",
                str(tmp_path / "dl.bin"),
                concurrency=2,
                chunk_size=10,
            ),
            "download",
            lambda res: res == [None],
            id="download",
        ),
        pytest.param(
            lambda fs, tmp_path: fs.rename(
                "test-bucket/file.txt", "test-bucket/renamed.txt"
            ),
            "rename",
            lambda res: res is None,
            id="rename",
        ),
        pytest.param(
            lambda fs, tmp_path: fs.delete("test-bucket/file.txt"),
            "delete",
            lambda res: res in ([None], ["test-bucket/file.txt"]),
            id="delete",
        ),
        pytest.param(
            lambda fs, tmp_path: fs.getxattr("test-bucket/a.txt", "k"),
            "getxattr",
            lambda res: res == "v",
            id="getxattr",
        ),
        pytest.param(
            lambda fs, tmp_path: fs.setxattrs("test-bucket/a.txt", k="v2"),
            "setxattrs",
            lambda res: isinstance(res, dict),
            id="setxattrs",
        ),
        pytest.param(
            lambda fs, tmp_path: fs.make_bucket_requester_pays("test-bucket", True),
            "make_bucket_requester_pays",
            lambda res: res is None,
            id="make_bucket_requester_pays",
        ),
        pytest.param(
            lambda fs, tmp_path: fs.checksum("test-bucket/a.txt"),
            "checksum",
            lambda res: isinstance(res, int),
            id="checksum",
        ),
        pytest.param(
            lambda fs, tmp_path: fs.ukey("test-bucket/a.txt"),
            "ukey",
            lambda res: len(res) == 64,
            id="ukey",
        ),
        pytest.param(
            lambda fs, tmp_path: fs.read_block("test-bucket/a.txt", 0, 2),
            "read_block",
            lambda res: len(res) == 2,
            id="read_block",
        ),
        pytest.param(
            lambda fs, tmp_path: fs.call("GET", "b/test-bucket"),
            None,
            lambda res: isinstance(res, tuple),
            id="untracked_call",
        ),
    ],
)
def test_aliases_and_composite_methods_record_own_method_names(
    detector, spied_gcs_harness, tmp_path, monkeypatch, invoke, expected_method, check
):
    """Verify aliases and composite helpers record their own method names while call stays untracked."""
    fs = spied_gcs_harness.fs
    monkeypatch.setattr(fs, "MIN_CHUNK_SIZE_FOR_CONCURRENCY", 1)

    async def _zero_threshold(bucket):
        return 0

    monkeypatch.setattr(fs, "_get_threshold_for_disk_reads", _zero_threshold)
    spied_gcs_harness.clear()
    fs.invalidate_cache()

    result = invoke(fs, tmp_path)

    assert check(result)
    assert len(spied_gcs_harness.seen) >= 1
    assert all(
        (fw_env, m) == ({"fw": "pandas"}, expected_method)
        for fw_env, m in spied_gcs_harness.seen
    )
    expected_method_enum = (
        GcsfsMethod[f"GCSFS_METHOD_{expected_method.upper()}"]
        if expected_method
        else GcsfsMethod.GCSFS_METHOD_UNSPECIFIED
    )
    assert len(spied_gcs_harness.feature_headers) == len(spied_gcs_harness.seen)
    assert all(
        (d.framework, d.method)
        == (GcsfsFramework.GCSFS_FRAMEWORK_PANDAS, expected_method_enum)
        for d in (_decode_features(h) for h in spied_gcs_harness.feature_headers)
    )


def test_file_handle_methods_record_distinct_file_tokens(detector, spied_gcs_harness):
    """Verify standalone file handle operations record their distinct `file_*` tokens after `open()`."""
    from gcsfs.core import GCS_MIN_BLOCK_SIZE

    fs = spied_gcs_harness.fs
    spied_gcs_harness.clear()

    with fs.open("test-bucket/w_auto.bin", "wb", block_size=GCS_MIN_BLOCK_SIZE) as wf:
        assert spied_gcs_harness.seen == []
        wf.write(b"x" * GCS_MIN_BLOCK_SIZE)
        overflow_methods = [m for _, m in spied_gcs_harness.seen]
        spied_gcs_harness.clear()

        wf.write(b"mid")
        wf.flush()
        soft_flush_seen = list(spied_gcs_harness.seen)
        wf.flush(force=True)
        force_flush_methods = [m for _, m in spied_gcs_harness.seen]
        spied_gcs_harness.clear()

    assert overflow_methods == ["file_write", "file_write"]
    assert soft_flush_seen == []
    assert force_flush_methods == ["file_flush"]
    assert spied_gcs_harness.seen == []

    spied_gcs_harness.clear()
    cf = fs.open(
        "test-bucket/w_tx.bin",
        "wb",
        autocommit=False,
        block_size=GCS_MIN_BLOCK_SIZE,
    )
    cf.write(b"x" * GCS_MIN_BLOCK_SIZE)
    spied_gcs_harness.clear()

    cf.commit()
    cf.closed = True

    assert [m for _, m in spied_gcs_harness.seen] == ["file_commit"]

    cf2 = fs.open(
        "test-bucket/w_discard.bin",
        "wb",
        autocommit=False,
        block_size=GCS_MIN_BLOCK_SIZE,
    )
    cf2.write(b"x" * GCS_MIN_BLOCK_SIZE)
    spied_gcs_harness.clear()

    cf2.discard()

    assert [m for _, m in spied_gcs_harness.seen] == ["file_discard"]

    # AdaptiveReadaheadCache spawns PrefetchProducer._loop during __init__; reads after
    # open() returns must record `file_read`, while `details` and `info()` record `file_details` and `file_info`.
    spied_gcs_harness.clear()
    with fs.open("test-bucket/file.txt", "rb", cache_type="adaptive") as af:
        open_methods = [m for _, m in spied_gcs_harness.seen]
        spied_gcs_harness.clear()

        data = af.read(4)
        read_methods = [m for _, m in spied_gcs_harness.seen]
        spied_gcs_harness.clear()

        af._details = None
        direct_details = af.details
        details_methods = [m for _, m in spied_gcs_harness.seen]
        spied_gcs_harness.clear()

        af._details = None
        info_details = af.info()
        info_methods = [m for _, m in spied_gcs_harness.seen]

    assert open_methods and all(m == "open" for m in open_methods)
    assert len(data) == 4
    assert read_methods and all(m == "file_read" for m in read_methods)
    assert int(direct_details["size"]) > 0
    assert details_methods and all(m == "file_details" for m in details_methods)
    assert int(info_details["size"]) > 0
    assert info_methods and all(m == "file_info" for m in info_methods)


def test_walk_generator_retains_outer_context_when_unprimed_or_primed(detector):
    """Verify unprimed and primed walk generators capture outer context when `walk()` is called."""
    dummy_fs = _DummyFS(skip_instance_cache=True)

    with _scoped_ctx(
        {Dimension.FRAMEWORK.value: "jax", Dimension.METHOD.value: "find"}
    ):
        unprimed_gen = dummy_fs.walk("test-bucket")
        primed_gen = dummy_fs.walk("test-bucket")
        first_step = next(primed_gen)

    detector.calls.clear()
    dummy_fs.clear()
    unprimed_steps = list(unprimed_gen)
    remaining_steps = list(primed_gen)

    assert len(unprimed_steps) == 4
    assert first_step == ("test-bucket/dir_0", [], ["file_0.txt"])
    assert len(remaining_steps) == 3
    assert [tokens for _, tokens in dummy_fs.seen] == [{"fw": "jax"}] * 7
    assert dummy_fs.seen_telemetry == ["find"] * 7
    assert detector.calls == []


def test_gcsfile_pickle_reopen_detects_worker_framework_and_honors_opt_out(
    detector, spied_gcs_harness, monkeypatch
):
    """Verify GCSFile pickle reopen detects worker framework, records `open`/`file_read`, and honors opt-out."""
    fs = spied_gcs_harness.fs
    gke_env = _CountingEnvDetector()
    monkeypatch.setattr(
        manager_mod,
        "default_usage_tracker",
        UsageMetricsTracker(detectors=[detector, gke_env]),
    )
    detector.value = None
    with fs.open("test-bucket/file.txt", "rb", size=4, cache_type="none") as f:
        assert f.caller_framework == ""
        assert len(f.read(1)) == 1
        payload = pickle.dumps(f)

    detector.value = "ray"
    detector.calls.clear()
    gke_env.calls = 0
    spied_gcs_harness.clear()
    with pickle.loads(payload) as reopened:
        assert reopened.caller_framework == "ray"
        assert reopened.caller_method == "open"
        assert len(detector.calls) == 1
        open_seen = list(spied_gcs_harness.seen)
        open_features = list(spied_gcs_harness.feature_headers)

        spied_gcs_harness.clear()
        assert reopened.read() == b"OCK"
        assert spied_gcs_harness.seen == [({"fw": "ray", "env": "gke"}, "file_read")]
        read_decoded = _decode_features(spied_gcs_harness.feature_headers[0])
        assert (
            read_decoded.framework,
            read_decoded.environment,
            read_decoded.method,
        ) == (
            GcsfsFramework.GCSFS_FRAMEWORK_RAY,
            GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE,
            GcsfsMethod.GCSFS_METHOD_FILE_READ,
        )
        assert len(detector.calls) == 1
    assert open_seen
    assert all(entry == ({"fw": "ray", "env": "gke"}, "open") for entry in open_seen)
    assert len(open_features) == len(open_seen)
    assert all(
        (d.framework, d.environment, d.method)
        == (
            GcsfsFramework.GCSFS_FRAMEWORK_RAY,
            GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE,
            GcsfsMethod.GCSFS_METHOD_OPEN,
        )
        for d in (_decode_features(h) for h in open_features)
    )

    detector.calls.clear()
    spied_gcs_harness.clear()
    gke_env.calls = 0
    with monkeypatch.context() as mp:
        mp.setenv("DO_NOT_TRACK", "1")
        with pickle.loads(payload) as reopened_opted_out:
            with _scoped_ctx(
                {Dimension.FRAMEWORK.value: "ray", Dimension.METHOD.value: "cat"}
            ):
                assert reopened_opted_out.read() == b"OCK"
    assert spied_gcs_harness.seen
    assert all(entry == ({}, None) for entry in spied_gcs_harness.seen)
    assert spied_gcs_harness.feature_headers == [""] * len(spied_gcs_harness.seen)
    assert detector.calls == []
    assert gke_env.calls == 0
    assert get_telemetry_context() == {}

    # Driver opted out -> worker with telemetry enabled uses the worker's setting.
    with monkeypatch.context() as mp:
        mp.setenv("DO_NOT_TRACK", "1")
        with fs.open("test-bucket/file.txt", "rb", size=4, cache_type="none") as f_opt:
            assert f_opt.caller_framework == ""
            driver_opted_out_payload = pickle.dumps(f_opt)

    detector.calls.clear()
    spied_gcs_harness.clear()
    with pickle.loads(driver_opted_out_payload) as reopened_enabled:
        assert reopened_enabled.caller_framework == "ray"
        spied_gcs_harness.clear()
        assert reopened_enabled.read() == b"MOCK"
        assert spied_gcs_harness.seen == [({"fw": "ray", "env": "gke"}, "file_read")]
        reenabled_decoded = _decode_features(spied_gcs_harness.feature_headers[0])
        assert (
            reenabled_decoded.framework,
            reenabled_decoded.environment,
            reenabled_decoded.method,
        ) == (
            GcsfsFramework.GCSFS_FRAMEWORK_RAY,
            GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE,
            GcsfsMethod.GCSFS_METHOD_FILE_READ,
        )


def test_unmapped_file_method_preserves_outer_or_unset_method(
    detector, spied_gcs_harness
):
    """Verify wrapped file methods outside `_FILE_METHOD_TOKENS` leave unset method unset and keep outer methods."""
    fs = spied_gcs_harness.fs

    class _HelperFile(_DummyFile):
        def inspect_via_fs(self):
            return get_telemetry_context().copy(), self.fs.info(self.path)

    wrap_file_methods(_HelperFile)
    dummy_fs = _DummyFS(skip_instance_cache=True)
    helper = _HelperFile(
        dummy_fs, "bucket/file.txt", mode="rb", size=3, cache_type="none"
    )
    helper.caller_framework = "pandas"

    ctx_before_info, _ = helper.inspect_via_fs()
    assert ctx_before_info == {Dimension.FRAMEWORK.value: "pandas"}
    assert dummy_fs.seen_telemetry == ["info"]

    dummy_fs.clear()
    with _scoped_ctx(
        {Dimension.FRAMEWORK.value: "ray", Dimension.METHOD.value: "find"}
    ):
        ctx_under_outer, _ = helper.inspect_via_fs()
    assert ctx_under_outer == {
        Dimension.FRAMEWORK.value: "pandas",
        Dimension.METHOD.value: "find",
    }
    assert dummy_fs.seen_telemetry == ["find"]

    with fs.open("test-bucket/file.txt", "rb", size=4, cache_type="none") as f:
        assert "/b/test-bucket/o/file.txt" in f.url()
    assert get_telemetry_context() == {}


# ============================================================================
# 6. Environment Detector Tests
# ============================================================================

_PRODUCT_NAME_PATH = "/sys/class/dmi/id/product_name"
_DEFAULT_CLUSTER_NAME_URL = (
    "http://169.254.169.254/computeMetadata/v1/instance/attributes/cluster-name"
)
_ENVIRONMENT_ENV_VARS = (
    "DO_NOT_TRACK",
    "GCSFS_NO_TELEMETRY",
    "NO_GCE_CHECK",
    "GCE_METADATA_HOST",
    "GCE_METADATA_ROOT",
    "GCE_METADATA_IP",
    "KUBERNETES_SERVICE_HOST",
    "K_SERVICE",
    "K_CONFIGURATION",
    "K_REVISION",
    "CLOUD_RUN_JOB",
    "CLOUD_RUN_EXECUTION",
    "CLOUD_RUN_WORKER_POOL",
    "CLOUD_RUN_REVISION",
    "FUNCTION_TARGET",
    "FUNCTION_NAME",
    "GAE_SERVICE",
    "GAE_VERSION",
    "GAE_INSTANCE",
    "GAE_ENV",
)


class _FakeMetadataResponse:
    def __init__(self, status: int, headers: dict, body: str):
        self.status = status
        self.headers = headers
        self._body = body

    async def text(self) -> str:
        return self._body


class _FakeRequestContext:
    def __init__(self, outcome):
        self._outcome = outcome

    async def __aenter__(self):
        if isinstance(self._outcome, BaseException):
            raise self._outcome
        return self._outcome

    async def __aexit__(self, *exc_info):
        return False


class _FakeClientSession:
    def __init__(self, host: "_FakeHost"):
        self._host = host

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        self._host.closed_sessions += 1
        return False

    def get(self, url, headers=None):
        self._host.requests.append((url, dict(headers or {})))
        return _FakeRequestContext(self._host.next_outcome())


class _FakeHost:
    """Fake DMI file, metadata server, and backoff sleeps for EnvironmentDetector."""

    def __init__(self):
        self.product_name = None
        self.dmi_reads = []
        self.outcomes = []
        self.requests = []
        self.opened_sessions = 0
        self.closed_sessions = 0
        self.sleep_delays = []
        self.on_sleep = None

    def open(self, path, *args, **kwargs):
        self.dmi_reads.append(path)
        if isinstance(self.product_name, BaseException):
            raise self.product_name
        if self.product_name is None:
            raise FileNotFoundError(path)
        return io.StringIO(self.product_name)

    def new_probe_session(self):
        self.opened_sessions += 1
        return _FakeClientSession(self)

    def next_outcome(self):
        if len(self.outcomes) > 1:
            return self.outcomes.pop(0)
        return self.outcomes[0]


def _mds_response(status: int, flavor: bool = True, body: str = ""):
    headers = {"Metadata-Flavor": "Google"} if flavor else {}
    return _FakeMetadataResponse(status, headers, body)


def _connector_error():
    connection_key = types.SimpleNamespace(host="169.254.169.254", port=80, ssl=False)
    return aiohttp.ClientConnectorError(
        connection_key, ConnectionRefusedError(111, "Connection refused")
    )


def _fake_winreg(entries=None, *, open_error=None):
    """Fake winreg module for Windows registry lookups."""
    if not isinstance(entries, dict):
        entries = {r"SYSTEM\HardwareConfig\Current": entries}
    opened_keys = []

    key_read = 0x20019
    key_query_value = 0x0001

    def open_key(root, sub_key, reserved=0, access=key_read):
        assert root == "HKEY_LOCAL_MACHINE"
        assert reserved == 0
        assert access == key_query_value
        opened_keys.append(sub_key)
        if open_error is not None:
            raise open_error
        if sub_key not in entries:
            raise FileNotFoundError(2, "missing key", sub_key)
        return contextlib.nullcontext((root, sub_key))

    def query_value_ex(key, value_name):
        root, sub_key = key
        assert root == "HKEY_LOCAL_MACHINE"
        assert value_name == "SystemProductName"
        outcome = entries[sub_key]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome, 1

    return types.SimpleNamespace(
        HKEY_LOCAL_MACHINE="HKEY_LOCAL_MACHINE",
        KEY_QUERY_VALUE=key_query_value,
        OpenKey=open_key,
        QueryValueEx=query_value_ex,
        opened_keys=opened_keys,
    )


def _wait_until_probe_settles(detector: EnvironmentDetector, timeout: float = 5.0):
    deadline = time.monotonic() + timeout
    while detector._probe_in_flight:
        if time.monotonic() > deadline:
            raise AssertionError("metadata probe did not settle")
        time.sleep(0.01)


async def _settle_probe(detector: EnvironmentDetector, timeout: float = 5.0):
    deadline = time.monotonic() + timeout
    while detector._probe_in_flight:
        if time.monotonic() > deadline:
            raise AssertionError("metadata probe did not settle")
        await asyncio.sleep(0.01)


@pytest.fixture
def fake_host(monkeypatch, tmp_path):
    """Opt back into the real EnvironmentDetector.detect with a hermetic fake host."""
    monkeypatch.setattr(EnvironmentDetector, "detect", _REAL_ENVIRONMENT_DETECT)
    for name in _ENVIRONMENT_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

    host = _FakeHost()
    monkeypatch.setattr(
        environment_module,
        "sys",
        types.SimpleNamespace(platform="linux", _getframe=sys._getframe),
    )
    monkeypatch.setattr(environment_module, "open", host.open, raising=False)
    monkeypatch.setattr(
        environment_module,
        "_K8S_SERVICE_ACCOUNT_PATH",
        str(tmp_path / "missing-serviceaccount"),
    )
    monkeypatch.setattr(
        environment_module, "_new_probe_session", host.new_probe_session
    )

    async def fake_backoff_sleep(delay):
        host.sleep_delays.append(delay)
        if host.on_sleep is not None:
            result = host.on_sleep()
            if inspect.isawaitable(result):
                await result
        await asyncio.sleep(0)

    monkeypatch.setattr(environment_module, "_backoff_sleep", fake_backoff_sleep)
    return host


def test_default_usage_tracker_registers_environment_detector():
    from gcsfs.telemetry.manager import default_usage_tracker

    detector_types = [type(d) for d in default_usage_tracker.detectors]

    assert detector_types == [FrameworkDetector, EnvironmentDetector]


def test_usage_tracker_emits_environment_token(fake_host):
    fake_host.product_name = "QEMU"
    tracker = UsageMetricsTracker(detectors=[EnvironmentDetector()])

    decoded = _decode_features(tracker.encode_features())

    assert decoded.environment == GcsfsEnvironment.GCSFS_ENVIRONMENT_OTHER
    assert tracker.get_dimension(Dimension.ENVIRONMENT) == "oth"


@pytest.mark.asyncio
async def test_first_detect_schedules_probe_and_later_detect_returns_verdict(
    fake_host, monkeypatch
):
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    fake_host.product_name = "Google Compute Engine\n"
    fake_host.outcomes = [_mds_response(200, body="prod-cluster")]
    detector = EnvironmentDetector()

    first = detector.detect()
    await _settle_probe(detector)
    second = detector.detect()

    assert first is None
    assert second == "gke"
    assert fake_host.requests == [
        (_DEFAULT_CLUSTER_NAME_URL, {"Metadata-Flavor": "Google"})
    ]


def test_detect_from_sync_caller_probes_on_fsspec_loop(fake_host):
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [_mds_response(404)]
    detector = EnvironmentDetector()

    first = detector.detect()
    _wait_until_probe_settles(detector)
    second = detector.detect()

    assert first is None
    assert second == "gce"
    assert detector._probe_future is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "product_name, env_vars",
    [
        pytest.param("Google Compute Engine", {}, id="linux-gce"),
        pytest.param("Google", {}, id="linux-legacy-google"),
        pytest.param(
            "Google Compute Engine",
            {"KUBERNETES_SERVICE_HOST": "10.0.0.1"},
            id="self-managed-k8s-on-gce",
        ),
    ],
)
async def test_google_flavored_404_resolves_gce(
    fake_host, monkeypatch, product_name, env_vars
):
    for name, value in env_vars.items():
        monkeypatch.setenv(name, value)
    fake_host.product_name = product_name
    fake_host.outcomes = [_mds_response(404)]
    detector = EnvironmentDetector()

    detector.detect()
    await _settle_probe(detector)
    detected = detector.detect()

    assert detected == "gce"


@pytest.mark.asyncio
@pytest.mark.parametrize("body", ["", "   \n"], ids=["empty", "whitespace"])
async def test_google_flavored_empty_200_resolves_gce_without_retry(fake_host, body):
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [_mds_response(200, body=body)]
    detector = EnvironmentDetector()

    detector.detect()
    await _settle_probe(detector)
    detected = detector.detect()

    assert detected == "gce"
    assert len(fake_host.requests) == 1
    assert fake_host.sleep_delays == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, flavor",
    [
        pytest.param(403, False, id="flavorless-403"),
        pytest.param(408, True, id="flavored-408"),
        pytest.param(429, True, id="flavored-429"),
        pytest.param(500, False, id="flavorless-500"),
        pytest.param(503, True, id="flavored-503"),
        pytest.param(
            504,
            False,
            id="flavorless-504",
            marks=pytest.mark.skipif(
                504 not in DEFAULT_RETRYABLE_STATUS_CODES,
                reason="google-auth < 2.40.3 omits 504",
            ),
        ),
    ],
)
async def test_transient_metadata_responses_retry_until_cluster_name(
    fake_host, status, flavor
):
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [
        _mds_response(status, flavor=flavor, body="Not Found\n"),
        _mds_response(status, flavor=flavor, body="Not Found\n"),
        _mds_response(200, body="prod-cluster"),
    ]
    detector = EnvironmentDetector()
    detected_during_retries = []
    fake_host.on_sleep = lambda: detected_during_retries.append(detector.detect())

    first = detector.detect()
    await _settle_probe(detector)
    resolved = detector.detect()

    assert first is None
    assert detected_during_retries == [None, None]
    assert fake_host.sleep_delays == [0.5, 1.0]
    assert fake_host.opened_sessions == 1
    assert fake_host.closed_sessions == 1
    assert resolved == "gke"


@pytest.mark.asyncio
@pytest.mark.parametrize("kubernetes", [False, True], ids=["gce-vm", "gcp-k8s"])
@pytest.mark.parametrize(
    "status, flavor",
    [
        pytest.param(200, False, id="flavorless-200"),
        pytest.param(404, False, id="flavorless-404"),
        pytest.param(403, True, id="flavored-403"),
        pytest.param(400, True, id="flavored-400"),
        pytest.param(401, False, id="flavorless-401"),
        pytest.param(405, True, id="flavored-405"),
        pytest.param(410, True, id="flavored-410"),
        pytest.param(499, False, id="flavorless-499"),
        pytest.param(501, True, id="flavored-501"),
        pytest.param(502, False, id="flavorless-502"),
    ],
)
async def test_definitive_metadata_responses_fall_back_without_retry(
    fake_host, monkeypatch, kubernetes, status, flavor
):
    if kubernetes:
        monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [
        _mds_response(status, flavor=flavor, body="prod-cluster"),
        _mds_response(200, body="prod-cluster"),
    ]
    detector = EnvironmentDetector()

    detector.detect()
    await _settle_probe(detector)
    detected = detector.detect()

    assert detected == ("gke" if kubernetes else "gce")
    assert len(fake_host.requests) == 1
    assert fake_host.sleep_delays == []


@pytest.mark.asyncio
async def test_definitive_response_after_transient_error_stops_retrying(fake_host):
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [
        asyncio.TimeoutError(),
        _mds_response(400, flavor=False),
        _mds_response(200, body="prod-cluster"),
    ]
    detector = EnvironmentDetector()

    detector.detect()
    await _settle_probe(detector)

    assert detector.detect() == "gce"
    assert len(fake_host.requests) == 2
    assert fake_host.sleep_delays == [0.5]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_factory",
    [
        pytest.param(_connector_error, id="connector-error"),
        pytest.param(asyncio.TimeoutError, id="timeout"),
    ],
)
@pytest.mark.parametrize(
    "final_response, expected",
    [
        pytest.param(
            _mds_response(200, body="prod-cluster"), "gke", id="cluster-name-gke"
        ),
        pytest.param(_mds_response(404), "gce", id="flavored-404-gce"),
    ],
)
async def test_network_errors_retry_until_metadata_server_answers(
    fake_host, error_factory, final_response, expected
):
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [error_factory(), error_factory(), final_response]
    detector = EnvironmentDetector()
    detected_during_retries = []
    fake_host.on_sleep = lambda: detected_during_retries.append(detector.detect())

    first = detector.detect()
    await _settle_probe(detector)
    resolved = detector.detect()

    assert first is None
    assert detected_during_retries == [None, None]
    assert resolved == expected


@pytest.mark.asyncio
async def test_exhaustion_on_gcp_bios_without_kubernetes_caches_gce(fake_host):
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [asyncio.TimeoutError()]
    detector = EnvironmentDetector()

    detector.detect()
    await _settle_probe(detector)
    detected = detector.detect()

    assert detected == "gce"
    assert len(fake_host.requests) == 6


@pytest.mark.asyncio
async def test_exhaustion_on_gcp_bios_kubernetes_caches_gke(fake_host, monkeypatch):
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [_mds_response(403, flavor=False)]
    detector = EnvironmentDetector()

    detector.detect()
    await _settle_probe(detector)
    detected = detector.detect()

    assert detected == "gke"
    assert detector._cached_env == "gke"
    assert detector._probe_future is None
    assert len(fake_host.requests) == 6


class _UndecodableMetadataResponse(_FakeMetadataResponse):
    async def text(self) -> str:
        raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")


@pytest.mark.asyncio
async def test_unexpected_probe_error_falls_back_without_rescheduling(fake_host):
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [
        _UndecodableMetadataResponse(200, {"Metadata-Flavor": "Google"}, "")
    ]
    detector = EnvironmentDetector()

    detector.detect()
    await _settle_probe(detector)
    first = detector.detect()
    second = detector.detect()

    assert first == "gce"
    assert second == "gce"
    assert detector._probe_future is None
    assert len(fake_host.requests) == 1


def test_detect_inside_repeated_asyncio_run_probes_once_on_fsspec_loop(
    fake_host, monkeypatch
):
    fake_host.product_name = "Google Compute Engine"
    started = threading.Event()
    release = threading.Event()
    real_probe_once = EnvironmentDetector._probe_metadata_server_once

    async def gated_probe_once(self, session: aiohttp.ClientSession, url: str):
        started.set()
        while not release.is_set():
            await asyncio.sleep(0.005)
        return await real_probe_once(self, session, url)

    monkeypatch.setattr(
        EnvironmentDetector,
        "_probe_metadata_server_once",
        gated_probe_once,
    )
    fake_host.outcomes = [_mds_response(200, body="prod-cluster")]
    detector = EnvironmentDetector()

    async def call_once():
        return detector.detect()

    first = asyncio.run(call_once())
    assert first is None
    assert started.wait(timeout=5)
    assert detector._probe_in_flight is True

    release.set()
    _wait_until_probe_settles(detector)
    second = asyncio.run(call_once())

    assert second == "gke"
    assert len(fake_host.requests) == 1


def test_post_fork_gc_of_stale_generation_probe_under_lock_does_not_deadlock(fake_host):
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [_mds_response(503, flavor=False)]
    detector = EnvironmentDetector()
    detector._probe_generation = 1
    detector._probe_in_flight = True

    class _SuspendOnce:
        def __await__(self):
            yield None

    async def suspend():
        await _SuspendOnce()

    fake_host.on_sleep = suspend
    coro = detector._probe_coro(1)
    coro.send(None)

    detector._reset_after_fork()
    assert detector._probe_generation == 2

    def close_while_holding_lock():
        with detector._lock:
            coro.close()

    worker = threading.Thread(target=close_while_holding_lock, daemon=True)
    worker.start()
    worker.join(timeout=5)

    assert not worker.is_alive()
    assert detector._probe_in_flight is False


@pytest.mark.asyncio
async def test_metadata_host_override_probes_that_host_even_off_gcp(
    fake_host, monkeypatch
):
    monkeypatch.setenv("GCE_METADATA_HOST", "metadata.internal:8080")
    fake_host.product_name = "QEMU"
    fake_host.outcomes = [_mds_response(200, body="prod-cluster")]
    detector = EnvironmentDetector()

    detector.detect()
    await _settle_probe(detector)
    detected = detector.detect()

    assert detected == "gke"
    assert [url for url, _ in fake_host.requests] == [
        "http://metadata.internal:8080/computeMetadata/v1/instance/attributes/"
        "cluster-name"
    ]


@pytest.mark.parametrize(
    "primary_var, secondary_var",
    [
        ("K_SERVICE", "K_REVISION"),
        ("CLOUD_RUN_JOB", "CLOUD_RUN_EXECUTION"),
        ("CLOUD_RUN_WORKER_POOL", "CLOUD_RUN_REVISION"),
        ("GAE_SERVICE", "GAE_INSTANCE"),
    ],
)
def test_serverless_env_without_kubernetes_is_oth_without_io(
    fake_host, monkeypatch, primary_var, secondary_var
):
    monkeypatch.setenv(primary_var, "svc")
    monkeypatch.setenv(secondary_var, "rev")
    fake_host.product_name = "Google Compute Engine"
    detector = EnvironmentDetector()

    detected = detector.detect()

    assert detected == "oth"
    assert fake_host.dmi_reads == []
    assert fake_host.opened_sessions == 0


@pytest.mark.parametrize(
    "env_vars",
    [
        pytest.param({"K_SERVICE": "svc"}, id="k-service-only"),
        pytest.param({"FUNCTION_TARGET": "fn"}, id="function-target-only"),
        pytest.param({"FUNCTION_NAME": "fn"}, id="function-name-only"),
        pytest.param({"GAE_ENV": "standard"}, id="gae-env-only"),
        pytest.param({"CLOUD_RUN_JOB": "job"}, id="cloud-run-job-only"),
        pytest.param(
            {"CLOUD_RUN_WORKER_POOL": "pool"}, id="cloud-run-worker-pool-only"
        ),
        pytest.param({"GAE_SERVICE": "default"}, id="gae-service-only"),
        pytest.param(
            {"FUNCTION_TARGET": "fn", "K_SERVICE": "svc"},
            id="function-target-without-k-revision",
        ),
        pytest.param(
            {
                "K_SERVICE": "svc",
                "K_REVISION": "rev",
                "KUBERNETES_SERVICE_HOST": "10.0.0.1",
            },
            id="serverless-pair-with-k8s",
        ),
    ],
)
def test_single_serverless_env_var_does_not_suppress_gcp_probe(
    fake_host, monkeypatch, env_vars
):
    for name, value in env_vars.items():
        monkeypatch.setenv(name, value)
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [_mds_response(200, body="prod-cluster")]
    detector = EnvironmentDetector()

    detector.detect()
    _wait_until_probe_settles(detector)
    detected = detector.detect()

    assert detected == "gke"
    assert len(fake_host.requests) == 1


@pytest.mark.parametrize(
    "product_name, kubernetes_host, expected",
    [
        pytest.param("QEMU", None, "oth", id="off-gcp"),
        pytest.param("Google Compute Engine", None, "gce", id="gce-vm"),
        pytest.param("Google Compute Engine", "10.0.0.1", "gke", id="gcp-k8s"),
    ],
)
def test_no_gce_check_resolves_from_local_signals_only(
    fake_host,
    monkeypatch,
    product_name,
    kubernetes_host,
    expected,
):
    monkeypatch.setenv("NO_GCE_CHECK", "true")
    if kubernetes_host is not None:
        monkeypatch.setenv("KUBERNETES_SERVICE_HOST", kubernetes_host)
    fake_host.product_name = product_name
    detector = EnvironmentDetector()

    detected = detector.detect()

    assert detected == expected
    assert fake_host.opened_sessions == 0


@pytest.mark.parametrize(
    "product_name",
    [
        pytest.param("Google Foo", id="google-prefix"),
        pytest.param("Amazon EC2", id="aws"),
        pytest.param(FileNotFoundError(_PRODUCT_NAME_PATH), id="missing-dmi"),
    ],
)
def test_non_google_bios_fast_skips_even_on_kubernetes(
    fake_host, monkeypatch, product_name
):
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    fake_host.product_name = product_name
    detector = EnvironmentDetector()

    detected = detector.detect()

    assert detected == "oth"
    assert fake_host.dmi_reads == ["/sys/class/dmi/id/product_name"]
    assert fake_host.opened_sessions == 0


def test_non_linux_platform_skips_dmi_and_probe(fake_host, monkeypatch):
    monkeypatch.setattr(environment_module.sys, "platform", "freebsd")
    detector = EnvironmentDetector()

    assert detector.detect() == "oth"
    assert fake_host.dmi_reads == []
    assert fake_host.opened_sessions == 0


def test_concurrent_detect_launches_exactly_one_probe(fake_host, monkeypatch):
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [_mds_response(200, body="prod-cluster")]
    probe_generations = []
    real_probe_coro = EnvironmentDetector._probe_coro

    def counting_probe_coro(self, generation):
        probe_generations.append(generation)
        return real_probe_coro(self, generation)

    monkeypatch.setattr(EnvironmentDetector, "_probe_coro", counting_probe_coro)
    detector = EnvironmentDetector()
    barrier = threading.Barrier(10)
    results = []

    def worker():
        barrier.wait(timeout=5)
        results.append(detector.detect())

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    _wait_until_probe_settles(detector)
    resolved = detector.detect()

    assert len(probe_generations) == 1
    assert len(results) == 10
    assert set(results) <= {None, "gke"}
    assert resolved == "gke"
    assert len(fake_host.requests) == 1


def test_detect_slow_race_returns_none_for_scheduler_and_verdict_for_late_peer(
    fake_host, monkeypatch
):
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [
        _mds_response(200, body="prod-cluster"),
        _mds_response(200, body="prod-cluster"),
    ]

    real_run_coro = asyncio.run_coroutine_threadsafe

    def preempting_run_coro(coro, loop):
        fut = real_run_coro(coro, loop)
        fut.result(timeout=5)
        return fut

    with monkeypatch.context() as m:
        m.setattr(asyncio, "run_coroutine_threadsafe", preempting_run_coro)
        scheduler_detector = EnvironmentDetector()
        assert scheduler_detector.detect() is None
        assert scheduler_detector.detect() == "gke"

    peer_detector = EnvironmentDetector()
    real_is_k8s = peer_detector._is_kubernetes
    settled_by_winner = False

    def k8s_after_winner_resolves():
        nonlocal settled_by_winner
        result = real_is_k8s()
        if not settled_by_winner:
            settled_by_winner = True
            assert peer_detector._schedule_probe() is True
            _wait_until_probe_settles(peer_detector)
        return result

    monkeypatch.setattr(peer_detector, "_is_kubernetes", k8s_after_winner_resolves)
    assert peer_detector.detect() == "gke"


def test_schedule_probe_recovers_after_loop_error_or_future_cancel(
    fake_host, monkeypatch
):
    import concurrent.futures

    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [_mds_response(404)]
    detector = EnvironmentDetector()

    def raise_loop_error():
        raise RuntimeError("loop unavailable")

    with monkeypatch.context() as m:
        m.setattr(fsspec.asyn, "get_loop", raise_loop_error)
        with pytest.raises(RuntimeError, match="loop unavailable"):
            detector.detect()

    assert detector._probe_in_flight is False
    assert detector._probe_future is None

    pending_future: concurrent.futures.Future = concurrent.futures.Future()

    def capture_without_running(coro, _loop):
        coro.close()
        return pending_future

    with monkeypatch.context() as m:
        m.setattr(asyncio, "run_coroutine_threadsafe", capture_without_running)
        assert detector.detect() is None

    assert detector._probe_in_flight is True
    assert detector._probe_future is pending_future
    assert pending_future.cancel() is True
    assert detector._probe_in_flight is False
    assert detector._probe_future is None

    assert detector.detect() is None
    _wait_until_probe_settles(detector)
    assert detector.detect() == "gce"


def test_module_fork_hook_resets_live_detectors(fake_host):
    detector = EnvironmentDetector()
    detector._cached_env = "gce"
    detector._probe_in_flight = True
    detector._probe_future = object()

    environment_module._reset_detectors_after_fork()

    assert detector in environment_module._live_detectors
    assert detector._cached_env == "gce"
    assert detector._probe_in_flight is False
    assert detector._probe_future is None


def test_fs_init_warmup_and_opt_out(fake_host, monkeypatch):
    from gcsfs.core import GCSFileSystem

    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [_mds_response(404)]
    fw_calls = []

    class CountingFW(BaseDetector):
        name = Dimension.FRAMEWORK

        def detect(self):
            fw_calls.append(1)
            return "pandas"

    env_disabled = EnvironmentDetector()
    disabled_tracker = UsageMetricsTracker(detectors=[CountingFW(), env_disabled])
    monkeypatch.setattr(manager_mod, "default_usage_tracker", disabled_tracker)
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    GCSFileSystem(token="anon", project="test-project", skip_instance_cache=True)

    assert fake_host.dmi_reads == []
    assert fake_host.opened_sessions == 0
    assert fw_calls == []
    assert disabled_tracker.get_dimension(Dimension.FRAMEWORK) is None
    assert disabled_tracker.get_dimension(Dimension.ENVIRONMENT) is None

    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    env_enabled = EnvironmentDetector()
    enabled_tracker = UsageMetricsTracker(detectors=[CountingFW(), env_enabled])
    monkeypatch.setattr(manager_mod, "default_usage_tracker", enabled_tracker)
    GCSFileSystem(token="anon", project="test-project", skip_instance_cache=True)

    assert fw_calls == []
    future = env_enabled._probe_future
    if future is not None:
        future.result(timeout=5)
    assert env_enabled.detect() == "gce"
    assert fake_host.opened_sessions == 1

    assert enabled_tracker.get_dimension(Dimension.FRAMEWORK) == "pandas"
    assert len(fw_calls) == 1
    fresh_env = EnvironmentDetector()
    enabled_tracker.detectors = [CountingFW(), fresh_env]
    first_dim = enabled_tracker.get_dimension(Dimension.ENVIRONMENT)
    if fresh_env._probe_future is not None:
        fresh_env._probe_future.result(timeout=5)
    assert first_dim in (None, "gce")
    assert enabled_tracker.get_dimension(Dimension.ENVIRONMENT) == "gce"
    assert len(fw_calls) == 1


def test_mid_operation_probe_resolution_updates_subsequent_requests(
    mock_gcs_harness, fake_host, monkeypatch
):
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [_mds_response(200, body="prod-cluster")]
    release_probe = threading.Event()
    real_probe_once = EnvironmentDetector._probe_metadata_server_once

    async def gated_probe_once(self, session: aiohttp.ClientSession, url: str):
        while not release_probe.is_set():
            await asyncio.sleep(0.005)
        return await real_probe_once(self, session, url)

    monkeypatch.setattr(
        EnvironmentDetector,
        "_probe_metadata_server_once",
        gated_probe_once,
    )

    fw_detector = _CallerThreadOnlyDetector("pandas")
    env_detector = EnvironmentDetector()
    monkeypatch.setattr(
        manager_mod,
        "default_usage_tracker",
        UsageMetricsTracker(detectors=[fw_detector, env_detector]),
    )
    assert env_detector.detect() is None

    fs = mock_gcs_harness.fs
    mock_gcs_harness.clear()
    orig_call = fs._call

    async def call_then_settle(method, path, *args, **kwargs):
        result = await orig_call(method, path, *args, **kwargs)
        if len(mock_gcs_harness.user_agents) == 1:
            release_probe.set()
            await _settle_probe(env_detector)
        return result

    monkeypatch.setattr(fs, "_call", call_then_settle)

    fs.cat(["test-bucket/f1.txt", "test-bucket/f2.txt"], batch_size=1)

    assert len(mock_gcs_harness.user_agents) == 2
    assert len(mock_gcs_harness.feature_headers) == 2
    first = _decode_features(mock_gcs_harness.feature_headers[0])
    second = _decode_features(mock_gcs_harness.feature_headers[1])
    assert (first.framework, first.environment) == (
        GcsfsFramework.GCSFS_FRAMEWORK_PANDAS,
        GcsfsEnvironment.GCSFS_ENVIRONMENT_UNSPECIFIED,
    )
    assert (second.framework, second.environment) == (
        GcsfsFramework.GCSFS_FRAMEWORK_PANDAS,
        GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE,
    )
    assert len(fw_detector.thread_names) == 1


@pytest.mark.skipif(not hasattr(asyncio, "eager_task_factory"), reason="Python 3.12+")
def test_eager_task_factory_probe_does_not_deadlock(fake_host, monkeypatch):
    fake_host.product_name = "Google Compute Engine"

    def fail_session():
        raise RuntimeError("boom")

    monkeypatch.setattr(environment_module, "_new_probe_session", fail_session)
    loop = fsspec.asyn.get_loop()

    async def swap_factory(new_factory):
        prev = loop.get_task_factory()
        loop.set_task_factory(new_factory)
        return prev

    prev_factory = asyncio.run_coroutine_threadsafe(
        swap_factory(asyncio.eager_task_factory), loop
    ).result(timeout=5)
    try:
        detector = EnvironmentDetector()
        worker = threading.Thread(target=detector.detect, daemon=True)
        worker.start()
        worker.join(timeout=5)
        assert not worker.is_alive()
        _wait_until_probe_settles(detector)
        assert detector.detect() == "gce"

        async def eager_caller():
            asyncio.get_running_loop().set_task_factory(asyncio.eager_task_factory)
            detector_in_async = EnvironmentDetector()
            detector_in_async.detect()
            await _settle_probe(detector_in_async)
            return detector_in_async.detect()

        assert asyncio.run(eager_caller()) == "gce"
    finally:
        asyncio.run_coroutine_threadsafe(swap_factory(prev_factory), loop).result(
            timeout=5
        )


def test_stub_environment_detection_skips_live_benchmark_runs(monkeypatch):
    import gcsfs.tests.conftest as tests_conftest

    def make_request(run_benchmarks):
        return types.SimpleNamespace(
            config=types.SimpleNamespace(
                getoption=lambda opt: (
                    run_benchmarks if opt == "--run-benchmarks" else False
                )
            )
        )

    monkeypatch.setattr(EnvironmentDetector, "detect", _REAL_ENVIRONMENT_DETECT)

    bench_gen = tests_conftest._stub_environment_detection.__wrapped__(
        make_request(run_benchmarks=True)
    )
    next(bench_gen)
    try:
        assert EnvironmentDetector.detect is _REAL_ENVIRONMENT_DETECT
    finally:
        bench_gen.close()

    unit_gen = tests_conftest._stub_environment_detection.__wrapped__(
        make_request(run_benchmarks=False)
    )
    next(unit_gen)
    try:
        assert EnvironmentDetector().detect() is None
    finally:
        unit_gen.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entries,mds_outcome,expected,expected_keys",
    [
        pytest.param(
            "Google Compute Engine ",
            _mds_response(404),
            "gce",
            [r"SYSTEM\HardwareConfig\Current"],
            id="gce-vm",
        ),
        pytest.param(
            {r"HARDWARE\DESCRIPTION\System\BIOS": "Google Compute Engine"},
            _mds_response(200, body="win-cluster"),
            "gke",
            [r"SYSTEM\HardwareConfig\Current", r"HARDWARE\DESCRIPTION\System\BIOS"],
            id="gke-pod-bios-fallback",
        ),
        pytest.param(
            "Google",
            _mds_response(200, body="win-cluster"),
            "gke",
            [r"SYSTEM\HardwareConfig\Current"],
            id="gke-node",
        ),
    ],
)
async def test_windows_gce_bios_probes_metadata_server(
    fake_host, monkeypatch, entries, mds_outcome, expected, expected_keys
):
    winreg = _fake_winreg(entries)
    monkeypatch.setattr(environment_module.sys, "platform", "win32")
    monkeypatch.setitem(sys.modules, "winreg", winreg)
    fake_host.outcomes = [mds_outcome]
    detector = EnvironmentDetector()

    assert detector.detect() is None
    await _settle_probe(detector)
    detected = detector.detect()

    assert detected == expected
    assert winreg.opened_keys == expected_keys
    assert fake_host.dmi_reads == []
    assert fake_host.requests == [
        (_DEFAULT_CLUSTER_NAME_URL, {"Metadata-Flavor": "Google"})
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entries,no_gce_check,mds_outcomes,expected_requests,expected_keys",
    [
        pytest.param(
            {r"HARDWARE\DESCRIPTION\System\BIOS": "Google"},
            "true",
            [],
            0,
            [r"SYSTEM\HardwareConfig\Current", r"HARDWARE\DESCRIPTION\System\BIOS"],
            id="no-gce-check",
        ),
        pytest.param(
            "Google",
            None,
            [_mds_response(404, flavor=False)],
            1,
            [r"SYSTEM\HardwareConfig\Current"],
            id="metadata-failure",
        ),
    ],
)
async def test_windows_gce_bios_falls_back_to_gke_on_kubernetes(
    fake_host,
    monkeypatch,
    entries,
    no_gce_check,
    mds_outcomes,
    expected_requests,
    expected_keys,
):
    winreg = _fake_winreg(entries)
    monkeypatch.setattr(environment_module.sys, "platform", "win32")
    monkeypatch.setitem(sys.modules, "winreg", winreg)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    if no_gce_check is not None:
        monkeypatch.setenv("NO_GCE_CHECK", no_gce_check)
    else:
        monkeypatch.delenv("NO_GCE_CHECK", raising=False)
    fake_host.outcomes = mds_outcomes
    detector = EnvironmentDetector()

    first = detector.detect()
    if no_gce_check is not None:
        assert first == "gke"
    else:
        assert first is None
        await _settle_probe(detector)
        assert detector.detect() == "gke"

    assert winreg.opened_keys == expected_keys
    assert len(fake_host.requests) == expected_requests
    assert fake_host.opened_sessions == expected_requests


@pytest.mark.parametrize(
    "entries,open_error,expected_keys",
    [
        pytest.param(
            "Virtual Machine",
            None,
            [r"SYSTEM\HardwareConfig\Current"],
            id="non-google-product",
        ),
        pytest.param(
            123,
            None,
            [r"SYSTEM\HardwareConfig\Current"],
            id="non-string-value",
        ),
        pytest.param(
            None,
            PermissionError("access denied"),
            list(environment_module._WINDOWS_PRODUCT_NAME_KEYS),
            id="open-key-error",
        ),
        pytest.param(
            {
                key: FileNotFoundError("missing value")
                for key in environment_module._WINDOWS_PRODUCT_NAME_KEYS
            },
            None,
            list(environment_module._WINDOWS_PRODUCT_NAME_KEYS),
            id="missing-registry-value",
        ),
        pytest.param(None, None, None, id="winreg-import-error"),
    ],
)
def test_non_gcp_windows_host_skips_probe(
    fake_host, monkeypatch, entries, open_error, expected_keys
):
    monkeypatch.setattr(environment_module.sys, "platform", "win32")
    if expected_keys is None:
        monkeypatch.setitem(sys.modules, "winreg", None)
        winreg = None
    else:
        winreg = _fake_winreg(entries, open_error=open_error)
        monkeypatch.setitem(sys.modules, "winreg", winreg)
    detector = EnvironmentDetector()

    detected = detector.detect()

    assert detected == "oth"
    assert detector._gcp_bios is False
    if winreg is not None:
        assert winreg.opened_keys == expected_keys
    assert fake_host.dmi_reads == []
    assert fake_host.opened_sessions == 0


def test_non_linux_non_windows_platform_skips_probe(fake_host, monkeypatch):
    monkeypatch.setattr(environment_module.sys, "platform", "darwin")
    monkeypatch.delitem(sys.modules, "winreg", raising=False)
    detector = EnvironmentDetector()

    assert detector.detect() == "oth"
    assert fake_host.dmi_reads == []
    assert fake_host.opened_sessions == 0


# ============================================================================
# 7. Structured Feature Encoding Tests
# ============================================================================


def _decode_features(encoded):
    return GcsfsFeatures.deserialize(base64.urlsafe_b64decode(encoded))


def test_encode_features_wire_parity(monkeypatch):
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)
    full_tokens = {
        Dimension.METHOD.value: "cat",
        Dimension.FRAMEWORK.value: "pandas",
        Dimension.ENVIRONMENT.value: "gke",
        Dimension.METHOD_PARAMS.value: "recursive,detail,withdirs,prefix,cache_options",
    }

    ls_enc = telemetry_schema.encode_features({Dimension.METHOD.value: "ls"})
    fw_enc = telemetry_schema.encode_features({Dimension.FRAMEWORK.value: "pandas"})
    env_enc = telemetry_schema.encode_features({Dimension.ENVIRONMENT.value: "oth"})
    full_enc = telemetry_schema.encode_features(full_tokens)
    reversed_mp = "cache_options.prefix.withdirs.detail.recursive"
    reordered = telemetry_schema.encode_features({**full_tokens, "mp": reversed_mp})
    single_mp = telemetry_schema.encode_features({"mp": "cache_options"})
    unknown_mp = telemetry_schema.encode_features({"fw": "pandas", "mp": ",.unknown.,"})
    empty_map = telemetry_schema.encode_features({})
    empty_mp = telemetry_schema.encode_features({"mp": ""})
    non_str = telemetry_schema.encode_features({"fw": 123, "mp": ["recursive"]})
    token = set_telemetry_context({"fw": "pandas", "mp": ""})
    try:
        tracker_encoded = manager_mod.default_usage_tracker.encode_features()
    finally:
        reset_telemetry_context(token)

    full_dec = _decode_features(full_enc)
    single_mp_dec = _decode_features(single_mp)
    mp = full_dec.method_params
    assert f"{Dimension.FRAMEWORK}" == "fw"
    assert str(Dimension.METHOD_PARAMS) == "mp"
    assert GRPC_METADATA_KEY == "x-goog-storage-gcsfs-features"
    assert (ls_enc, fw_enc, env_enc) == ("CAE=", "EAE=", "GAM=")
    assert reordered == full_enc
    assert unknown_mp == "EAE="
    assert tracker_encoded == "EAE="
    assert empty_map is None
    assert empty_mp is None
    assert non_str is None
    assert "method_params" not in _decode_features(unknown_mp)
    assert full_dec.method == GcsfsMethod.GCSFS_METHOD_CAT
    assert full_dec.framework == GcsfsFramework.GCSFS_FRAMEWORK_PANDAS
    assert full_dec.environment == GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE
    assert "method_params" in full_dec
    assert (mp.recursive, mp.detail, mp.withdirs, mp.prefix, mp.cache_options) == (
        True,
    ) * 5
    assert single_mp_dec.method_params.cache_options is True
    assert "recursive" not in single_mp_dec.method_params


def test_encode_features_opt_out_and_cache(monkeypatch):
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)
    cache = telemetry_schema._serialize_features
    cache.cache_clear()
    enc = telemetry_schema.encode_features
    tracker = manager_mod.default_usage_tracker
    unknown = {"fw": "unknown_lib", "mp": "bogus", "env": "", "m": "unspecified"}
    opt_out_results = []
    opt_out_hits = []
    reenabled_results = []
    token = set_telemetry_context(Dimension.FRAMEWORK, "pandas")

    try:
        unknown_before = enc(unknown)
        size_before = cache.cache_info().currsize
        first = tracker.encode_features()
        second = tracker.encode_features()
        info_warm = cache.cache_info()
        unknown_warm = enc({"fw": "unknown_lib", "mp": "bogus", "env": ""})
        size_after_warm_unknown = cache.cache_info().currsize
        for env_var, env_val in (("DO_NOT_TRACK", "1"), ("GCSFS_NO_TELEMETRY", "true")):
            monkeypatch.setenv(env_var, env_val)
            opt_out_results.append(tracker.encode_features())
            opt_out_hits.append(cache.cache_info().hits)
            monkeypatch.delenv(env_var, raising=False)
            reenabled_results.append(tracker.encode_features())
    finally:
        reset_telemetry_context(token)

    assert unknown_before is None
    assert size_before == 0
    assert (first, second) == ("EAE=", "EAE=")
    assert (info_warm.misses, info_warm.hits, info_warm.currsize) == (1, 1, 1)
    assert unknown_warm is None
    assert size_after_warm_unknown == 1
    assert opt_out_results == [None, None]
    assert opt_out_hits == [info_warm.hits, info_warm.hits + 1]
    assert reenabled_results == ["EAE=", "EAE="]


def _max_token(enum_cls, prefix):
    members = (m for m in enum_cls if not m.name.endswith("_UNSPECIFIED"))
    member = max(members, key=int)
    return member, member.name.removeprefix(prefix).lower()


def test_encode_features_worst_case_bounds():
    max_method, m_tok = _max_token(GcsfsMethod, "GCSFS_METHOD_")
    max_fw, fw_tok = _max_token(GcsfsFramework, "GCSFS_FRAMEWORK_")
    max_env, env_tok = _max_token(GcsfsEnvironment, "GCSFS_ENVIRONMENT_")
    tokens = {
        Dimension.METHOD.value: m_tok,
        Dimension.FRAMEWORK.value: fw_tok,
        Dimension.ENVIRONMENT.value: env_tok,
        Dimension.METHOD_PARAMS.value: "recursive,detail,withdirs,prefix,cache_options",
    }

    encoded = telemetry_schema.encode_features(tokens)

    assert encoded is not None
    decoded_bytes = base64.urlsafe_b64decode(encoded)
    assert len(decoded_bytes) <= 256
    assert len(encoded) <= 256
    decoded = GcsfsFeatures.deserialize(decoded_bytes)
    mp = decoded.method_params
    assert decoded.method == max_method
    assert decoded.framework == max_fw
    assert decoded.environment == max_env
    assert (mp.recursive, mp.detail, mp.withdirs, mp.prefix, mp.cache_options) == (
        True,
    ) * 5


def test_tracker_encode_features_includes_process_scoped_env():
    tracker = _tracker_with(_ProcessScopedEnvDetector("gke"))

    token = set_telemetry_context(tracker.collect_tokens_map(call_scoped_only=True))
    try:
        snapshot = get_telemetry_context()
        encoded = tracker.encode_features()
    finally:
        reset_telemetry_context(token)

    assert snapshot == {"fw": "pandas"}
    assert encoded == "EAEYAg=="


def test_detector_tokens_map_to_schema_enums():
    framework_tokens = sorted(set(FrameworkDetector.KNOWN_FRAMEWORKS.values()))
    env_tokens = sorted(
        {
            value
            for name, value in vars(environment_module).items()
            if name.startswith("ENV_") and isinstance(value, str)
        }
    )

    encoded_frameworks = {
        token: telemetry_schema.encode_features({Dimension.FRAMEWORK.value: token})
        for token in framework_tokens
    }
    encoded_envs = {
        token: telemetry_schema.encode_features({Dimension.ENVIRONMENT.value: token})
        for token in env_tokens
    }

    assert set(env_tokens) >= {
        environment_module.ENV_GCE,
        environment_module.ENV_GKE,
        environment_module.ENV_OTHER,
    }
    for token, encoded in encoded_frameworks.items():
        assert encoded is not None, f"Unmapped framework token: {token}"
        decoded = _decode_features(encoded)
        assert "framework" in decoded
        assert decoded.framework != GcsfsFramework.GCSFS_FRAMEWORK_UNSPECIFIED
    for token, encoded in encoded_envs.items():
        assert encoded is not None, f"Unmapped environment token: {token}"
        decoded = _decode_features(encoded)
        assert "environment" in decoded
        assert decoded.environment != GcsfsEnvironment.GCSFS_ENVIRONMENT_UNSPECIFIED


def test_tracked_and_file_methods_map_to_schema_enums():
    """Verify every tracked FS method and file token maps to GcsfsMethod without reserved numbers."""
    file_tokens = {tok for tok in _FILE_METHOD_TOKENS.values() if tok}
    all_method_tokens = set(_TRACKED_METHODS) | file_tokens

    encoded_methods = {
        token: telemetry_schema.encode_features({Dimension.METHOD.value: token})
        for token in sorted(all_method_tokens)
    }

    assert len(_TRACKED_METHODS) == 63
    assert len(file_tokens) == 8
    assert {2, 42, 43, 45}.isdisjoint(int(m) for m in GcsfsMethod)
    for token, encoded in encoded_methods.items():
        assert encoded is not None, f"Unmapped method token: {token}"
        decoded = _decode_features(encoded)
        assert "method" in decoded
        assert decoded.method != GcsfsMethod.GCSFS_METHOD_UNSPECIFIED


# ============================================================================
# 8. gRPC Control Plane Telemetry Interceptor Tests
# ============================================================================

_CONTROL_LAYOUT_NAME = "projects/_/buckets/b/storageLayout"
_CONTROL_OP_NAME = "projects/_/buckets/b/operations/op1"


@pytest_asyncio.fixture
async def control_plane_loopback(monkeypatch):
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)
    counting = _CountingDetector("pandas")
    monkeypatch.setattr(
        manager_mod, "default_usage_tracker", UsageMetricsTracker(detectors=[counting])
    )
    recorded_calls = []

    async def get_storage_layout(request, context):
        recorded_calls.append(("GetStorageLayout", list(context.invocation_metadata())))
        return storage_control_v2.StorageLayout(name=request.name, location_type="zone")

    async def create_folder(request, context):
        recorded_calls.append(("CreateFolder", list(context.invocation_metadata())))
        return storage_control_v2.Folder(
            name=f"{request.parent}/folders/{request.folder_id}"
        )

    class OperationsServicer(operations_pb2_grpc.OperationsServicer):
        async def GetOperation(self, request, context):
            recorded_calls.append(("GetOperation", list(context.invocation_metadata())))
            return operations_pb2.Operation(name=request.name, done=True)

    def _unary(fn, req_cls, resp_cls):
        return grpc.unary_unary_rpc_method_handler(
            fn,
            request_deserializer=req_cls.deserialize,
            response_serializer=resp_cls.serialize,
        )

    server = grpc.aio.server()
    svc = storage_control_v2
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "google.storage.control.v2.StorageControl",
                {
                    "GetStorageLayout": _unary(
                        get_storage_layout,
                        svc.GetStorageLayoutRequest,
                        svc.StorageLayout,
                    ),
                    "CreateFolder": _unary(
                        create_folder, svc.CreateFolderRequest, svc.Folder
                    ),
                },
            ),
        )
    )
    operations_pb2_grpc.add_OperationsServicer_to_server(OperationsServicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    instances = []

    try:
        transport_cls = svc.StorageControlAsyncClient.get_transport_class(
            "grpc_asyncio"
        )
        orig_create_channel = transport_cls.create_channel
        monkeypatch.setattr(
            transport_cls,
            "create_channel",
            lambda **kwargs: orig_create_channel(
                ssl_credentials=grpc.local_channel_credentials(), **kwargs
            ),
        )
        ExtendedGcsFileSystem.clear_instance_cache()
        endpoint = f"http://127.0.0.1:{port}"

        def make_fs(**kwargs):
            inst = ExtendedGcsFileSystem(token="anon", endpoint_url=endpoint, **kwargs)
            instances.append(inst)
            return inst

        fs = make_fs()
        client = await fs._get_control_plane_client()

        def metadata_values(metadata, key=GRPC_METADATA_KEY):
            return [v for k, v in metadata if k == key]

        yield types.SimpleNamespace(
            endpoint=endpoint,
            fs=fs,
            make_fs=make_fs,
            client=client,
            ops=client.transport.operations_client,
            counting=counting,
            recorded_calls=recorded_calls,
            metadata_values=metadata_values,
        )
    finally:
        for instance in {id(i): i for i in instances}.values():
            await instance._close_resources()
        ExtendedGcsFileSystem.clear_instance_cache()
        await server.stop(0)


@pytest.mark.asyncio
async def test_control_plane_interceptor_attaches_metadata_on_unary_and_lro(
    control_plane_loopback,
):
    fs_cached = control_plane_loopback.make_fs()

    layout = await control_plane_loopback.client.get_storage_layout(
        name=_CONTROL_LAYOUT_NAME
    )
    op = await control_plane_loopback.ops.get_operation(_CONTROL_OP_NAME)

    assert fs_cached is control_plane_loopback.fs
    assert layout.location_type == "zone"
    assert op.name == _CONTROL_OP_NAME and op.done is True
    assert [name for name, _ in control_plane_loopback.recorded_calls] == [
        "GetStorageLayout",
        "GetOperation",
    ]
    for _, md in control_plane_loopback.recorded_calls:
        assert control_plane_loopback.metadata_values(md) == ["EAE="]
        assert control_plane_loopback.metadata_values(md, "x-goog-request-params")
    first_val = control_plane_loopback.metadata_values(
        control_plane_loopback.recorded_calls[0][1]
    )[0]
    assert _decode_features(first_val).framework == (
        GcsfsFramework.GCSFS_FRAMEWORK_PANDAS
    )


@pytest.mark.asyncio
async def test_control_plane_interceptor_debug_logging(
    control_plane_loopback,
):
    caller_md = [(GRPC_METADATA_KEY, "caller-val")]
    grpc_logger = logging.getLogger(
        "google.cloud.storage_control_v2.services.storage_control.transports.grpc_asyncio"
    )
    log_records: list[logging.LogRecord] = []

    class _ListHandler(logging.Handler):
        def emit(self, record):
            log_records.append(record)

    log_handler = _ListHandler(logging.DEBUG)
    orig_level = grpc_logger.level

    grpc_logger.setLevel(logging.DEBUG)
    grpc_logger.addHandler(log_handler)
    try:
        await control_plane_loopback.client.get_storage_layout(
            name=_CONTROL_LAYOUT_NAME
        )
        await control_plane_loopback.client.get_storage_layout(
            name=_CONTROL_LAYOUT_NAME, metadata=caller_md
        )
    finally:
        grpc_logger.removeHandler(log_handler)
        grpc_logger.setLevel(orig_level)

    logged_features = [
        record.metadata.get(GRPC_METADATA_KEY)
        for record in log_records
        if "Sending request" in record.getMessage()
        and isinstance(getattr(record, "metadata", None), dict)
    ]
    wire_features = [
        control_plane_loopback.metadata_values(md)
        for _, md in control_plane_loopback.recorded_calls
    ]
    assert wire_features == [["EAE="], ["EAE="]]
    assert logged_features == ["EAE=", "EAE="]
    assert all(
        control_plane_loopback.metadata_values(md, "x-goog-request-params")
        for _, md in control_plane_loopback.recorded_calls
    )


@pytest.mark.asyncio
async def test_control_plane_interceptor_loopback_and_opt_out(
    control_plane_loopback, monkeypatch
):
    caller_md = [(GRPC_METADATA_KEY, "caller-val")]
    client = control_plane_loopback.client
    ops = control_plane_loopback.ops

    await client.get_storage_layout(name=_CONTROL_LAYOUT_NAME)
    calls_before_opt_out = len(control_plane_loopback.counting.calls)
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    await client.get_storage_layout(name=_CONTROL_LAYOUT_NAME)
    await client.get_storage_layout(name=_CONTROL_LAYOUT_NAME, metadata=caller_md)
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    monkeypatch.setenv("GCSFS_NO_TELEMETRY", "true")
    await ops.get_operation(_CONTROL_OP_NAME)
    await ops.get_operation(_CONTROL_OP_NAME, metadata=caller_md)
    calls_during_opt_out = len(control_plane_loopback.counting.calls)
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)
    await client.get_storage_layout(name=_CONTROL_LAYOUT_NAME)

    wire_features = [
        control_plane_loopback.metadata_values(md)
        for _, md in control_plane_loopback.recorded_calls
    ]
    assert wire_features == [["EAE="], [], [], [], [], ["EAE="]]
    assert all(
        control_plane_loopback.metadata_values(md, "x-goog-request-params")
        for _, md in control_plane_loopback.recorded_calls
    )
    assert calls_during_opt_out == calls_before_opt_out


@pytest.mark.asyncio
async def test_control_plane_interceptor_attaches_metadata_on_sync_mkdir(
    control_plane_loopback, monkeypatch
):
    from gcsfs.tests import layouts

    fs_sync = control_plane_loopback.make_fs(skip_instance_cache=True)
    fs_sync._storage_layout_cache["b"] = layouts.STANDARD_HIERARCHICAL
    monkeypatch.setattr(fs_sync, "_info", mock.AsyncMock(side_effect=FileNotFoundError))

    await asyncio.to_thread(fs_sync.mkdir, "b/sub")

    assert [name for name, _ in control_plane_loopback.recorded_calls] == [
        "CreateFolder"
    ]
    md = control_plane_loopback.recorded_calls[0][1]
    assert [
        (_decode_features(v).method, _decode_features(v).framework)
        for v in control_plane_loopback.metadata_values(md)
    ] == [(GcsfsMethod.GCSFS_METHOD_MKDIR, GcsfsFramework.GCSFS_FRAMEWORK_PANDAS)]
    assert all(
        control_plane_loopback.metadata_values(md, "x-goog-request-params")
        for _, md in control_plane_loopback.recorded_calls
    )


@pytest.mark.asyncio
async def test_control_plane_interceptor_preserves_call_details(
    monkeypatch,
):
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)
    interceptor = ControlPlaneTelemetryInterceptor()
    seen_calls = []

    async def spy_next(details, request):
        seen_calls.append((details, request))
        return "spy-result"

    call_creds = grpc.metadata_call_credentials(lambda ctx, cb: cb((), None))

    def _details(md, timeout=12.5):
        return grpc.aio.ClientCallDetails(
            b"/google.storage.control.v2.StorageControl/GetStorageLayout",
            timeout,
            md,
            call_creds,
            True,
        )

    req = object()
    md_req = ("x-goog-request-params", "bucket=b")
    details_md = _details(grpc.aio.Metadata(md_req))
    details_mixed = _details((md_req, ("X-Goog-Storage-Gcsfs-Features", "CallerMixed")))
    details_none = _details(None, timeout=5.0)
    results = []

    for fw, details, opt_out in (
        ("pandas", details_md, False),
        ("pandas", details_mixed, False),
        ("pandas", details_none, False),
        ("unknown_lib", details_md, False),
        ("unknown_lib", details_mixed, False),
        ("pandas", details_mixed, True),
    ):
        with monkeypatch.context() as mp:
            if opt_out:
                mp.setenv("DO_NOT_TRACK", "1")
            token = set_telemetry_context(Dimension.FRAMEWORK, fw)
            try:
                results.append(
                    await interceptor.intercept_unary_unary(spy_next, details, req)
                )
            finally:
                reset_telemetry_context(token)

    def _meta(d):
        return (d.method, d.timeout, d.credentials, d.wait_for_ready)

    forwarded_attached, attached_req = seen_calls[0]
    forwarded_replaced, _ = seen_calls[1]
    forwarded_empty_md, _ = seen_calls[2]
    forwarded_stripped, _ = seen_calls[4]
    forwarded_opt_out_stripped, _ = seen_calls[5]
    assert results == ["spy-result"] * 6
    assert attached_req is req and forwarded_attached is not details_md
    assert _meta(forwarded_attached) == (details_md.method, 12.5, call_creds, True)
    assert isinstance(forwarded_attached.metadata, grpc.aio.Metadata)
    assert tuple(forwarded_attached.metadata) == (
        md_req,
        ("x-goog-storage-gcsfs-features", "EAE="),
    )
    assert tuple(details_md.metadata) == (md_req,)
    assert isinstance(forwarded_replaced.metadata, grpc.aio.Metadata)
    assert tuple(forwarded_replaced.metadata) == (
        md_req,
        ("x-goog-storage-gcsfs-features", "EAE="),
    )
    assert _meta(forwarded_empty_md) == (details_none.method, 5.0, call_creds, True)
    assert isinstance(forwarded_empty_md.metadata, grpc.aio.Metadata)
    assert tuple(forwarded_empty_md.metadata) == (
        ("x-goog-storage-gcsfs-features", "EAE="),
    )
    assert seen_calls[3][0] is details_md
    assert isinstance(forwarded_stripped.metadata, grpc.aio.Metadata)
    assert tuple(forwarded_stripped.metadata) == (md_req,)
    assert isinstance(forwarded_opt_out_stripped.metadata, grpc.aio.Metadata)
    assert tuple(forwarded_opt_out_stripped.metadata) == (md_req,)


# ============================================================================
# 9. HTTP Feature Header & Header-Building Tests
# ============================================================================


@pytest.mark.parametrize(
    ("caller", "expected_keys", "expected_ua_key", "expected_ua"),
    [
        pytest.param(
            None,
            ("User-Agent", "X-Goog-Storage-Gcsfs-Features"),
            "User-Agent",
            f"python-gcsfs/{version}",
            id="none-headers",
        ),
        pytest.param(
            {},
            ("User-Agent", "X-Goog-Storage-Gcsfs-Features"),
            "User-Agent",
            f"python-gcsfs/{version}",
            id="empty-dict",
        ),
        pytest.param(
            {"Range": "bytes=0-15"},
            ("Range", "User-Agent", "X-Goog-Storage-Gcsfs-Features"),
            "User-Agent",
            f"python-gcsfs/{version}",
            id="unrelated-range-header",
        ),
        pytest.param(
            {"User-Agent": "custom-ua"},
            ("User-Agent", "X-Goog-Storage-Gcsfs-Features"),
            "User-Agent",
            "custom-ua",
            id="caller-user-agent-title-case",
        ),
        pytest.param(
            {"user-agent": "custom-lower-ua"},
            ("user-agent", "X-Goog-Storage-Gcsfs-Features"),
            "user-agent",
            "custom-lower-ua",
            id="caller-user-agent-lowercase",
        ),
        pytest.param(
            {"x-goog-storage-gcsfs-features": "caller-lower"},
            ("User-Agent", "X-Goog-Storage-Gcsfs-Features"),
            "User-Agent",
            f"python-gcsfs/{version}",
            id="caller-features-lowercase-replaced",
        ),
        pytest.param(
            {"X-Goog-Storage-Gcsfs-Features": "caller-canonical"},
            ("User-Agent", "X-Goog-Storage-Gcsfs-Features"),
            "User-Agent",
            f"python-gcsfs/{version}",
            id="caller-features-canonical-replaced",
        ),
        pytest.param(
            {"X-GOOG-STORAGE-GCSFS-FEATURES": "caller-upper"},
            ("User-Agent", "X-Goog-Storage-Gcsfs-Features"),
            "User-Agent",
            f"python-gcsfs/{version}",
            id="caller-features-uppercase-replaced",
        ),
        pytest.param(
            {
                "user-agent": "custom-lower-ua",
                "x-goog-storage-gcsfs-features": "caller-both",
            },
            ("user-agent", "X-Goog-Storage-Gcsfs-Features"),
            "user-agent",
            "custom-lower-ua",
            id="caller-both-preserves-ua-replaces-features",
        ),
        pytest.param(
            [
                ("Range", "bytes=0-15"),
                ("x-goog-storage-gcsfs-features", "caller-tuple"),
            ],
            ("Range", "User-Agent", "X-Goog-Storage-Gcsfs-Features"),
            "User-Agent",
            f"python-gcsfs/{version}",
            id="caller-sequence-of-pairs",
        ),
    ],
)
def test_get_headers_single_lookup_and_caller_precedence(
    monkeypatch,
    caller,
    expected_keys,
    expected_ua_key,
    expected_ua,
):
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)
    fs = GCSFileSystem(token="anon", skip_instance_cache=True)
    counting = _CountingDetector("pandas")
    tracker = UsageMetricsTracker(detectors=[counting])
    collect_spy = mock.create_autospec(
        tracker.collect_tokens_map, wraps=tracker.collect_tokens_map
    )
    enabled_spy = mock.create_autospec(tracker.is_enabled, wraps=tracker.is_enabled)
    monkeypatch.setattr(tracker, "collect_tokens_map", collect_spy)
    monkeypatch.setattr(tracker, "is_enabled", enabled_spy)
    monkeypatch.setattr(manager_mod, "default_usage_tracker", tracker)
    snapshot = copy.copy(caller)

    headers = fs._get_headers(caller)

    assert HTTP_HEADER_KEY == "X-Goog-Storage-Gcsfs-Features"
    assert (
        collect_spy.call_count,
        enabled_spy.call_count,
        len(counting.calls),
    ) == (1, 1, 1)
    assert caller == snapshot
    assert headers is not caller
    assert tuple(headers) == expected_keys
    caller_dict = dict(caller) if caller else {}
    non_feature_caller = {
        k: v for k, v in caller_dict.items() if k.lower() != HTTP_HEADER_KEY.lower()
    }
    assert non_feature_caller.items() <= headers.items()
    assert headers[expected_ua_key] == expected_ua
    assert headers[HTTP_HEADER_KEY] == "EAE="
    assert [k for k in headers if k.lower() == HTTP_HEADER_KEY.lower()] == [
        HTTP_HEADER_KEY
    ]

    # When telemetry is enabled but inactive (encode_features() is None), strip incoming features.
    with monkeypatch.context() as mp:
        mp.setattr(
            manager_mod,
            "default_usage_tracker",
            UsageMetricsTracker(detectors=[_CountingDetector(None)]),
        )
        inactive_headers = fs._get_headers(caller)
    assert not any(k.lower() == HTTP_HEADER_KEY.lower() for k in inactive_headers)
    assert non_feature_caller.items() <= inactive_headers.items()
    assert inactive_headers[expected_ua_key] == expected_ua

    # When telemetry is opted out, any incoming features header is stripped too.
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    disabled_headers = fs._get_headers(caller)
    assert not any(k.lower() == HTTP_HEADER_KEY.lower() for k in disabled_headers)
    assert non_feature_caller.items() <= disabled_headers.items()
    assert disabled_headers[expected_ua_key] == expected_ua


def test_get_headers_skips_schema_import_when_no_tokens_active(monkeypatch):
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)
    empty_tracker = UsageMetricsTracker(detectors=[_CountingDetector(None)])
    monkeypatch.setattr(manager_mod, "default_usage_tracker", empty_tracker)
    fs = GCSFileSystem(token="anon", skip_instance_cache=True)
    monkeypatch.delitem(sys.modules, "gcsfs.telemetry.schema", raising=False)
    monkeypatch.delattr(sys.modules["gcsfs.telemetry"], "schema", raising=False)

    headers = fs._get_headers(None, cache_type="none", cache_source="explicit")

    assert "gcsfs.telemetry.schema" not in sys.modules
    assert headers["User-Agent"] == f"python-gcsfs/{version} cache_type/none:e"
    assert HTTP_HEADER_KEY not in headers


def test_get_headers_includes_process_scoped_env_with_call_context(monkeypatch):
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)
    env_tracker = _tracker_with(_ProcessScopedEnvDetector("gke"))
    monkeypatch.setattr(manager_mod, "default_usage_tracker", env_tracker)
    fs = GCSFileSystem(token="anon", skip_instance_cache=True)
    token = set_telemetry_context(env_tracker.collect_tokens_map(call_scoped_only=True))

    try:
        headers = fs._get_headers(None)
    finally:
        reset_telemetry_context(token)

    assert headers["User-Agent"] == f"python-gcsfs/{version}"
    assert headers[HTTP_HEADER_KEY] == "EAEYAg=="


def test_http_user_agent_excludes_all_telemetry_and_feature_header_carries_all(
    monkeypatch,
):
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)
    tracker = UsageMetricsTracker(detectors=[_CountingDetector("pandas")])
    monkeypatch.setattr(manager_mod, "default_usage_tracker", tracker)
    fs = GCSFileSystem(token="anon", skip_instance_cache=True)
    token = set_telemetry_context(
        {"m": "cat", "mp": "recursive,cache_options", "fw": "pandas", "env": "gke"}
    )

    try:
        headers = fs._get_headers(None, cache_type="none", cache_source="explicit")
    finally:
        reset_telemetry_context(token)

    decoded = _decode_features(headers[HTTP_HEADER_KEY])
    assert headers["User-Agent"] == f"python-gcsfs/{version} cache_type/none:e"
    assert (decoded.method, decoded.framework, decoded.environment) == (
        GcsfsMethod.GCSFS_METHOD_CAT,
        GcsfsFramework.GCSFS_FRAMEWORK_PANDAS,
        GcsfsEnvironment.GCSFS_ENVIRONMENT_GKE,
    )
    mp = decoded.method_params
    assert mp.recursive is True and mp.cache_options is True


def test_wrapped_fs_and_open_file_dynamic_opt_out_toggle(mock_gcs_harness, monkeypatch):
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)
    counting = _CountingDetector("pandas")
    tracker = UsageMetricsTracker(detectors=[counting])
    monkeypatch.setattr(manager_mod, "default_usage_tracker", tracker)
    GCSFileSystem.clear_instance_cache()
    fs1 = GCSFileSystem(token="anon")
    fs2 = GCSFileSystem(token="anon")
    env_results = []
    read_results = []

    try:
        same_singleton = fs1 is fs2
        for env_var, env_val in (("DO_NOT_TRACK", "1"), ("GCSFS_NO_TELEMETRY", "true")):
            enabled = fs1._get_headers(None)
            calls_before = len(counting.calls)
            monkeypatch.setenv(env_var, env_val)
            encoded = manager_mod.default_usage_tracker.encode_features()
            disabled = fs2._get_headers(None)
            calls_delta = len(counting.calls) - calls_before
            monkeypatch.delenv(env_var, raising=False)
            noop = (encoded, calls_delta)
            env_results.append((enabled, disabled, noop))
        reenabled = fs1._get_headers(None)
    finally:
        GCSFileSystem.clear_instance_cache()

    with mock_gcs_harness.fs.open("test-bucket/file.txt", "rb", cache_type="none") as f:
        for opt_out in (False, True, False):
            if opt_out:
                monkeypatch.setenv("DO_NOT_TRACK", "1")
            else:
                monkeypatch.delenv("DO_NOT_TRACK", raising=False)
            mock_gcs_harness.clear()
            f.seek(0)
            f.read(5)
            uas = list(mock_gcs_harness.user_agents)
            read_results.append((opt_out, uas, list(mock_gcs_harness.feature_headers)))

    assert same_singleton
    for enabled, disabled, noop in env_results:
        assert enabled["User-Agent"] == f"python-gcsfs/{version}"
        assert enabled[HTTP_HEADER_KEY] == "EAE="
        assert noop == (None, 0)
        assert disabled["User-Agent"] == f"python-gcsfs/{version}"
        assert not any(k.lower() == "x-goog-storage-gcsfs-features" for k in disabled)
    assert reenabled["User-Agent"] == f"python-gcsfs/{version}"
    assert reenabled[HTTP_HEADER_KEY] == "EAE="
    for opt_out, uas, features in read_results:
        assert uas and all(
            ua == f"python-gcsfs/{version} cache_type/none:e" for ua in uas
        )
        if opt_out:
            assert features == [""] * len(uas)
        else:
            decoded = [_decode_features(h) for h in features]
            assert all(
                (d.method, d.framework)
                == (
                    GcsfsMethod.GCSFS_METHOD_FILE_READ,
                    GcsfsFramework.GCSFS_FRAMEWORK_PANDAS,
                )
                for d in decoded
            )


def test_unrecognized_long_framework_omitted_while_method_params_encoded(monkeypatch):
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)
    monkeypatch.setattr(
        manager_mod, "default_usage_tracker", UsageMetricsTracker(detectors=[])
    )
    fs = GCSFileSystem(token="anon", skip_instance_cache=True)
    long_mp = ("padding," * 10) + "cache_options"
    token = set_telemetry_context({"fw": "a" * 100, "mp": long_mp})

    try:
        headers = fs._get_headers(None)
    finally:
        reset_telemetry_context(token)

    decoded = _decode_features(headers[HTTP_HEADER_KEY])
    assert headers["User-Agent"] == f"python-gcsfs/{version}"
    assert decoded.framework == GcsfsFramework.GCSFS_FRAMEWORK_UNSPECIFIED
    assert decoded.method_params.cache_options is True
