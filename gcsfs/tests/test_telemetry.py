"""Unit tests for the gcsfs telemetry subsystem."""

from __future__ import annotations

import asyncio
import base64
import inspect
import logging
import os
import sys
import threading
import types
from unittest import mock

import fsspec.asyn
import fsspec.spec
import grpc
import grpc.aio
import pytest
from google.cloud import storage_control_v2
from google.longrunning import operations_pb2, operations_pb2_grpc

import gcsfs.telemetry.manager as manager_mod
from gcsfs.extended_gcsfs import ExtendedGcsFileSystem
from gcsfs.telemetry import schema as telemetry_schema
from gcsfs.telemetry.context import (
    GRPC_METADATA_KEY,
    Dimension,
    get_telemetry_context,
    reset_telemetry_context,
    sanitize_token,
    set_telemetry_context,
)
from gcsfs.telemetry.detector import BaseDetector, FrameworkDetector
from gcsfs.telemetry.features_proto import (
    GcsfsEnvironment,
    GcsfsFeatures,
    GcsfsFramework,
    GcsfsMethod,
)
from gcsfs.telemetry.grpc import ControlPlaneTelemetryInterceptor
from gcsfs.telemetry.manager import UsageMetricsTracker
from gcsfs.telemetry.wrapper import (
    _gcs_async_wrapper,
    _gcs_sync_wrapper,
    mirror_gcs_methods,
    wrap_file_methods,
)

# ============================================================================
# 1. Sanitizer Tests
# ============================================================================


def test_sanitize_token_valid_and_edge_cases():
    # Valid bare token values
    assert sanitize_token("pandas") == "pandas"
    assert sanitize_token("datasets") == "datasets"
    assert sanitize_token("scikit-learn") == "scikit-learn"
    assert sanitize_token("version.1.2.3") == "version.1.2.3"
    assert sanitize_token("my_custom_framework-v2") == "my_custom_framework-v2"

    # Slashes, CRLF, and forbidden characters sanitized to underscores
    assert sanitize_token("my/custom/lib") == "my_custom_lib"
    assert (
        sanitize_token("pandas\r\nInjected-Header: bad")
        == "pandas__Injected-Header__bad"
    )
    assert (
        sanitize_token("custom\r\nInjected-Header: injected_val")
        == "custom__Injected-Header__injected_val"
    )
    assert sanitize_token("host:8080") == "host_8080"
    assert sanitize_token("my@framework#1$2%3^4*5") == "my_framework_1_2_3_4_5"
    assert sanitize_token("  spaced framework  ") == "spaced_framework"

    # Length truncation
    long_str = "a" * 100
    assert len(sanitize_token(long_str, max_len=64)) == 64
    assert len(sanitize_token(long_str, max_len=10)) == 10

    # Empty, none, non-string, or punctuation-only tokens
    assert sanitize_token(None) is None
    assert sanitize_token("") is None
    assert sanitize_token(123) is None
    assert sanitize_token("   ") is None
    assert sanitize_token("///") is None
    assert sanitize_token("...") is None
    assert sanitize_token("---") is None


# ============================================================================
# 2. Framework Detector Tests
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
    assert "env/gke" in tracker.get_tokens()

    # With GCSFS_NO_TELEMETRY=1
    monkeypatch.setenv("GCSFS_NO_TELEMETRY", "1")
    assert not tracker.is_enabled()
    assert tracker.collect_tokens_map() == {}
    assert tracker.get_tokens() == []

    # With GCSFS_NO_TELEMETRY=true
    monkeypatch.setenv("GCSFS_NO_TELEMETRY", "true")
    assert not tracker.is_enabled()
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)

    # With DO_NOT_TRACK=1
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    assert not tracker.is_enabled()
    assert tracker.collect_tokens_map() == {}
    assert tracker.get_tokens() == []

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
# 3. Context Management & Concurrency Tests
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
# 4. UsageMetricsTracker Tests
# ============================================================================


def test_multidimensional_telemetry_registration():
    class DummyEnvDetector(BaseDetector):
        name = "env"

        def detect(self):
            return "gke"

    tracker = UsageMetricsTracker(detectors=[DummyEnvDetector(), FrameworkDetector()])
    assert "env/gke" in tracker.get_tokens()

    # When framework is active in context
    token = set_telemetry_context(Dimension.FRAMEWORK, "pandas")
    try:
        tokens = tracker.get_tokens()
        assert "env/gke" in tokens
        assert "fw/pandas" in tokens
        assert tracker.get_dimension(Dimension.FRAMEWORK) == "pandas"
    finally:
        reset_telemetry_context(token)


def test_get_tokens_sanitization_user_controlled_context():
    """Verify that get_tokens and get_dimension sanitize forbidden chars and CRLF from user context."""
    tracker = UsageMetricsTracker()

    # User sets context with CRLF and custom headers
    token = set_telemetry_context(
        Dimension.FRAMEWORK, "custom\r\nInjected-Header: injected_val"
    )
    try:
        assert tracker.get_tokens() == ["fw/custom__Injected-Header__injected_val"]
        assert (
            tracker.get_dimension(Dimension.FRAMEWORK)
            == "custom__Injected-Header__injected_val"
        )
    finally:
        reset_telemetry_context(token)

    # Slashes in bare value sanitized to underscores (e.g. my/custom/lib -> fw/my_custom_lib)
    token = set_telemetry_context(Dimension.FRAMEWORK, "my/custom/lib")
    try:
        assert tracker.get_tokens() == ["fw/my_custom_lib"]
        assert tracker.get_dimension(Dimension.FRAMEWORK) == "my_custom_lib"
    finally:
        reset_telemetry_context(token)

    # Punctuation-only or whitespace/empty values do not produce invalid tokens like 'fw/'
    for malformed in ["///", "...", "---", "   ", ""]:
        token = set_telemetry_context(Dimension.FRAMEWORK, malformed)
        try:
            assert tracker.get_tokens() == []
            assert tracker.get_dimension(Dimension.FRAMEWORK) is None
        finally:
            reset_telemetry_context(token)


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

    tokens1 = tracker.collect_tokens_map()
    assert tokens1 == {Dimension.FRAMEWORK.value: ""}
    assert call_count == 1

    # Second call
    t = set_telemetry_context(tokens1)
    try:
        tokens2 = tracker.collect_tokens_map()
    finally:
        reset_telemetry_context(t)
    assert tokens2 == {Dimension.FRAMEWORK.value: ""}
    assert call_count == 1


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
    tokens_map = tracker.collect_tokens_map()
    assert tokens_map == {"fw": ""}
    assert call_count == 1

    # When the captured tokens_map is bridged into ContextVar:
    token = set_telemetry_context(tokens_map)
    try:
        # All downstream operations on event loop (get_tokens, get_dimension, collect_tokens_map)
        # must now be O(1) without re-running detect()
        assert tracker.get_tokens() == []
        assert tracker.get_dimension(Dimension.FRAMEWORK) is None
        tokens_map_2 = tracker.collect_tokens_map()
        assert tokens_map_2 == {"fw": ""}
        assert call_count == 1  # Still 1, no additional detect() calls!
    finally:
        reset_telemetry_context(token)


# ============================================================================
# 5. Method Wrappers & Context Bridging Tests
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


class _DummyFS(fsspec.asyn.AsyncFileSystem):
    """Fake async filesystem whose leaf calls record (thread, User-Agent tokens)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen = []

    def _record(self):
        tokens = tuple(manager_mod.default_usage_tracker.get_tokens())
        self.seen.append((threading.get_ident(), tokens))

    async def _ls(self, path, detail=False, **kwargs):
        self._record()
        return [path]

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        self._record()
        return b"abc"[start:end]

    async def _walk(self, path, maxdepth=None, **kwargs):
        for i in range(4):
            await self._ls(f"{path}/dir_{i}")
            yield (f"{path}/dir_{i}", [], [f"file_{i}.txt"])


mirror_gcs_methods(_DummyFS)


@pytest.mark.asyncio
async def test_gcs_async_wrapper_scopes_context(detector):
    """Verify that _gcs_async_wrapper scopes context so inner HTTP requests skip stack detection."""

    # 1. Simulate an async file operation (like _cat_file) making 5 inner HTTP requests
    async def simulated_cat_file():
        for _ in range(5):
            # Each HTTP request calls get_tokens() for the User-Agent header
            assert manager_mod.default_usage_tracker.get_tokens() == ["fw/pandas"]
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
    assert closed_with == [{"fw": "pandas"}]


@pytest.mark.asyncio
async def test_async_gen_detects_framework_only_once(detector):
    """Verify sync walk and native `async for _walk` detect framework only once across all yields."""
    fs = _DummyFS(skip_instance_cache=True)

    # 1. Sync walk across 4 yielded directories -> detect() must run strictly ONCE
    assert len(list(fs.walk("bucket"))) == 4
    assert len(detector.calls) == 1

    # 2. Native async _walk across 4 yielded directories -> detect() must run strictly ONCE
    detector.calls.clear()
    assert len([item async for item in fs._walk("bucket")]) == 4
    assert len(detector.calls) == 1

    # Every inner _ls call carried the framework, and nothing leaked afterwards
    assert {tokens for _, tokens in fs.seen} == {("fw/pandas",)}
    assert get_telemetry_context() == {}


@pytest.mark.asyncio
async def test_native_async_gen_scopes_context_per_step_and_closes(detector):
    """Verify native async gen wrapper scopes context to each step (no leak to consumer) and closes on early exit."""
    closed = False
    seen_inside = []

    async def sample_async_gen():
        nonlocal closed
        try:
            for i in range(3):
                seen_inside.append(get_telemetry_context().get("fw"))
                yield i
        finally:
            closed = True

    agen = _gcs_async_wrapper(sample_async_gen)()
    assert await agen.__anext__() == 0
    # Context is active inside the generator step but never leaks to the consumer between yields
    assert seen_inside == ["pandas"]
    assert get_telemetry_context() == {}

    # Early exit (like `break` in `async for`) closes the underlying generator
    await agen.aclose()
    assert closed


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
        f = file_cls(fs, "bucket/f.txt", mode="rb", size=3, cache_type="none")
        # Differs from the detector's "pandas", so a re-detection would show up
        f.caller_framework = "torch"
        out = []
        t = threading.Thread(target=lambda: out.append(f.read()))
        t.start()
        t.join()
        assert out == [b"abc"], file_cls.__name__
        assert [tokens for _, tokens in fs.seen] == [("fw/torch",)], file_cls.__name__
    assert detector.calls == []


@pytest.mark.asyncio
async def test_file_telemetry_wrapper_and_wrap_file_methods():
    """Verify wrap_file_methods and _file_telemetry_wrapper cover sync and async methods."""

    class DummyFile:
        def __init__(self, caller_framework: str):
            self.caller_framework = caller_framework

        def sync_op(self):
            return get_telemetry_context().get(Dimension.FRAMEWORK)

        async def async_op(self):
            return get_telemetry_context().get(Dimension.FRAMEWORK)

        @property
        def details(self):
            return get_telemetry_context().get(Dimension.FRAMEWORK)

        @staticmethod
        def static_helper(x):
            return x * 2

        @classmethod
        def class_helper(cls):
            return cls.__name__

    wrap_file_methods(DummyFile)

    f = DummyFile("pytorch")
    assert f.sync_op() == "pytorch"
    assert get_telemetry_context().get(Dimension.FRAMEWORK) is None

    assert f.details == "pytorch"
    assert get_telemetry_context().get(Dimension.FRAMEWORK) is None

    assert await f.async_op() == "pytorch"
    assert get_telemetry_context().get(Dimension.FRAMEWORK) is None

    # Called through the instance: a wrapped staticmethod would receive `self` and fail
    assert f.static_helper(3) == 6
    assert f.class_helper() == "DummyFile"


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

    mirror_gcs_methods(SubDummyFS)

    # Private internal helpers (not on AsyncFileSystem and no public sync counterpart) must not be wrapped,
    # whereas AsyncFileSystem async-only methods (_process_limits) are async-wrapped without a sync mirror.
    assert not getattr(SubDummyFS._internal_helper, "_is_gcs_async_wrapped", False)
    assert not hasattr(SubDummyFS, "internal_helper")
    assert getattr(_DummyFS._process_limits, "_is_gcs_async_wrapped", False)
    assert not hasattr(_DummyFS, "process_limits")

    sub_fs = SubDummyFS(skip_instance_cache=True)
    assert sub_fs.ls("bucket") == ["sub:bucket"]
    assert list(sub_fs.walk("bucket")) == [("sub_walk:bucket", [], ["b.txt"])]

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
    """Verify _run_sync captures context on caller thread and propagates it to fsspecIO thread."""
    caller_thread_id = threading.get_ident()
    fs = _DummyFS(skip_instance_cache=True)

    assert fs.ls("bucket") == ["bucket"]
    [(thread_id, tokens)] = fs.seen
    # Ran on the background fsspecIO thread, with the caller thread's framework
    assert thread_id != caller_thread_id
    assert tokens == ("fw/pandas",)
    # Detection ran once, on the caller thread: the loop thread's stack has no framework frames
    assert detector.calls == [caller_thread_id]
    assert get_telemetry_context() == {}


# ============================================================================
# 6. GCSFileSystem & GCSFile Integration Tests
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
    ("ls", lambda fs, tmp_path, monkeypatch: fs.ls("test-bucket")),
    ("info", lambda fs, tmp_path, monkeypatch: fs.info("test-bucket/file.txt")),
    ("exists", lambda fs, tmp_path, monkeypatch: fs.exists("test-bucket/file.txt")),
    ("cat", lambda fs, tmp_path, monkeypatch: fs.cat("test-bucket/file.txt")),
    (
        "cat_file",
        lambda fs, tmp_path, monkeypatch: fs.cat_file("test-bucket/file.txt"),
    ),
    (
        "head",
        lambda fs, tmp_path, monkeypatch: fs.head("test-bucket/file.txt", size=5),
    ),
    (
        "tail",
        lambda fs, tmp_path, monkeypatch: fs.tail("test-bucket/file.txt", size=5),
    ),
    ("find", lambda fs, tmp_path, monkeypatch: fs.find("test-bucket")),
    ("walk", lambda fs, tmp_path, monkeypatch: list(fs.walk("test-bucket"))),
    ("glob", lambda fs, tmp_path, monkeypatch: fs.glob("test-bucket/*")),
    ("du", lambda fs, tmp_path, monkeypatch: fs.du("test-bucket")),
    ("size", lambda fs, tmp_path, monkeypatch: fs.size("test-bucket/file.txt")),
    (
        "read_bytes",
        lambda fs, tmp_path, monkeypatch: fs.read_bytes("test-bucket/file.txt"),
    ),
    (
        "read_text",
        lambda fs, tmp_path, monkeypatch: fs.read_text("test-bucket/file.txt"),
    ),
    (
        "write_bytes",
        lambda fs, tmp_path, monkeypatch: fs.write_bytes(
            "test-bucket/file.txt", b"data"
        ),
    ),
    (
        "write_text",
        lambda fs, tmp_path, monkeypatch: fs.write_text("test-bucket/file.txt", "data"),
    ),
    (
        "pipe_file",
        lambda fs, tmp_path, monkeypatch: fs.pipe_file("test-bucket/file.txt", b"x"),
    ),
    ("rm", lambda fs, tmp_path, monkeypatch: fs.rm("test-bucket/file.txt")),
    ("rmdir", lambda fs, tmp_path, monkeypatch: fs.rmdir("test-bucket")),
    (
        "merge",
        lambda fs, tmp_path, monkeypatch: fs.merge(
            "test-bucket/merged.txt", ["test-bucket/a.txt", "test-bucket/b.txt"]
        ),
    ),
    ("buckets", lambda fs, tmp_path, monkeypatch: fs.buckets),
    ("open_read", _read_file_helper),
    ("open_write", _write_file_helper),
    ("get_file_concurrent", _get_file_concurrent_helper),
]


@pytest.mark.parametrize(
    "mock_gcs_harness", ["core", "extended"], indirect=True, ids=["core", "extended"]
)
@pytest.mark.parametrize(
    "name,op", _COMMON_FS_OPS, ids=[name for name, _ in _COMMON_FS_OPS]
)
def test_all_parent_methods_telemetry_coverage(
    mock_gcs_harness, tmp_path, monkeypatch, name, op
):
    """
    Verify that inherited and overridden FileSystem and File methods (on both
    GCSFileSystem and ExtendedGcsFileSystem) detect the caller framework on the
    caller thread and propagate it to every outgoing User-Agent header.

    No telemetry context is pre-set: the detector only reports a framework on the
    caller thread, so a method missing its telemetry wrapper would send requests
    without the token (detection would only happen on the fsspecIO loop thread).
    """
    fs = mock_gcs_harness.fs
    captured = mock_gcs_harness.user_agents
    fs.dircache.clear()

    fw_name = f"test-{name}"
    detector = _CallerThreadOnlyDetector(fw_name)
    monkeypatch.setattr(
        manager_mod,
        "default_usage_tracker",
        UsageMetricsTracker(detectors=[detector]),
    )
    captured.clear()

    op(fs, tmp_path, monkeypatch)

    assert len(captured) > 0, f"operation {name} did not execute any network calls"
    assert all(
        f"fw/{fw_name}" in ua for ua in captured
    ), f"operation {name} had requests missing fw/{fw_name}: {captured}"
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

    assert (
        not unwrapped
    ), f"{cls_name} methods missing telemetry wrappers: {sorted(unwrapped)}"


def test_zonal_file_lifecycle_detects_framework_once(monkeypatch):
    """
    Across a Rapid (zonal) file's whole life (open -> read -> seek -> read -> info ->
    close), the caller framework is detected exactly once, on the caller thread, and
    every operation runs with it.

    Opening for read issues a JSON API `_info` request from `ZonalFile.__init__` (MRD
    pool setup) before `GCSFile.__init__` caches `caller_framework`, so `_open` must scope
    telemetry; afterwards the wrapped file methods reuse the cached value.
    """
    from gcsfs import zb_hns_utils
    from gcsfs.core import GCSFileSystem as CoreGCSFileSystem
    from gcsfs.extended_gcsfs import ExtendedGcsFileSystem
    from gcsfs.tests import layouts

    fw = "test-zonal-lifecycle"
    data = b"0123456789"
    detector = _CallerThreadOnlyDetector(fw)
    monkeypatch.setattr(
        manager_mod, "default_usage_tracker", UsageMetricsTracker(detectors=[detector])
    )
    user_agents = []
    # (operation, framework in the active telemetry context) for gRPC-side operations
    grpc_ops = []

    async def fake_call(self, method, path, *args, **kwargs):
        user_agents.append(self._get_headers(kwargs.get("headers"))["User-Agent"])
        return {
            "kind": "storage#object",
            "name": "obj",
            "bucket": "zonal-bucket",
            "size": str(len(data)),
            "generation": "1",
            "timeFinalized": "2026-01-01T00:00:00.000Z",
        }

    async def fake_lookup_bucket_layout(self, bucket):
        return layouts.ZONAL_HIERARCHICAL

    async def fake_mrd_pool_initialize(self):
        return None

    async def fake_mrd_pool_close(self):
        grpc_ops.append(("mrd_pool.close", get_telemetry_context().get("fw")))

    async def fake_cat_file(self, path, start=None, end=None, **kwargs):
        # Zonal reads go through the gRPC MRD path; record the context they run with
        grpc_ops.append(("_cat_file", get_telemetry_context().get("fw")))
        return data[start:end]

    monkeypatch.setattr(CoreGCSFileSystem, "_call", fake_call)
    monkeypatch.setattr(
        ExtendedGcsFileSystem, "_lookup_bucket_layout", fake_lookup_bucket_layout
    )
    monkeypatch.setattr(ExtendedGcsFileSystem, "_cat_file", fake_cat_file)
    monkeypatch.setattr(zb_hns_utils.MRDPool, "initialize", fake_mrd_pool_initialize)
    monkeypatch.setattr(zb_hns_utils.MRDPool, "close", fake_mrd_pool_close)
    monkeypatch.setattr(
        zb_hns_utils.MRDPool, "persisted_size", len(data), raising=False
    )

    fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)
    f = fs.open("zonal-bucket/obj", "rb", cache_type="none")
    try:
        # Open: MRD pool `_info` request is tagged; GCSFile.__init__ reused the detection
        assert f.caller_framework == fw
        assert user_agents, "zonal open did not issue any JSON API requests"
        assert detector.thread_names == [threading.current_thread().name]

        assert f.read(4) == data[:4]
        f.seek(0)
        assert f.read(2) == data[:2]
        assert f.info()["size"] == len(data)
    finally:
        f.close()

    # Detected exactly once (at open, on the caller thread) for the file's whole life
    assert detector.thread_names == [
        threading.current_thread().name
    ], f"framework detection ran more than once: {detector.thread_names}"
    assert all(
        f"fw/{fw}" in ua for ua in user_agents
    ), f"zonal file sent HTTP requests without the framework token: {user_agents}"
    assert [op for op, _ in grpc_ops].count("_cat_file") == 2
    assert ("mrd_pool.close", fw) in grpc_ops
    assert all(
        ctx_fw == fw for _, ctx_fw in grpc_ops
    ), f"zonal operations ran without the framework context: {grpc_ops}"
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
    assert get_telemetry_context() == {}


def test_zonal_file_write_propagates_caller_framework():
    """Verify ZonalFile.write (gRPC append on the loop thread) runs with the file's framework."""
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

    # Wrapped file methods reuse the cached value instead of re-running detection
    f.write(b"data")
    f.tell()
    assert detect_count == 1
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
    assert captured_context.get(Dimension.FRAMEWORK.value) == "pandas"


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


@pytest.mark.asyncio
async def test_gcsfs_native_async_telemetry_propagation(mock_gcs_harness):
    """Verify native async GCSFileSystem methods propagate telemetry context."""
    fs = mock_gcs_harness.fs
    mock_gcs_harness.clear()

    token = set_telemetry_context(Dimension.FRAMEWORK, "jax")
    try:
        res = await fs._cat(["test-bucket/f1.txt", "test-bucket/f2.txt"])
        assert len(res) == 2
        assert len(mock_gcs_harness.user_agents) >= 2
        assert all("fw/jax" in ua for ua in mock_gcs_harness.user_agents)
    finally:
        reset_telemetry_context(token)


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


# ============================================================================
# 8. gRPC Control Plane Telemetry Interceptor Tests
# ============================================================================


@pytest.mark.asyncio
async def test_control_plane_interceptor_loopback_and_opt_out(monkeypatch, caplog):
    from gcsfs.tests import layouts

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
    fs = None
    fs_sync = None
    feat_key = "x-goog-storage-gcsfs-features"
    caller_md = [(feat_key, "caller-val")]
    layout_name = "projects/_/buckets/b/storageLayout"
    op_name = "projects/_/buckets/b/operations/op1"
    grpc_logger = "google.cloud.storage_control_v2.services.storage_control.transports.grpc_asyncio"

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
        fs = ExtendedGcsFileSystem(token="anon", endpoint_url=endpoint)
        fs_cached = ExtendedGcsFileSystem(token="anon", endpoint_url=endpoint)
        client = await fs._get_control_plane_client()
        ops = client.transport.operations_client
        monkeypatch.setattr(logging.getLogger("google"), "propagate", True)
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger=grpc_logger):
            layout = await client.get_storage_layout(name=layout_name)
            op = await ops.get_operation(op_name)
            await client.get_storage_layout(name=layout_name, metadata=caller_md)
            calls_before_opt_out = len(counting.calls)
            monkeypatch.setenv("DO_NOT_TRACK", "1")
            await client.get_storage_layout(name=layout_name)
            await client.get_storage_layout(name=layout_name, metadata=caller_md)
            monkeypatch.delenv("DO_NOT_TRACK", raising=False)
            monkeypatch.setenv("GCSFS_NO_TELEMETRY", "true")
            await ops.get_operation(op_name)
            calls_during_opt_out = len(counting.calls)
            monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)
            await client.get_storage_layout(name=layout_name)
        logged_features = [
            record.metadata.get(feat_key)
            for record in caplog.records
            if "Sending request" in record.getMessage()
            and isinstance(getattr(record, "metadata", None), dict)
        ]

        fs_sync = ExtendedGcsFileSystem(
            token="anon", endpoint_url=endpoint, skip_instance_cache=True
        )
        fs_sync._storage_layout_cache["b"] = layouts.STANDARD_HIERARCHICAL
        monkeypatch.setattr(
            fs_sync, "_info", mock.AsyncMock(side_effect=FileNotFoundError)
        )
        await asyncio.to_thread(fs_sync.mkdir, "b/sub")
    finally:
        for instance in (fs, fs_sync):
            if instance is not None:
                await instance._close_resources()
        ExtendedGcsFileSystem.clear_instance_cache()
        await server.stop(0)

    def _md_values(call_idx, key):
        return [v for k, v in recorded_calls[call_idx][1] if k == key]

    expected_log = ["EAE=", "EAE=", "caller-val", None, "caller-val", None, "EAE="]
    expected_wire = [[v] if v else [] for v in expected_log] + [["EAE="]]
    assert fs_cached is fs
    assert layout.location_type == "zone"
    assert op.name == op_name and op.done is True
    assert [name for name, _ in recorded_calls] == [
        "GetStorageLayout",
        "GetOperation",
        *["GetStorageLayout"] * 3,
        "GetOperation",
        "GetStorageLayout",
        "CreateFolder",
    ]
    assert [_md_values(i, feat_key) for i in range(8)] == expected_wire
    assert logged_features == expected_log
    assert all(_md_values(i, "x-goog-request-params") for i in range(7))
    assert calls_during_opt_out == calls_before_opt_out
    assert _decode_features(_md_values(0, feat_key)[0]).framework == (
        GcsfsFramework.GCSFS_FRAMEWORK_PANDAS
    )


@pytest.mark.asyncio
async def test_control_plane_interceptor_preserves_caller_header_and_call_details(
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

    for fw, details in (
        ("pandas", details_md),
        ("pandas", details_mixed),
        ("pandas", details_none),
        ("unknown_lib", details_md),
    ):
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
    forwarded_empty_md, _ = seen_calls[2]
    assert results == ["spy-result"] * 4
    assert attached_req is req and forwarded_attached is not details_md
    assert _meta(forwarded_attached) == (details_md.method, 12.5, call_creds, True)
    assert isinstance(forwarded_attached.metadata, grpc.aio.Metadata)
    assert tuple(forwarded_attached.metadata) == (
        md_req,
        ("x-goog-storage-gcsfs-features", "EAE="),
    )
    assert tuple(details_md.metadata) == (md_req,)
    assert seen_calls[1][0] is details_mixed
    assert _meta(forwarded_empty_md) == (details_none.method, 5.0, call_creds, True)
    assert isinstance(forwarded_empty_md.metadata, grpc.aio.Metadata)
    assert tuple(forwarded_empty_md.metadata) == (
        ("x-goog-storage-gcsfs-features", "EAE="),
    )
    assert seen_calls[3][0] is details_md
