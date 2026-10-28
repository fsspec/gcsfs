"""Unit tests for the gcsfs telemetry subsystem."""

from __future__ import annotations

import asyncio
import contextlib
import gc
import inspect
import io
import os
import sys
import threading
import time
import types

import aiohttp
import fsspec.asyn
import fsspec.spec
import pytest

import gcsfs.telemetry.detector as environment_module
import gcsfs.telemetry.manager as manager_mod
from gcsfs.telemetry.context import (
    Dimension,
    get_telemetry_context,
    reset_telemetry_context,
    sanitize_token,
    set_telemetry_context,
)
from gcsfs.telemetry.detector import (
    BaseDetector,
    EnvironmentDetector,
    FrameworkDetector,
)
from gcsfs.telemetry.manager import UsageMetricsTracker
from gcsfs.telemetry.wrapper import (
    _gcs_async_wrapper,
    _gcs_sync_wrapper,
    mirror_gcs_methods,
    wrap_file_methods,
)

# Captured at import time, before the conftest autouse fixture stubs detect().
_REAL_ENVIRONMENT_DETECT = EnvironmentDetector.detect

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


@contextlib.contextmanager
def _fsspec_loop_unset():
    saved_loop = fsspec.asyn.loop[0]
    fsspec.asyn.loop[0] = None
    try:
        yield
    finally:
        fsspec.asyn.loop[0] = saved_loop


@pytest.fixture
def without_fsspec_loop():
    """Simulate a pure-async process where fsspec never started its IO loop."""
    with _fsspec_loop_unset():
        yield


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

    tokens = tracker.get_tokens()

    assert tokens == ["env/oth"]
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
@pytest.mark.parametrize(
    "status, flavor",
    [
        pytest.param(403, False, id="flavorless-403"),
        pytest.param(404, False, id="flavorless-404"),
        pytest.param(500, False, id="flavorless-500"),
        pytest.param(503, True, id="flavored-503"),
    ],
)
async def test_untrusted_metadata_responses_retry_until_cluster_name(
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
    assert resolved == "gke"


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
async def test_exhaustion_on_gcp_bios_kubernetes_stays_unknown(fake_host, monkeypatch):
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [_mds_response(403, flavor=False)]
    detector = EnvironmentDetector()

    detector.detect()
    await _settle_probe(detector)
    detected_after_exhaustion = detector.detect()

    assert detected_after_exhaustion is None
    assert detector._cached_env is None
    assert detector._probe_exhausted is True
    assert detector._probe_task is None
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
    assert detector._probe_task is None
    assert len(fake_host.requests) == 1


def test_closed_loop_with_pending_probe_is_rescheduled(fake_host, without_fsspec_loop):
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [_mds_response(503, flavor=False)]
    detector = EnvironmentDetector()

    async def run_until_probe_sleeps():
        probe_sleeping = asyncio.Event()

        async def block_forever():
            probe_sleeping.set()
            await asyncio.Event().wait()

        fake_host.on_sleep = block_forever
        detector.detect()
        await probe_sleeping.wait()

    async def resolve_on_new_loop():
        detector.detect()
        await detector._probe_task
        return detector.detect()

    abandoned_loop = asyncio.new_event_loop()
    abandoned_loop.run_until_complete(run_until_probe_sleeps())
    abandoned_loop.close()
    in_flight_after_close = detector._probe_in_flight
    fake_host.on_sleep = None
    fake_host.outcomes = [_mds_response(200, body="prod-cluster")]
    resolved = asyncio.run(resolve_on_new_loop())

    assert in_flight_after_close is True
    assert resolved == "gke"


def test_gc_of_stranded_probe_under_lock_does_not_deadlock(
    fake_host, without_fsspec_loop
):
    fake_host.product_name = "Google Compute Engine"
    fake_host.outcomes = [_mds_response(503, flavor=False)]
    detector = EnvironmentDetector()

    async def run_until_probe_sleeps():
        probe_sleeping = asyncio.Event()

        async def block_forever():
            probe_sleeping.set()
            await asyncio.Event().wait()

        fake_host.on_sleep = block_forever
        detector.detect()
        await probe_sleeping.wait()

    gc.disable()
    try:
        abandoned_loop = asyncio.new_event_loop()
        abandoned_loop.run_until_complete(run_until_probe_sleeps())
        abandoned_loop.close()
        del abandoned_loop

        def collect_while_holding_lock():
            with detector._lock:
                detector._probe_task = None
                gc.collect()

        worker = threading.Thread(target=collect_while_holding_lock, daemon=True)
        worker.start()
        worker.join(timeout=5)
    finally:
        gc.enable()

    assert not worker.is_alive()
    assert detector._probe_in_flight is True


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
    "serverless_var", ["K_SERVICE", "CLOUD_RUN_JOB", "FUNCTION_TARGET", "GAE_ENV"]
)
def test_serverless_env_without_kubernetes_is_oth_without_io(
    fake_host, monkeypatch, serverless_var
):
    monkeypatch.setenv(serverless_var, "value")
    fake_host.product_name = "Google Compute Engine"
    detector = EnvironmentDetector()

    detected = detector.detect()

    assert detected == "oth"
    assert fake_host.dmi_reads == []
    assert fake_host.opened_sessions == 0


@pytest.mark.parametrize(
    "product_name, kubernetes_host, expected, expected_exhausted",
    [
        pytest.param("QEMU", None, "oth", False, id="off-gcp"),
        pytest.param("Google Compute Engine", None, "gce", False, id="gce-vm"),
        pytest.param("Google Compute Engine", "10.0.0.1", None, True, id="gcp-k8s"),
    ],
)
def test_no_gce_check_resolves_from_local_signals_only(
    fake_host,
    monkeypatch,
    product_name,
    kubernetes_host,
    expected,
    expected_exhausted,
):
    monkeypatch.setenv("NO_GCE_CHECK", "true")
    if kubernetes_host is not None:
        monkeypatch.setenv("KUBERNETES_SERVICE_HOST", kubernetes_host)
    fake_host.product_name = product_name
    detector = EnvironmentDetector()

    detected = detector.detect()

    assert detected == expected
    assert detector._probe_exhausted is expected_exhausted
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
