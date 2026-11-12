"""Unit tests for the gcsfs telemetry subsystem."""

from __future__ import annotations

import asyncio
import contextlib
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
# 7. Environment Detector Tests
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


def _fake_winreg(product_name):
    """Fake winreg module for HKLM\\SYSTEM\\HardwareConfig\\Current."""

    def open_key(root, sub_key):
        assert root == "HKEY_LOCAL_MACHINE"
        assert sub_key == r"SYSTEM\HardwareConfig\Current"
        return contextlib.nullcontext((root, sub_key))

    def query_value_ex(key, value_name):
        assert key == ("HKEY_LOCAL_MACHINE", r"SYSTEM\HardwareConfig\Current")
        assert value_name == "SystemProductName"
        if isinstance(product_name, BaseException):
            raise product_name
        return product_name, 1

    return types.SimpleNamespace(
        HKEY_LOCAL_MACHINE="HKEY_LOCAL_MACHINE",
        OpenKey=open_key,
        QueryValueEx=query_value_ex,
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
        pytest.param(404, False, id="flavorless-404"),
        pytest.param(429, True, id="flavored-429"),
        pytest.param(499, False, id="flavorless-499"),
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

    async def gated_probe_once(url: str):
        started.set()
        while not release.is_set():
            await asyncio.sleep(0.005)
        return await real_probe_once(url)

    monkeypatch.setattr(
        EnvironmentDetector,
        "_probe_metadata_server_once",
        staticmethod(gated_probe_once),
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

    async def gated_probe_once(url: str):
        while not release_probe.is_set():
            await asyncio.sleep(0.005)
        return await real_probe_once(url)

    monkeypatch.setattr(
        EnvironmentDetector,
        "_probe_metadata_server_once",
        staticmethod(gated_probe_once),
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
    assert "fw/pandas" in mock_gcs_harness.user_agents[0]
    assert "env/" not in mock_gcs_harness.user_agents[0]
    assert "fw/pandas" in mock_gcs_harness.user_agents[1]
    assert "env/gke" in mock_gcs_harness.user_agents[1]
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "product_name,mds_outcome,expected",
    [
        pytest.param("Google Compute Engine ", _mds_response(404), "gce", id="gce-vm"),
        pytest.param(
            "Google", _mds_response(200, body="win-cluster"), "gke", id="gke-node"
        ),
    ],
)
async def test_windows_gce_bios_probes_metadata_server(
    fake_host, monkeypatch, product_name, mds_outcome, expected
):
    monkeypatch.setattr(environment_module.sys, "platform", "win32")
    monkeypatch.setitem(sys.modules, "winreg", _fake_winreg(product_name))
    fake_host.outcomes = [mds_outcome]
    detector = EnvironmentDetector()

    assert detector.detect() is None
    await _settle_probe(detector)
    detected = detector.detect()

    assert detected == expected
    assert fake_host.dmi_reads == []
    assert fake_host.requests == [
        (_DEFAULT_CLUSTER_NAME_URL, {"Metadata-Flavor": "Google"})
    ]


@pytest.mark.parametrize(
    "registry_outcome",
    [
        pytest.param("Virtual Machine", id="non-google-product"),
        pytest.param(123, id="non-string-value"),
        pytest.param(FileNotFoundError("missing value"), id="missing-registry-value"),
    ],
)
def test_non_gcp_windows_host_skips_probe(fake_host, monkeypatch, registry_outcome):
    monkeypatch.setattr(environment_module.sys, "platform", "win32")
    monkeypatch.setitem(sys.modules, "winreg", _fake_winreg(registry_outcome))
    detector = EnvironmentDetector()

    detected = detector.detect()

    assert detected == "oth"
    assert detector._gcp_bios is False
    assert fake_host.dmi_reads == []
    assert fake_host.opened_sessions == 0


def test_non_linux_non_windows_platform_skips_probe(fake_host, monkeypatch):
    monkeypatch.setattr(environment_module.sys, "platform", "darwin")
    monkeypatch.delitem(sys.modules, "winreg", raising=False)
    detector = EnvironmentDetector()

    assert detector.detect() == "oth"
    assert fake_host.dmi_reads == []
    assert fake_host.opened_sessions == 0
