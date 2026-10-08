"""Unit tests for the gcsfs telemetry subsystem."""

from __future__ import annotations

import os
import sys
import types

import pytest

from gcsfs.telemetry.context import (
    Dimension,
    get_telemetry_context,
    reset_telemetry_context,
    sanitize_token,
    set_telemetry_context,
)
from gcsfs.telemetry.detectors.base import BaseDetector
from gcsfs.telemetry.detectors.framework import FrameworkDetector
from gcsfs.telemetry.manager import UsageMetricsTracker

# ============================================================================
# 1. Sanitizer Tests
# ============================================================================


def test_sanitize_token_valid_and_edge_cases():
    # Valid single-segment and multi-segment tokens
    assert sanitize_token("pandas") == "pandas"
    assert sanitize_token("datasets") == "datasets"
    assert sanitize_token("scikit-learn") == "scikit-learn"
    assert sanitize_token("version.1.2.3") == "version.1.2.3"
    assert sanitize_token("my_custom_framework-v2") == "my_custom_framework-v2"
    assert sanitize_token("fw/pandas") == "fw/pandas"
    assert sanitize_token("env/gke") == "env/gke"

    # Multi-slash values sanitized to single slash
    assert sanitize_token("fw/my/custom/lib") == "fw/my_custom_lib"

    # CRLF and forbidden characters sanitized
    assert (
        sanitize_token("pandas\r\nInjected-Header: bad")
        == "pandas__Injected-Header__bad"
    )
    assert (
        sanitize_token("fw/custom\r\nInjected-Header: injected_val")
        == "fw/custom__Injected-Header__injected_val"
    )
    assert sanitize_token("host:8080") == "host_8080"
    assert sanitize_token("my@framework#1$2%3^4*5") == "my_framework_1_2_3_4_5"
    assert sanitize_token("  spaced framework  ") == "spaced_framework"

    # Length truncation
    long_str = "a" * 100
    assert len(sanitize_token(long_str, max_len=64)) == 64
    assert len(sanitize_token(long_str, max_len=10)) == 10

    # Empty, none, non-string, or malformed tokens
    assert sanitize_token(None) is None
    assert sanitize_token("") is None
    assert sanitize_token(123) is None
    assert sanitize_token("   ") is None
    assert sanitize_token("fw/") is None
    assert sanitize_token("/pandas") is None
    assert sanitize_token("///") is None


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
    assert detector.detect() == "fw/dask"


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


def test_framework_detector_opt_out(monkeypatch):
    detector = FrameworkDetector()

    # Default: active
    assert detector.is_enabled()

    # With GCSFS_NO_TELEMETRY=1
    monkeypatch.setenv("GCSFS_NO_TELEMETRY", "1")
    assert not detector.is_enabled()
    assert detector.detect() is None

    # With GCSFS_NO_TELEMETRY=true
    monkeypatch.setenv("GCSFS_NO_TELEMETRY", "true")
    assert not detector.is_enabled()
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)

    # With DO_NOT_TRACK=1
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    assert not detector.is_enabled()
    assert detector.detect() is None

    # With DO_NOT_TRACK=true
    monkeypatch.setenv("DO_NOT_TRACK", "true")
    assert not detector.is_enabled()
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
    assert detected == "fw/lightning"


# ============================================================================
# 3. Context Management & Concurrency Tests
# ============================================================================


def test_set_and_get_telemetry_context():
    # Initial state
    assert get_telemetry_context(Dimension.FRAMEWORK) is None

    # Set caller framework
    token = set_telemetry_context(Dimension.FRAMEWORK, "fw/custom-engine")
    assert get_telemetry_context(Dimension.FRAMEWORK) == "fw/custom-engine"

    # Reset
    reset_telemetry_context(token)
    assert get_telemetry_context(Dimension.FRAMEWORK) is None


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
            if get_telemetry_context(Dimension.FRAMEWORK) is not None:
                errors.append(f"{framework_name}: initial context not None")

            # 2. Set thread-local context
            token = set_telemetry_context(Dimension.FRAMEWORK, f"fw/{framework_name}")
            try:
                # 3. Synchronize all threads so all 4 contexts are simultaneously active
                barrier.wait(timeout=5)

                # 4. Repeatedly verify context during concurrent overlap
                for _ in range(5):
                    current = get_telemetry_context(Dimension.FRAMEWORK)
                    if current != f"fw/{framework_name}":
                        errors.append(
                            f"{framework_name}: expected fw/{framework_name}, got {current}"
                        )
                    time.sleep(0.005)
            finally:
                reset_telemetry_context(token)

            # 5. Verify thread context is cleanly reset
            if get_telemetry_context(Dimension.FRAMEWORK) is not None:
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

    token = set_telemetry_context(Dimension.FRAMEWORK, "fw/parent-process")
    try:
        assert get_telemetry_context() == {"fw": "fw/parent-process"}

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
        assert get_telemetry_context(Dimension.FRAMEWORK) == "fw/parent-process"
    finally:
        reset_telemetry_context(token)


# ============================================================================
# 4. UsageMetricsTracker Tests
# ============================================================================


def test_multidimensional_telemetry_registration():
    class DummyEnvDetector(BaseDetector):
        name = "env"

        def detect(self):
            return "env/gke"

    tracker = UsageMetricsTracker(detectors=[DummyEnvDetector(), FrameworkDetector()])
    assert "env/gke" in tracker.get_tokens()

    # When framework is active in context
    token = set_telemetry_context(Dimension.FRAMEWORK, "fw/pandas")
    try:
        tokens = tracker.get_tokens()
        assert "env/gke" in tokens
        assert "fw/pandas" in tokens
        assert tracker.get_dimension(Dimension.FRAMEWORK) == "fw/pandas"
    finally:
        reset_telemetry_context(token)


def test_get_tokens_sanitization_user_controlled_context():
    """Verify that get_tokens and get_dimension sanitize forbidden chars and CRLF from user context."""
    tracker = UsageMetricsTracker()

    # User sets context with CRLF and custom headers
    token = set_telemetry_context(
        Dimension.FRAMEWORK, "fw/custom\r\nInjected-Header: injected_val"
    )
    try:
        tokens = tracker.get_tokens()
        assert tokens == ["fw/custom__Injected-Header__injected_val"]
        assert (
            tracker.get_dimension(Dimension.FRAMEWORK)
            == "fw/custom__Injected-Header__injected_val"
        )
    finally:
        reset_telemetry_context(token)

    # Extra slashes in value (e.g. fw/my/custom/lib -> fw/my_custom_lib)
    token = set_telemetry_context(Dimension.FRAMEWORK, "fw/my/custom/lib")
    try:
        tokens = tracker.get_tokens()
        assert tokens == ["fw/my_custom_lib"]
        assert tracker.get_dimension(Dimension.FRAMEWORK) == "fw/my_custom_lib"
    finally:
        reset_telemetry_context(token)

    # Dangling / leading slash or empty should not produce invalid tokens like 'fw/'
    for malformed in ["fw/", "/pandas", "   ", ""]:
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


@pytest.mark.asyncio
async def test_gcs_async_wrapper_scopes_context(monkeypatch):
    """Verify that _gcs_async_wrapper scopes context so inner HTTP requests skip stack detection."""
    import gcsfs.telemetry.manager as manager_mod
    from gcsfs.telemetry.manager import _gcs_async_wrapper

    # 1. Count how many times detect() actually executes
    detect_count = 0

    class MockDetector(BaseDetector):
        name = Dimension.FRAMEWORK

        def detect(self):
            nonlocal detect_count
            detect_count += 1
            return "fw/pandas"

    tracker = UsageMetricsTracker(detectors=[MockDetector()])
    monkeypatch.setattr(manager_mod, "default_usage_tracker", tracker)

    # 2. Simulate an async file operation (like _cat_file) making 5 inner HTTP requests
    async def simulated_cat_file():
        for _ in range(5):
            # Each HTTP request calls tracker.get_tokens() for the User-Agent header
            tokens = tracker.get_tokens()
            assert tokens == ["fw/pandas"]
        return "done"

    # 3. Wrap with _gcs_async_wrapper
    wrapped_cat_file = _gcs_async_wrapper(simulated_cat_file)

    # 4. Run the operation
    assert await wrapped_cat_file() == "done"

    # 5. Check: detect() ran ONCE at entry (instead of 5 times for 5 HTTP requests)
    assert detect_count == 1

    # 6. Check: ContextVar is cleanly reset after the operation finishes
    assert get_telemetry_context() == {}


@pytest.mark.asyncio
async def test_consecutive_native_async_calls_isolated(monkeypatch):
    """Verify that consecutive native async calls under different frameworks are 100% isolated."""
    import gcsfs.telemetry.manager as manager_mod
    from gcsfs.telemetry.manager import _gcs_async_wrapper

    current_fw = "pandas"
    monkeypatch.setattr(
        manager_mod.default_usage_tracker,
        "collect_tokens_map",
        lambda: {"fw": f"fw/{current_fw}"},
    )

    captured = []

    async def file_operation():
        captured.append(get_telemetry_context().get("fw"))

    wrapped = _gcs_async_wrapper(file_operation)

    current_fw = "pandas"
    await wrapped()
    assert captured[-1] == "fw/pandas"
    assert get_telemetry_context() == {}

    current_fw = "torch"
    await wrapped()
    assert captured[-1] == "fw/torch"
    assert get_telemetry_context() == {}


def test_async_gen_wrapper_aclose_on_early_exit():
    """Verify that _gcs_async_gen_wrapper properly closes the underlying async generator on early exit."""
    from gcsfs.core import GCSFileSystem
    from gcsfs.telemetry.manager import _gcs_async_gen_wrapper

    closed = False

    async def sample_async_gen():
        nonlocal closed
        try:
            yield 1
            yield 2
            yield 3
        finally:
            closed = True

    fs = GCSFileSystem(token="anon")
    wrapped_gen = _gcs_async_gen_wrapper(sample_async_gen, obj=fs)
    gen_instance = wrapped_gen()
    val = next(gen_instance)
    assert val == 1
    assert not closed
    # Close the sync generator early (simulating break in for loop)
    gen_instance.close()
    assert closed


@pytest.mark.asyncio
async def test_file_telemetry_wrapper_sync_and_async():
    """Verify _file_telemetry_wrapper propagates caller_framework for both sync and async methods."""
    from gcsfs.telemetry.manager import _file_telemetry_wrapper

    class DummyFile:
        def __init__(self, caller_framework: str):
            self.caller_framework = caller_framework

        def sync_op(self):
            return get_telemetry_context(Dimension.FRAMEWORK)

        async def async_op(self):
            return get_telemetry_context(Dimension.FRAMEWORK)

    DummyFile.sync_op = _file_telemetry_wrapper(DummyFile.sync_op)
    DummyFile.async_op = _file_telemetry_wrapper(DummyFile.async_op)

    f = DummyFile("fw/pytorch")
    assert f.sync_op() == "fw/pytorch"
    assert get_telemetry_context(Dimension.FRAMEWORK) is None

    assert await f.async_op() == "fw/pytorch"
    assert get_telemetry_context(Dimension.FRAMEWORK) is None


def test_mirror_gcs_methods_and_subclass_polymorphism(monkeypatch):
    """Verify mirror_gcs_methods binds sync/async/gen wrappers and preserves subclass overrides & mocks."""
    import fsspec.asyn

    from gcsfs.telemetry.manager import mirror_gcs_methods

    class BaseDummyFS(fsspec.asyn.AsyncFileSystem):
        async def _ls(self, path, detail=False, **kwargs):
            return [f"base:{path}"]

        async def _cat_file(self, path, start=None, end=None, **kwargs):
            return f"base_cat:{path}".encode()

        async def _walk(self, path, maxdepth=None, **kwargs):
            yield (f"base_walk:{path}", [], ["a.txt"])

    mirror_gcs_methods(BaseDummyFS)

    class SubDummyFS(BaseDummyFS):
        async def _ls(self, path, detail=False, **kwargs):
            return [f"sub:{path}"]

        async def _walk(self, path, maxdepth=None, **kwargs):
            yield (f"sub_walk:{path}", [], ["b.txt"])

    sub_fs = SubDummyFS(skip_instance_cache=True)
    assert sub_fs.ls("bucket") == ["sub:bucket"]
    assert list(sub_fs.walk("bucket")) == [("sub_walk:bucket", [], ["b.txt"])]

    base_fs = BaseDummyFS(skip_instance_cache=True)

    async def mocked_cat_file(self, path, start=None, end=None, **kwargs):
        return f"mocked:{path}".encode()

    monkeypatch.setattr(BaseDummyFS, "_cat_file", mocked_cat_file)
    assert base_fs.cat_file("bucket/file.txt") == b"mocked:bucket/file.txt"
