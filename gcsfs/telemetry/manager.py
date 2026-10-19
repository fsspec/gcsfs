"""Central Telemetry Coordinator managing detectors and context propagation."""

from __future__ import annotations

import contextlib
import functools
import inspect
import os
from typing import Any, Callable, Dict, List, Optional

from gcsfs.telemetry.context import (
    Dimension,
    get_telemetry_context,
    has_telemetry_context,
    reset_telemetry_context,
    sanitize_token,
    set_telemetry_context,
)
from gcsfs.telemetry.detector import BaseDetector, FrameworkDetector


def _gcs_async_wrapper(func: Callable) -> Callable:
    """
    GCS-specific async wrapper that scopes telemetry context during native async execution,
    avoiding repetitive stack-walking across internal coroutines or chunk fetches.
    """
    if getattr(func, "_is_gcs_async_wrapped", False):
        return func

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # O(1) fast path for nested calls (already inside _run_sync or another wrapped call)
        if has_telemetry_context():
            return await func(*args, **kwargs)

        tokens_map = default_usage_tracker.collect_tokens_map()
        token = set_telemetry_context(tokens_map)
        try:
            return await func(*args, **kwargs)
        finally:
            reset_telemetry_context(token)

    wrapper._is_gcs_async_wrapped = True
    return wrapper


def _resolve_target_method(func: Callable, args: tuple) -> tuple[Any, Callable, tuple]:
    """Dynamically resolve `func` on `self` to preserve subclass overrides and mocks."""
    self = args[0]
    method_name = getattr(func, "__name__", None)
    actual_func = getattr(self, method_name, None) if method_name else None
    if actual_func is not None:
        return self, actual_func, args[1:]
    return self, func, args


def _run_sync(loop: Any, func: Callable, *args, **kwargs) -> Any:
    """Set telemetry context on the caller thread and execute `func` via `fsspec.asyn.sync`."""
    import fsspec.asyn

    token = None
    if not has_telemetry_context():
        token = set_telemetry_context(default_usage_tracker.collect_tokens_map())
    try:
        return fsspec.asyn.sync(loop, func, *args, **kwargs)
    finally:
        if token is not None:
            reset_telemetry_context(token)


def _gcs_sync_wrapper(func: Callable) -> Callable:
    """
    GCS-specific sync wrapper for coroutines and async generators that captures
    caller thread telemetry and bridges it into the background loop thread via `fsspec.asyn.sync`.
    """
    if getattr(func, "_is_gcs_sync_wrapped", False):
        return func

    if inspect.isasyncgenfunction(func):

        @functools.wraps(func)
        def gen_wrapper(*args, **kwargs):
            self, actual_func, pass_args = _resolve_target_method(func, args)
            gen = actual_func(*pass_args, **kwargs)
            try:
                while True:
                    try:
                        yield _run_sync(self.loop, gen.__anext__)
                    except StopAsyncIteration:
                        break
            finally:
                if hasattr(gen, "aclose"):
                    with contextlib.suppress(Exception):
                        _run_sync(self.loop, gen.aclose)

        gen_wrapper._is_gcs_sync_wrapped = True
        return gen_wrapper

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self, actual_func, pass_args = _resolve_target_method(func, args)
        return _run_sync(self.loop, actual_func, *pass_args, **kwargs)

    wrapper._is_gcs_sync_wrapped = True
    return wrapper


def _setup_file_telemetry(file_obj) -> Optional[Any]:
    fw = getattr(file_obj, "caller_framework", None)

    if not isinstance(fw, str):
        return None

    # O(1) zero-allocation fast path check
    if get_telemetry_context().get(Dimension.FRAMEWORK) == fw:
        return None

    return set_telemetry_context(Dimension.FRAMEWORK, fw)


@contextlib.contextmanager
def _file_telemetry_context(file_obj):
    """Context manager to safely handle file telemetry token setup and teardown."""
    token = _setup_file_telemetry(file_obj)
    try:
        yield
    finally:
        if token is not None:
            reset_telemetry_context(token)


def _file_telemetry_wrapper(func):
    """Wraps a GCSFile sync or async method to propagate self.caller_framework."""
    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            with _file_telemetry_context(self):
                return await func(self, *args, **kwargs)

        return async_wrapper

    @functools.wraps(func)
    def sync_wrapper(self, *args, **kwargs):
        with _file_telemetry_context(self):
            return func(self, *args, **kwargs)

    return sync_wrapper


def wrap_file_methods():
    from gcsfs.core import GCSFile

    for name, attr in list(GCSFile.__dict__.items()):
        if (
            not name.startswith("__")
            and callable(attr)
            and not isinstance(attr, (classmethod, staticmethod))
        ):
            if not getattr(attr, "_is_telemetry_wrapped", False):
                wrapped = _file_telemetry_wrapper(attr)
                wrapped._is_telemetry_wrapped = True
                setattr(GCSFile, name, wrapped)


def mirror_gcs_methods(obj: type) -> None:
    """
    Binds sync and native async methods on a GCSFileSystem class using
    telemetry-aware wrappers.
    Uses class __mro__ to find all async coroutines and async generators,
    avoiding eager property evaluation.
    """
    from fsspec.asyn import async_methods

    # Collect coroutine and async generator method names strictly from class dictionaries in MRO
    method_names = set(async_methods)

    for cls in obj.__mro__:
        if cls is object:
            continue
        for name, attr in cls.__dict__.items():
            # Only pick private methods that are NOT dunders (e.g. '_ls', '_cat_file', '_info')
            if name.startswith("_") and not (
                name.startswith("__") and name.endswith("__")
            ):
                if inspect.iscoroutinefunction(attr) or inspect.isasyncgenfunction(
                    attr
                ):
                    method_names.add(name)

    # Bind the wrappers for valid methods
    from fsspec.spec import AbstractFileSystem

    for method in method_names:
        smethod = method[
            1:
        ]  # e.g., "_ls" -> "ls", "_cat_file" -> "cat_file", "_walk" -> "walk"

        async_fn = getattr(obj, method, None)
        mth = None

        if inspect.iscoroutinefunction(async_fn):
            # Wrap each native async method once. Skip ones a parent already wrapped, so subclasses
            # inherit the parent's wrapper instead of a copy that would hide patches on the parent.
            if not getattr(async_fn, "_is_gcs_async_wrapped", False):
                async_fn = _gcs_async_wrapper(async_fn)
                setattr(obj, method, async_fn)
            mth = _gcs_sync_wrapper(async_fn)

        elif inspect.isasyncgenfunction(async_fn):
            mth = _gcs_sync_wrapper(async_fn)

        # Only mirror sync method if smethod is a valid public method
        if mth is not None and (
            hasattr(AbstractFileSystem, smethod)
            or method in async_methods
            or hasattr(obj, smethod)
        ):
            if not getattr(mth, "__doc__", None):
                mth.__doc__ = getattr(
                    getattr(AbstractFileSystem, smethod, None), "__doc__", ""
                )
            setattr(obj, smethod, mth)


class UsageMetricsTracker:
    """Coordinates multi-dimensional detectors and context propagation."""

    def __init__(self, detectors: Optional[List[BaseDetector]] = None):
        self.detectors: List[BaseDetector] = (
            list(detectors) if detectors is not None else []
        )

    def is_enabled(self) -> bool:
        """Check if telemetry collection is enabled globally."""
        return not any(
            os.environ.get(env, "").lower() in ("1", "true", "yes")
            for env in ("DO_NOT_TRACK", "GCSFS_NO_TELEMETRY")
        )

    def collect_tokens_map(self) -> Dict[str, str]:
        """
        Collect active telemetry tokens across all registered detectors.

        Returns
        -------
        Dict[str, str]: Mapping of dimension key to bare token value
            (or empty string "" as a negative-cache sentinel when undetected).
        """
        if not self.is_enabled():
            return {}

        tokens = get_telemetry_context()

        # Run registered detectors for any dimensions not already set
        for detector in self.detectors:
            dim_key = (
                detector.name.value
                if isinstance(detector.name, Dimension)
                else str(detector.name)
            )
            if dim_key not in tokens:
                try:
                    detected_val = detector.detect()
                    tokens[dim_key] = detected_val or ""
                except Exception:
                    tokens[dim_key] = ""

        return tokens

    def get_tokens(self) -> List[str]:
        """
        Return a list of all active telemetry tokens to be included in HTTP User-Agent.

        Returns
        -------
        List[str]: List of sanitized formatted token strings, e.g. ['fw/pandas', 'env/gke'].
        """
        token_map = self.collect_tokens_map()
        out = []
        for dim_key, val in token_map.items():
            clean_val = sanitize_token(val)
            if clean_val:
                out.append(f"{dim_key}/{clean_val}")
        return out

    def get_dimension(self, dimension: Dimension) -> Optional[str]:
        """
        Resolve a specific dimension's sanitized bare token value (e.g. Dimension.FRAMEWORK -> 'pandas').
        """
        token_map = self.collect_tokens_map()
        return sanitize_token(token_map.get(dimension.value))


# Default singleton instance pre-configured with standard framework detector
default_usage_tracker = UsageMetricsTracker(
    detectors=[
        FrameworkDetector(),
    ]
)
