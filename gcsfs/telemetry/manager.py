"""Central Telemetry Coordinator managing detectors and context propagation."""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Dict, List, Optional

from gcsfs.telemetry.context import (
    Dimension,
    get_telemetry_context,
    reset_telemetry_context,
    set_telemetry_context,
)
from gcsfs.telemetry.detectors.base import BaseDetector
from gcsfs.telemetry.detectors.framework import FrameworkDetector
from gcsfs.telemetry.sanitizer import sanitize_token


def _gcs_async_wrapper(func: Callable, obj: Any = None) -> Callable:
    """
    GCS-specific async wrapper that scopes telemetry context during native async execution,
    avoiding repetitive stack-walking across internal coroutines or chunk fetches.
    """
    if getattr(func, "_is_gcs_async_wrapped", False):
        return func

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # Fast path: If telemetry context is already active (e.g. nested call or _sync), pass through in O(1)
        if get_telemetry_context():
            return await func(*args, **kwargs)

        tokens_map = default_usage_tracker.collect_tokens_map()
        token = set_telemetry_context(tokens_map)
        try:
            return await func(*args, **kwargs)
        finally:
            reset_telemetry_context(token)

    wrapper._is_gcs_async_wrapped = True
    return wrapper


def _gcs_sync_wrapper(func: Callable, obj: Any = None) -> Callable:
    """
    GCS-specific sync wrapper that captures caller thread telemetry
    and bridges it into the background asyncio loop thread via self._sync.
    """
    if getattr(func, "_is_gcs_sync_wrapped", False):
        return func

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self = obj or args[0]
        if hasattr(self, "_sync"):
            return self._sync(func, *args, **kwargs)
        # Fallback if unbound
        import fsspec.asyn

        return fsspec.asyn.sync(self.loop, func, *args, **kwargs)

    wrapper._is_gcs_sync_wrapped = True
    return wrapper


def _gcs_async_gen_wrapper(func: Callable, obj: Any = None) -> Callable:
    """
    GCS-specific async generator wrapper that routes each generator yield
    through self._sync to capture caller thread telemetry context.
    """
    if getattr(func, "_is_gcs_gen_wrapped", False):
        return func

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self = obj or args[0]
        gen = func(*args, **kwargs)
        try:
            while True:
                try:
                    if hasattr(self, "_sync"):
                        yield self._sync(gen.__anext__)
                    else:
                        import fsspec.asyn

                        yield fsspec.asyn.sync(self.loop, gen.__anext__)
                except StopAsyncIteration:
                    break
        finally:
            if hasattr(gen, "aclose"):
                try:
                    import fsspec.asyn

                    fsspec.asyn.sync(self.loop, gen.aclose)
                except Exception:
                    pass

    wrapper._is_gcs_gen_wrapped = True
    return wrapper


def mirror_gcs_methods(obj: Any) -> None:
    """
    Binds sync and native async methods on a GCSFileSystem class or instance using
    telemetry-aware wrappers.
    Uses class __mro__ to find all async coroutines and async generators,
    avoiding eager property evaluation.
    """
    from fsspec.asyn import async_methods

    is_class = isinstance(obj, type)
    target_type = obj if is_class else type(obj)

    # Collect coroutine and async generator method names strictly from class dictionaries in MRO
    method_names = set(async_methods)

    for cls in target_type.__mro__:
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
            # Only wrap private async methods defined directly on this class to preserve inheritance
            if is_class and method in target_type.__dict__:
                wrapped_async = _gcs_async_wrapper(async_fn)
                setattr(obj, method, wrapped_async)
            mth = _gcs_sync_wrapper(async_fn, obj=None if is_class else obj)

        elif inspect.isasyncgenfunction(async_fn):
            mth = _gcs_async_gen_wrapper(async_fn, obj=None if is_class else obj)

        # Only mirror sync method if smethod is a valid public method
        if mth is not None and (
            hasattr(AbstractFileSystem, smethod)
            or method in async_methods
            or hasattr(target_type, smethod)
        ):
            if not getattr(mth, "__doc__", None):
                mth.__doc__ = getattr(
                    getattr(AbstractFileSystem, smethod, None), "__doc__", ""
                )
            setattr(obj, smethod, mth)


# Alias for backward compatibility
mirror_gcs_sync_methods = mirror_gcs_methods


class UsageMetricsTracker:
    """Coordinates multi-dimensional detectors and context propagation."""

    def __init__(self, detectors: Optional[List[BaseDetector]] = None):
        self.detectors: List[BaseDetector] = (
            list(detectors) if detectors is not None else []
        )

    def register_detector(self, detector: BaseDetector) -> None:
        """Register a new detector for a telemetry dimension (e.g. env, op)."""
        self.detectors.append(detector)

    def collect_tokens_map(self) -> Dict[str, str]:
        """
        Collect active telemetry tokens across all registered detectors.

        Returns
        -------
        Dict[str, str]: Mapping of dimension name to formatted token string.
        """
        tokens = get_telemetry_context()

        # Run registered detectors for any dimensions not already set
        for detector in self.detectors:
            dim_key = (
                detector.name.value
                if isinstance(detector.name, Dimension)
                else str(detector.name)
            )
            if detector.is_enabled() and dim_key not in tokens:
                try:
                    detected_val = detector.detect()
                    tokens[dim_key] = detected_val or ""
                except Exception:
                    pass

        return tokens

    def get_tokens(self) -> List[str]:
        """
        Return a list of all active telemetry tokens to be included in HTTP User-Agent.

        Returns
        -------
        List[str]: List of formatted token strings, e.g. ['fw/pandas', 'env/gke'].
        """
        token_map = self.collect_tokens_map()
        sanitized_tokens = []
        for token in token_map.values():
            clean = sanitize_token(token)
            if clean:
                sanitized_tokens.append(clean)
        return sanitized_tokens

    def get_dimension(self, dimension: Dimension) -> Optional[str]:
        """
        Resolve a specific dimension token (e.g. Dimension.FRAMEWORK).
        """
        token_map = self.collect_tokens_map()
        return sanitize_token(token_map.get(dimension.value))


# Default singleton instance pre-configured with standard framework detector
default_usage_tracker = UsageMetricsTracker(
    detectors=[
        FrameworkDetector(),
    ]
)
