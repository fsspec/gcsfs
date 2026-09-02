"""Central Telemetry Coordinator managing detectors and context propagation."""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Dict, List, Optional

from gcsfs.telemetry.context import Dimension, get_telemetry_context
from gcsfs.telemetry.detectors.base import BaseDetector
from gcsfs.telemetry.detectors.framework import FrameworkDetector


def _gcs_sync_wrapper(func: Callable, obj: Any = None) -> Callable:
    """
    GCS-specific sync wrapper that captures caller thread telemetry
    and bridges it into the background asyncio loop thread via self._sync.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self = obj or (args[0] if args else None)
        if self is not None and hasattr(self, "_sync"):
            return self._sync(func, *args, **kwargs)
        # Fallback if unbound
        import fsspec.asyn

        return fsspec.asyn.sync(self.loop, func, *args, **kwargs)

    return wrapper


def _gcs_async_gen_wrapper(func: Callable, obj: Any = None) -> Callable:
    """
    GCS-specific async generator wrapper that routes each generator yield
    through self._sync to capture caller thread telemetry context.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self = obj or (args[0] if args else None)
        gen = func(*args, **kwargs)
        while True:
            try:
                if self is not None and hasattr(self, "_sync"):
                    yield self._sync(gen.__anext__)
                else:
                    import fsspec.asyn

                    yield fsspec.asyn.sync(self.loop, gen.__anext__)
            except StopAsyncIteration:
                break

    return wrapper


def mirror_gcs_sync_methods(obj: Any) -> None:
    """
    Binds sync methods on a GCSFileSystem instance using _gcs_sync_wrapper.
    Uses __dict__ across AsyncFileSystem, GCSFileSystem, and the concrete type
    to find all async coroutines and async generators, avoiding object inspection.
    """
    from fsspec.asyn import AsyncFileSystem, async_methods

    from gcsfs.core import GCSFileSystem

    # Collect coroutine and async generator method names strictly from target class dictionaries
    target_classes = {AsyncFileSystem, GCSFileSystem, type(obj)}
    method_names = set(async_methods + ["_call"])

    for cls in target_classes:
        for name, attr in cls.__dict__.items():
            # Only pick private methods that are NOT dunders (e.g. '_ls', '_cat_file', '_info')
            if name.startswith("_") and not (
                name.startswith("__") and name.endswith("__")
            ):
                if inspect.iscoroutinefunction(attr) or inspect.isasyncgenfunction(
                    attr
                ):
                    method_names.add(name)

    # Bind the sync wrapper only for valid public methods
    from fsspec.spec import AbstractFileSystem

    for method in method_names:
        smethod = method[
            1:
        ]  # e.g., "_ls" -> "ls", "_cat_file" -> "cat_file", "_walk" -> "walk"

        # Only mirror if smethod is a valid public method (in AbstractFileSystem, async_methods, or the class)
        if not (
            hasattr(AbstractFileSystem, smethod)
            or method in async_methods
            or hasattr(type(obj), smethod)
        ):
            continue

        async_fn = getattr(obj, method, None)
        if inspect.iscoroutinefunction(async_fn):
            mth = _gcs_sync_wrapper(async_fn, obj=obj)
        elif inspect.isasyncgenfunction(async_fn):
            mth = _gcs_async_gen_wrapper(async_fn, obj=obj)
        else:
            continue

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
                    if detected_val:
                        tokens[dim_key] = detected_val
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
        return list(token_map.values())

    def get_dimension(self, dimension: Dimension) -> Optional[str]:
        """
        Resolve a specific dimension token (e.g. Dimension.FRAMEWORK).
        """
        token_map = self.collect_tokens_map()
        return token_map.get(dimension.value)


# Default singleton instance pre-configured with standard framework detector
default_usage_tracker = UsageMetricsTracker(
    detectors=[
        FrameworkDetector(),
    ]
)
