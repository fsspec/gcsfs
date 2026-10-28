"""Method wrappers and cross-thread context propagation helpers for telemetry."""

from __future__ import annotations

import contextlib
import functools
import inspect
from typing import Any, Callable, Dict, Optional

from gcsfs.telemetry import manager
from gcsfs.telemetry.context import (
    Dimension,
    get_telemetry_context,
    has_telemetry_context,
    reset_telemetry_context,
    set_telemetry_context,
)


@contextlib.contextmanager
def _telemetry_context(tokens_map: Optional[Dict[str, str]] = None):
    """Scope telemetry context for the current execution, no-oping if already active."""
    if has_telemetry_context():
        yield
        return
    token = set_telemetry_context(
        tokens_map
        if tokens_map is not None
        else manager.default_usage_tracker.collect_tokens_map()
    )
    try:
        yield
    finally:
        reset_telemetry_context(token)


def _run_sync(
    loop: Any,
    func: Callable,
    *args,
    tokens_map: Optional[Dict[str, str]] = None,
    **kwargs,
) -> Any:
    """Set telemetry context on the caller thread and execute `func` via `fsspec.asyn.sync`."""
    import fsspec.asyn

    with _telemetry_context(tokens_map):
        return fsspec.asyn.sync(loop, func, *args, **kwargs)


def _gcs_async_wrapper(func: Callable, super_cls: Optional[type] = None) -> Callable:
    """
    GCS-specific async wrapper for coroutines and async generators that scopes telemetry
    context during native async execution, avoiding repetitive stack-walking across
    internal coroutines, chunk fetches, or generator steps.
    """
    if getattr(func, "_is_gcs_async_wrapped", False):
        return func

    def _resolve(args: tuple) -> tuple[Callable, tuple]:
        if super_cls is not None and args:
            return getattr(super(super_cls, args[0]), func.__name__), args[1:]
        return func, args

    if inspect.isasyncgenfunction(func):

        @functools.wraps(func)
        async def gen_wrapper(*args, **kwargs):
            target, pass_args = _resolve(args)
            # Detect once per iteration; scope context to each step so it never leaks to the consumer
            tokens_map = manager.default_usage_tracker.collect_tokens_map()
            gen = target(*pass_args, **kwargs)
            try:
                while True:
                    with _telemetry_context(tokens_map):
                        try:
                            item = await gen.__anext__()
                        except StopAsyncIteration:
                            return
                    yield item
            finally:
                await gen.aclose()

        gen_wrapper._is_gcs_async_wrapped = True
        return gen_wrapper

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        target, pass_args = _resolve(args)
        # O(1) fast path for nested calls (already inside _run_sync or another wrapped call)
        if has_telemetry_context():
            return await target(*pass_args, **kwargs)
        with _telemetry_context():
            return await target(*pass_args, **kwargs)

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
            # Detect once on the caller thread and reuse it for every yielded step
            tokens_map = manager.default_usage_tracker.collect_tokens_map()
            gen = actual_func(*pass_args, **kwargs)
            try:
                while True:
                    try:
                        yield _run_sync(self.loop, gen.__anext__, tokens_map=tokens_map)
                    except StopAsyncIteration:
                        break
            finally:
                if hasattr(gen, "aclose"):
                    with contextlib.suppress(Exception):
                        _run_sync(self.loop, gen.aclose, tokens_map=tokens_map)

        gen_wrapper._is_gcs_sync_wrapped = True
        return gen_wrapper

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self, actual_func, pass_args = _resolve_target_method(func, args)
        return _run_sync(self.loop, actual_func, *pass_args, **kwargs)

    wrapper._is_gcs_sync_wrapped = True
    return wrapper


def mirror_gcs_methods(obj: type) -> None:
    """
    Binds sync and native async methods on a GCSFileSystem class using
    telemetry-aware wrappers.
    Uses class __mro__ to find all async coroutines and async generators,
    avoiding eager property evaluation.

    Note: methods inherited from fsspec (e.g. `AsyncFileSystem._cat`) are wrapped and stored
    on `obj` itself, since fsspec's shared base classes must not be modified. A subclass that
    lists another `AsyncFileSystem` subclass *after* `obj` (e.g. `class MyFS(GCSFileSystem,
    Mixin)`) will therefore resolve those methods to `obj`'s wrapper instead of the mixin's
    override. Mixins listed before `obj` (as in `ExtendedGcsFileSystem`) are unaffected.
    """
    from fsspec.asyn import AsyncFileSystem, async_methods

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

        is_public_sync = (
            hasattr(AbstractFileSystem, smethod)
            or method in async_methods
            or hasattr(obj, smethod)
        )
        # Only wrap public API or AsyncFileSystem methods (skip gcsfs-internal helpers like _request)
        if not (is_public_sync or hasattr(AsyncFileSystem, method)):
            continue

        async_fn = getattr(obj, method, None)

        if inspect.iscoroutinefunction(async_fn) or inspect.isasyncgenfunction(
            async_fn
        ):
            # Wrap each native async method once. Skip ones a parent already wrapped, so subclasses
            # inherit the parent's wrapper instead of a copy that would hide patches on the parent.
            if not getattr(async_fn, "_is_gcs_async_wrapped", False):
                super_cls = None if method in obj.__dict__ else obj
                async_fn = _gcs_async_wrapper(async_fn, super_cls=super_cls)
                setattr(obj, method, async_fn)
            if is_public_sync:
                mth = _gcs_sync_wrapper(async_fn)
                if not getattr(mth, "__doc__", None):
                    mth.__doc__ = getattr(
                        getattr(AbstractFileSystem, smethod, None), "__doc__", ""
                    )
                setattr(obj, smethod, mth)


def _setup_file_telemetry(file_obj) -> Optional[Any]:
    fw = getattr(file_obj, "caller_framework", None)

    if not isinstance(fw, str):
        return None

    # Fast path: framework already active (e.g. nested file method calls), skip a redundant set/reset
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


def _file_telemetry_wrapper(func: Callable) -> Callable:
    """Wraps a File sync or async method to propagate `self.caller_framework`."""
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


def wrap_file_methods(cls: Any = None) -> None:
    """Wrap all callable methods and properties on a File class (`GCSFile` by default, or `ZonalFile`)."""
    if cls is None:
        from gcsfs.core import GCSFile

        cls = GCSFile

    for name, attr in list(cls.__dict__.items()):
        if name.startswith("__") or isinstance(attr, (classmethod, staticmethod)):
            continue
        is_prop = isinstance(attr, property)
        func = attr.fget if is_prop else (attr if callable(attr) else None)
        if func is not None and not getattr(func, "_is_telemetry_wrapped", False):
            wrapped = _file_telemetry_wrapper(func)
            wrapped._is_telemetry_wrapped = True
            setattr(cls, name, attr.getter(wrapped) if is_prop else wrapped)
