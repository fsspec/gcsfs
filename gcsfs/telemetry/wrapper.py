"""Method wrappers and cross-thread context propagation helpers for telemetry."""

from __future__ import annotations

import contextlib
import contextvars
import functools
import inspect
import types
from collections.abc import Callable
from typing import Any

from gcsfs.telemetry import manager
from gcsfs.telemetry.context import (
    ALL_DIMENSIONS,
    Dimension,
    get_telemetry_context,
    has_telemetry_context,
    reset_telemetry_context,
    set_telemetry_context,
)

_UNSET = object()

_TRACKED_METHODS = frozenset(
    {
        "cat",
        "cat_file",
        "cat_ranges",
        "copy",
        "cp",
        "cp_file",
        "created",
        "disk_usage",
        "du",
        "exists",
        "expand_path",
        "find",
        "get",
        "get_file",
        "glob",
        "head",
        "info",
        "isdir",
        "isfile",
        "ls",
        "makedirs",
        "merge",
        "mkdir",
        "mkdirs",
        "modified",
        "move",
        "mv",
        "mv_file",
        "open",
        "open_async",
        "pipe",
        "pipe_file",
        "put",
        "put_file",
        "read_text",
        "rm",
        "rm_file",
        "rmdir",
        "sign",
        "size",
        "sizes",
        "tail",
        "touch",
        "tree",
        "walk",
        "write_text",
    }
)

_SYNC_ALIAS_METHODS: dict[str, str] = {
    "listdir": "ls",
}

_UNTRACKED_COMPOSITE_METHODS = frozenset(
    {
        "checksum",
        "read_block",
        "ukey",
    }
)


def _tracked_method_name(func: Callable) -> str | None:
    """Return the tracked public method name for `func`, or None if untracked."""
    raw_name = getattr(func, "__name__", "")
    if raw_name in _SYNC_ALIAS_METHODS:
        return _SYNC_ALIAS_METHODS[raw_name]
    name = raw_name.removeprefix("_")
    return name if name in _TRACKED_METHODS else None


def _resolve_super_method(
    func: Callable, super_cls: type | None, args: tuple
) -> tuple[Callable, tuple]:
    """Resolve `func` via `super(super_cls, self)` when wrapped on a subclass."""
    if super_cls is not None and args and isinstance(args[0], super_cls):
        return getattr(super(super_cls, args[0]), func.__name__), args[1:]
    return func, args


@contextlib.contextmanager
def _telemetry_context(tokens_map: dict[str, str] | None):
    """Scope `tokens_map` as the active telemetry context for the current execution."""
    if tokens_map is None:
        yield
        return
    effective = tokens_map if tokens_map else _disabled_tokens_map()
    token = set_telemetry_context(effective)
    try:
        yield
    finally:
        reset_telemetry_context(token)


def _disabled_tokens_map() -> dict[str, str]:
    """Return negative-cache sentinels for all dimensions and registered detectors."""
    out = {d.value: "" for d in ALL_DIMENSIONS}
    for det in manager.default_usage_tracker.detectors:
        key = det.name.value if isinstance(det.name, Dimension) else str(det.name)
        out[key] = ""
    return out


def _is_valid_reopen_tuple(reopen_tel: Any) -> bool:
    """Return whether `reopen_tel` is a well-formed `(framework, method, ...)` tuple."""
    return (
        isinstance(reopen_tel, tuple)
        and len(reopen_tel) >= 2
        and isinstance(reopen_tel[0], str)
        and isinstance(reopen_tel[1], str)
    )


def _resolve_reopen_tokens(reopen_tel: Any) -> dict[str, str]:
    """Build the telemetry token map for a pickled file handle being reopened."""
    if not manager.default_usage_tracker.is_enabled() or reopen_tel is None:
        return _disabled_tokens_map()
    fw, method = reopen_tel[0], reopen_tel[1]
    with _telemetry_context(
        {Dimension.FRAMEWORK.value: fw, Dimension.METHOD.value: method}
    ):
        return manager.default_usage_tracker.collect_tokens_map()


def with_telemetry_context(func: Callable) -> Callable:
    """
    Decorator for plain sync entry points that `mirror_gcs_methods` doesn't wrap (e.g. `_open`).

    Detects the caller framework and records `Dimension.METHOD` once on the caller thread
    when no method context is active, so nested requests carry them across thread boundaries.
    """
    method_name = _tracked_method_name(func)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if has_telemetry_context(Dimension.METHOD):
            return func(*args, **kwargs)
        tokens_map = manager.default_usage_tracker.collect_tokens_map(
            method=method_name or ""
        )
        with _telemetry_context(tokens_map):
            return func(*args, **kwargs)

    return wrapper


def _run_sync(
    loop: Any,
    func: Callable,
    *args,
    tokens_map: dict[str, str] | None = None,
    **kwargs,
) -> Any:
    """Set telemetry context on the caller thread and execute `func` via `fsspec.asyn.sync`."""
    import fsspec.asyn

    if tokens_map is None:
        if has_telemetry_context(Dimension.METHOD):
            return fsspec.asyn.sync(loop, func, *args, **kwargs)
        resolved_method = _tracked_method_name(func)
        tokens_map = manager.default_usage_tracker.collect_tokens_map(
            method=resolved_method or ""
        )
    with _telemetry_context(tokens_map):
        return fsspec.asyn.sync(loop, func, *args, **kwargs)


def _gcs_async_wrapper(
    func: Callable,
    super_cls: type | None = None,
    method_name: Any = _UNSET,
) -> Callable:
    """
    GCS-specific async wrapper for coroutines and async generators that scopes telemetry
    context during native async execution, avoiding repetitive stack-walking across
    internal coroutines, chunk fetches, or generator steps.
    """
    if getattr(func, "_is_gcs_async_wrapped", False):
        return func

    if method_name is _UNSET:
        method_name = _tracked_method_name(func)

    if inspect.isasyncgenfunction(func):

        @functools.wraps(func)
        async def gen_wrapper(*args, **kwargs):
            target, pass_args = _resolve_super_method(func, super_cls, args)
            # Snapshot once on first step and reuse across all yields and close
            if not manager.default_usage_tracker.is_enabled():
                tokens_map = _disabled_tokens_map()
            elif has_telemetry_context(Dimension.METHOD):
                tokens_map = get_telemetry_context()
            else:
                tokens_map = manager.default_usage_tracker.collect_tokens_map(
                    method=method_name or ""
                )
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
                with _telemetry_context(tokens_map):
                    await gen.aclose()

        gen_wrapper._is_gcs_async_wrapped = True
        return gen_wrapper

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        target, pass_args = _resolve_super_method(func, super_cls, args)
        # O(1) fast path for nested calls (already inside _run_sync or another wrapped call)
        if has_telemetry_context(Dimension.METHOD):
            return await target(*pass_args, **kwargs)
        tokens_map = manager.default_usage_tracker.collect_tokens_map(
            method=method_name or ""
        )
        with _telemetry_context(tokens_map):
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


def _gcs_sync_wrapper(
    func: Callable,
    super_cls: type | None = None,
    method_name: Any = _UNSET,
) -> Callable:
    """
    GCS-specific sync wrapper for coroutines, async generators, and synchronous methods
    that captures caller thread telemetry and bridges it into execution.
    """
    if getattr(func, "_is_gcs_sync_wrapped", False):
        return func

    if method_name is _UNSET:
        method_name = _tracked_method_name(func)

    if inspect.isasyncgenfunction(func):

        @functools.wraps(func)
        def gen_wrapper(*args, **kwargs):
            self, actual_func, pass_args = _resolve_target_method(func, args)
            # Snapshot once on first step and reuse across all yields and close
            if not manager.default_usage_tracker.is_enabled():
                tokens_map = _disabled_tokens_map()
            elif has_telemetry_context(Dimension.METHOD):
                tokens_map = get_telemetry_context()
            else:
                tokens_map = manager.default_usage_tracker.collect_tokens_map(
                    method=method_name or ""
                )
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

    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self, actual_func, pass_args = _resolve_target_method(func, args)
            tokens_map = (
                None
                if has_telemetry_context(Dimension.METHOD)
                else manager.default_usage_tracker.collect_tokens_map(
                    method=method_name or ""
                )
            )
            return _run_sync(
                self.loop,
                actual_func,
                *pass_args,
                tokens_map=tokens_map,
                **kwargs,
            )

        wrapper._is_gcs_sync_wrapped = True
        return wrapper

    @functools.wraps(func)
    def sync_fn_wrapper(*args, **kwargs):
        target, pass_args = _resolve_super_method(func, super_cls, args)
        reopen_tel = kwargs.get("_reopen_telemetry", _UNSET)
        if reopen_tel is not _UNSET and (
            reopen_tel is None or _is_valid_reopen_tuple(reopen_tel)
        ):
            with _telemetry_context(_resolve_reopen_tokens(reopen_tel)):
                return target(*pass_args, **kwargs)
        if has_telemetry_context(Dimension.METHOD):
            return target(*pass_args, **kwargs)
        tokens_map = manager.default_usage_tracker.collect_tokens_map(
            method=method_name or ""
        )
        with _telemetry_context(tokens_map):
            return target(*pass_args, **kwargs)

    sync_fn_wrapper._is_gcs_sync_wrapped = True
    return sync_fn_wrapper


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
            # Pick non-dunder private methods (e.g. '_ls') or tracked public async methods (e.g. 'open_async')
            if (
                name.startswith("_")
                and not (name.startswith("__") and name.endswith("__"))
            ) or name in _TRACKED_METHODS:
                if inspect.iscoroutinefunction(attr) or inspect.isasyncgenfunction(
                    attr
                ):
                    method_names.add(name)

    # Bind the wrappers for valid methods
    from fsspec.spec import AbstractFileSystem

    sync_method_names = set(getattr(obj, "_gcs_sync_method_names", ()))
    for method in method_names:
        is_private = method.startswith("_")
        smethod = method.removeprefix(
            "_"
        )  # e.g., "_ls" -> "ls", "_cat_file" -> "cat_file", "_walk" -> "walk"
        tracked_name = smethod if smethod in _TRACKED_METHODS else None

        is_public_sync = is_private and (
            hasattr(AbstractFileSystem, smethod)
            or method in async_methods
            or hasattr(obj, smethod)
        )
        # Only wrap public API or AsyncFileSystem methods (skip gcsfs-internal helpers like _request)
        if not (
            is_public_sync
            or hasattr(AsyncFileSystem, method)
            or method in _TRACKED_METHODS
        ):
            continue

        async_fn = getattr(obj, method, None)

        if inspect.iscoroutinefunction(async_fn) or inspect.isasyncgenfunction(
            async_fn
        ):
            # Wrap each native async method once. Skip ones a parent already wrapped, so subclasses
            # inherit the parent's wrapper instead of a copy that would hide patches on the parent.
            was_already_async_wrapped = getattr(
                async_fn, "_is_gcs_async_wrapped", False
            )
            if not was_already_async_wrapped:
                super_cls = None if method in obj.__dict__ else obj
                async_fn = _gcs_async_wrapper(
                    async_fn, super_cls=super_cls, method_name=tracked_name
                )
                setattr(obj, method, async_fn)
            if is_public_sync and (
                not was_already_async_wrapped
                or not getattr(
                    getattr(obj, smethod, None), "_is_gcs_sync_wrapped", False
                )
            ):
                local_sync = obj.__dict__.get(smethod)
                if (
                    local_sync is not None
                    and method not in obj.__dict__
                    and callable(local_sync)
                    and not inspect.iscoroutinefunction(local_sync)
                    and not inspect.isasyncgenfunction(local_sync)
                    and not getattr(local_sync, "_is_gcs_sync_wrapped", False)
                ):
                    continue
                mth = _gcs_sync_wrapper(async_fn, method_name=tracked_name)
                mth.__dict__.pop("_is_gcs_async_wrapped", None)
                if not getattr(mth, "__doc__", None):
                    mth.__doc__ = getattr(
                        getattr(AbstractFileSystem, smethod, None), "__doc__", ""
                    )
                setattr(obj, smethod, mth)
                sync_method_names.add(smethod)
    obj._gcs_sync_method_names = frozenset(sync_method_names)

    # Wrap synchronous public entry points, aliases (e.g. listdir -> ls), and composite helpers
    sync_targets: list[tuple[str, str | None]] = [
        (name, name) for name in _TRACKED_METHODS
    ]
    sync_targets.extend(_SYNC_ALIAS_METHODS.items())
    sync_targets.extend((name, None) for name in _UNTRACKED_COMPOSITE_METHODS)

    for sync_name, target_method in sync_targets:
        raw_attr = inspect.getattr_static(obj, sync_name, None)
        if isinstance(raw_attr, (staticmethod, classmethod)):
            continue
        sync_fn = getattr(obj, sync_name, None)
        if (
            callable(sync_fn)
            and not inspect.iscoroutinefunction(sync_fn)
            and not inspect.isasyncgenfunction(sync_fn)
            and not getattr(sync_fn, "_is_gcs_sync_wrapped", False)
        ):
            super_cls = None if sync_name in obj.__dict__ else obj
            setattr(
                obj,
                sync_name,
                _gcs_sync_wrapper(
                    sync_fn, super_cls=super_cls, method_name=target_method
                ),
            )


def bind_gcs_sync_methods(fs: Any) -> None:
    """
    Bind the class-level telemetry sync wrappers set by `mirror_gcs_methods` onto `fs`.

    This matches `fsspec.asyn.mirror_sync_methods`, which fsspec runs for every async filesystem
    instance (and which bound GCSFileSystem's sync methods before they moved to the class). The
    bound methods stored in `fs.__dict__` reference `fs`, so the instance is only freed by the
    cyclic garbage collector rather than as soon as its last outside reference is dropped. This
    matters after `fork()`: fsspec clears `cls._cache` in the child (e.g. in
    `subprocess.Popen(..., preexec_fn=...)`, used by `ray.init()`), and an instance only the cache
    referenced must not be torn down there, because deallocating its inherited `grpc.aio` channel
    in the child hangs or crashes.

    This only protects children that exec right away (CPython disables the cyclic GC in the child
    around `preexec_fn`). A forked child that keeps running Python, such as a fork-mode
    `multiprocessing` or PyTorch `DataLoader` worker, can still collect an inherited instance once
    ordinary allocations trigger the cyclic GC, and hang the same way. That was also the case
    before the wrappers moved to the class; use the "spawn" or "forkserver" start method there.

    Methods a subclass overrides with a plain sync function are left alone.
    """
    cls = type(fs)
    for name in getattr(cls, "_gcs_sync_method_names", ()):
        method = getattr(cls, name, None)
        # Require a real function carrying the marker: mocks (e.g. `mock.patch.object(cls, "ls")`)
        # auto-create any attribute, and binding one would pass `fs` as an extra positional arg.
        if (
            inspect.isfunction(method)
            and getattr(method, "_is_gcs_sync_wrapped", False) is True
        ):
            setattr(fs, name, types.MethodType(method, fs))


def _setup_file_telemetry(file_obj) -> contextvars.Token | None:
    """Apply a file handle's cached telemetry context when needed and return its reset token."""
    if not manager.default_usage_tracker.is_enabled():
        return None

    current = get_telemetry_context()
    if getattr(file_obj, "_telemetry_disabled", False):
        disabled_map = _disabled_tokens_map()
        if current == disabled_map:
            return None
        return set_telemetry_context(disabled_map)

    fw = getattr(file_obj, "caller_framework", None)
    has_fw = isinstance(fw, str)
    has_method = hasattr(file_obj, "caller_method")
    if not has_fw and not has_method:
        return None

    if has_method:
        method = file_obj.caller_method
    elif Dimension.METHOD.value in current:
        method = current[Dimension.METHOD.value]
    else:
        method = "open"

    # Fast path: telemetry already active (e.g. nested file method calls), skip a redundant set/reset
    if (not has_fw or current.get(Dimension.FRAMEWORK.value) == fw) and (
        current.get(Dimension.METHOD.value, _UNSET) == method
    ):
        return None

    if has_fw:
        current[Dimension.FRAMEWORK.value] = fw
    current[Dimension.METHOD.value] = method
    token = set_telemetry_context(current)

    missing_detector = any(
        (det.name.value if isinstance(det.name, Dimension) else str(det.name))
        not in current
        for det in manager.default_usage_tracker.detectors
    )
    if missing_detector:
        try:
            filled = manager.default_usage_tracker.collect_tokens_map()
            if filled != current:
                # Initial token holds pre-call state; a single reset unwinds both sets.
                set_telemetry_context(filled)
        except BaseException:
            reset_telemetry_context(token)
            raise
    return token


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
    """Wraps a File sync or async method to propagate caller telemetry."""
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


def _clear_disabled_file_telemetry(file_obj) -> None:
    """Mark a file handle opted out and clear its cached telemetry attributes."""
    file_obj._telemetry_disabled = True
    file_obj.caller_framework = ""
    file_obj.caller_method = ""


def _prepare_file_init_telemetry(
    file_obj, kwargs: dict[str, Any]
) -> tuple[bool, Any, dict[str, str] | None]:
    """Initialize file handle telemetry attributes and capture any active outer context."""
    reopen_tel = kwargs.pop("_reopen_telemetry", _UNSET)
    if (
        reopen_tel is not _UNSET
        and reopen_tel is not None
        and not _is_valid_reopen_tuple(reopen_tel)
    ):
        reopen_tel = _UNSET
    enabled = (
        manager.default_usage_tracker.is_enabled()
        and reopen_tel is not None
        and not getattr(file_obj, "_telemetry_disabled", False)
    )
    if not enabled:
        _clear_disabled_file_telemetry(file_obj)
        return False, reopen_tel, None
    if reopen_tel is not _UNSET:
        file_obj.caller_framework = reopen_tel[0]
        file_obj.caller_method = reopen_tel[1]
    ctx = get_telemetry_context() if has_telemetry_context() else None
    if not hasattr(file_obj, "caller_method"):
        file_obj.caller_method = (
            ctx[Dimension.METHOD.value]
            if (ctx is not None and Dimension.METHOD.value in ctx)
            else "open"
        )
    return True, reopen_tel, ctx


def _resolve_file_init_tokens(
    file_obj, enabled: bool, reopen_tel: Any
) -> dict[str, str]:
    """Build telemetry tokens to scope around file initialization."""
    if not enabled:
        return _disabled_tokens_map()
    if reopen_tel is not _UNSET:
        return _resolve_reopen_tokens(reopen_tel)
    return manager.default_usage_tracker.collect_tokens_map(
        method=file_obj.caller_method
    )


def wrap_file_methods(cls: type) -> None:
    """Wrap all callable methods and properties on a File class (e.g. `GCSFile` or `ZonalFile`)."""
    from gcsfs.core import GCSFile

    is_gcs_file = issubclass(cls, GCSFile)

    orig_init = cls.__dict__.get("__init__", cls.__init__)
    if not getattr(orig_init, "_is_telemetry_init_wrapped", False):

        @functools.wraps(orig_init)
        def init_wrapper(*args, **kwargs):
            self = args[0]
            enabled, reopen_tel, ctx = _prepare_file_init_telemetry(self, kwargs)
            need_scope = is_gcs_file and (
                not enabled
                or reopen_tel is not _UNSET
                or not has_telemetry_context(Dimension.METHOD)
            )
            if need_scope:
                tokens_map = _resolve_file_init_tokens(self, enabled, reopen_tel)
                with _telemetry_context(tokens_map):
                    orig_init(*args, **kwargs)
            else:
                orig_init(*args, **kwargs)

            if not enabled:
                _clear_disabled_file_telemetry(self)
            elif (
                ctx is not None
                and not hasattr(self, "caller_framework")
                and Dimension.FRAMEWORK.value in ctx
            ):
                self.caller_framework = ctx[Dimension.FRAMEWORK.value]

        init_wrapper._is_telemetry_init_wrapped = True
        setattr(cls, "__init__", init_wrapper)

    for name, attr in list(cls.__dict__.items()):
        if name.startswith("__") or isinstance(attr, (classmethod, staticmethod)):
            continue
        is_prop = isinstance(attr, property)
        func = attr.fget if is_prop else (attr if callable(attr) else None)
        if func is not None and not getattr(func, "_is_telemetry_wrapped", False):
            wrapped = _file_telemetry_wrapper(func)
            wrapped._is_telemetry_wrapped = True
            setattr(cls, name, attr.getter(wrapped) if is_prop else wrapped)
