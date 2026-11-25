"""
Method wrappers and cross-thread context propagation helpers for telemetry.

These wrappers capture and propagate call-scoped telemetry tokens across sync/async
and thread boundaries via Python `contextvars` (`collect_tokens_map(call_scoped_only=True)`).
Process-wide detectors (`call_scoped = False`) are skipped when capturing per-call `ContextVar`
snapshots (so background probes are never frozen mid-flight) and are read per request via
`get_tokens()` (`call_scoped_only=False`) unless a value is already set in the context.

Overview of the 6 Execution Flows
---------------------------------
1. Synchronous FileSystem Coroutine Call (e.g. `fs.ls()`, `fs.cat_file()`):
   [Caller Thread]
     fs.ls() -> _gcs_sync_wrapper
       -> _run_sync() -> _telemetry_context(): captures call-scoped tokens ONCE & sets ContextVar
       -> fsspec.asyn.sync(): copies ContextVar to the loop thread (fsspecIO by default)
   [Event Loop Thread]
     fs._ls() -> _gcs_async_wrapper (sees ContextVar already set -> fast-path O(1))
       -> _get_headers(): reads active telemetry tokens for outgoing request headers

2. Synchronous FileSystem Generator (e.g. `for x in fs.walk(...)`):
   [Caller Thread]
     fs.walk() -> _gcs_sync_wrapper (gen_wrapper)
       -> collect_tokens_map(call_scoped_only=True): captures call-scoped tokens ONCE
          when `fs.walk()` is called and returns an inner generator
       -> per yielded step: _run_sync() advances the async generator on the loop thread
          with those captured tokens, resetting ContextVar before yielding so it never
          leaks into the caller's `for` loop body
       -> finally: _run_sync() closes the async generator (`aclose`) with the same tokens

3. Native Asynchronous FileSystem Coroutine Call (e.g. `await fs._cat(...)`):
   [Event Loop Thread]
     await fs._cat() -> _gcs_async_wrapper
       -> _telemetry_context(): captures call-scoped tokens ONCE & sets ContextVar
       -> inner `await fs._cat_file()` sees ContextVar already set -> fast-path O(1)
       -> _get_headers(): reads active telemetry tokens for outgoing request headers

4. Native Asynchronous FileSystem Generator (e.g. `async for x in fs._walk(...)`):
   [Event Loop Thread]
     async for x in fs._walk() -> _gcs_async_wrapper (gen_wrapper)
       -> collect_tokens_map(call_scoped_only=True): captures call-scoped tokens ONCE
          when iteration starts
       -> per yielded step: scopes `_telemetry_context()` around each step (`__anext__`)
          and resets before `yield` so ContextVar never leaks into the caller's `async for` body
       -> finally: closes the underlying async generator (`aclose`)

5. Plain Synchronous Entry Points (e.g. `ExtendedGcsFileSystem._open`):
   Unlike Flow #1 (which bridges a single `async def` coroutine via `fsspec.asyn.sync()`),
   this wraps a plain `def` function that stays on the caller thread and orchestrates
   multiple downstream steps or raw `fsspec.asyn.sync()` calls:
   [Caller Thread]
     fs.open() -> ExtendedGcsFileSystem._open() (@with_telemetry_context)
       -> _telemetry_context(): captures call-scoped tokens ONCE on the caller thread
       -> all inner steps (wrapped sync calls, raw `asyn.sync()` calls, and
          `GCSFile.__init__`) reuse that active ContextVar in O(1)

6. File Object Lifecycle (e.g. `with fs.open(...) as f: f.read()`):
   - Open (`GCSFile.__init__`): caches the caller's framework ONCE on the file instance
     (storing `""` when undetected so negative lookups never re-run detectors) and records
     `open` (or an active outer filesystem method such as `read_text` or `head`) for
     requests issued during initialization.
   - File Operations (`_file_telemetry_wrapper` via `wrap_file_methods`):
     restores the cached framework and attaches per-operation `file_*` method tokens
     (`file_read`, `file_write`, `file_flush`, `file_close`, `file_info`, `file_details`,
     `file_commit`, `file_discard`) across wrapped file methods and properties (chunk fetches,
     upload hooks, `details`, background prefetcher `_async_fetch_range`, explicit
     `GCSFile.flush(force=True)`, and `_close_impl` on the `_defer_close` worker thread),
     while implicit `write()` buffer flushes (`force=False`) keep `file_write` and any
     outer filesystem method that opened the handle continues to apply across threads.

Choosing a Wrapper (Maintainer Guide)
-------------------------------------
- `mirror_gcs_methods(cls)` + `bind_gcs_sync_methods(self)`: Auto-wraps `_foo` coroutines
  and async generators matching `AbstractFileSystem`/`AsyncFileSystem` (or an existing
  `cls.foo` attribute, skipping internal helpers like `_request`) and binds `foo` sync
  mirrors for the public methods.
- `_gcs_sync_wrapper(fn)`: Explicitly wraps async helpers whose sync bridge name does not
  follow the `_foo` -> `foo` convention (e.g. `_sync_lookup_bucket_layout`).
- `@with_telemetry_context`: Decorates plain synchronous entry points not backed by a
  single coroutine (e.g. `ExtendedGcsFileSystem._open`).
- `wrap_file_methods(cls)`: Wraps `__init__` and non-dunder instance methods and properties
  defined directly in `cls.__dict__` on file classes (`GCSFile`, `ZonalFile`).
"""

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
    Dimension,
    get_telemetry_context,
    has_telemetry_context,
    reset_telemetry_context,
    set_telemetry_context,
)
from gcsfs.telemetry.features_proto import GcsfsMethod

_TRACKED_METHODS = frozenset(
    m.name.removeprefix("GCSFS_METHOD_").lower()
    for m in GcsfsMethod
    if not m.name.endswith("_UNSPECIFIED")
    and not m.name.startswith("GCSFS_METHOD_FILE_")
)

_FILE_METHOD_TOKENS: dict[str, str] = {
    "_async_fetch_range": "file_read",
    "_close_impl": "file_close",
    "_ensure_aaow": "file_write",
    "_fetch_range": "file_read",
    "_init_aaow": "file_write",
    "_initiate_upload": "file_write",
    "_simple_upload": "file_write",
    "_upload_chunk": "file_write",
    "close": "file_close",
    "commit": "file_commit",
    "details": "file_details",
    "discard": "file_discard",
    "flush": "file_flush",
    "info": "file_info",
    "write": "file_write",
}


def _tracked_method_name(func: Callable) -> str:
    """Return the tracked public method name for `func`, or empty string if untracked."""
    name = getattr(func, "__name__", "").removeprefix("_")
    return name if name in _TRACKED_METHODS else ""


def _resolve_super_method(
    func: Callable, super_cls: type | None, args: tuple
) -> tuple[Callable, tuple]:
    """Resolve `func` via `super(super_cls, self)` when wrapped on a subclass."""
    if super_cls is not None and args and isinstance(args[0], super_cls):
        return getattr(super(super_cls, args[0]), func.__name__), args[1:]
    return func, args


@contextlib.contextmanager
def _telemetry_context(tokens_map: dict[str, str]):
    """Scope `tokens_map` for the current execution."""
    token = set_telemetry_context(tokens_map)
    try:
        yield
    finally:
        reset_telemetry_context(token)


def with_telemetry_context(func: Callable) -> Callable:
    """
    Decorator for plain sync entry points that `mirror_gcs_methods` doesn't wrap
    (e.g. `ExtendedGcsFileSystem._open`).

    Captures call-scoped telemetry (including `Dimension.METHOD`) once on the caller thread
    when no method context is active, so any requests issued underneath (including via
    `asyn.sync` to the loop thread) carry it.
    """
    return _gcs_sync_wrapper(func)


def _run_sync(
    loop: Any,
    func: Callable,
    *args,
    tokens_map: dict[str, str] | None = None,
    method_name: str = "",
    **kwargs,
) -> Any:
    """Set telemetry context on the caller thread and execute `func` via `fsspec.asyn.sync`."""
    import fsspec.asyn

    if tokens_map is None:
        if has_telemetry_context(Dimension.METHOD):
            return fsspec.asyn.sync(loop, func, *args, **kwargs)
        tokens_map = manager.default_usage_tracker.collect_tokens_map(
            call_scoped_only=True, method=method_name
        )
    with _telemetry_context(tokens_map):
        return fsspec.asyn.sync(loop, func, *args, **kwargs)


def _snapshot_generator_tokens(method_name: str) -> dict[str, str]:
    """Capture call-scoped telemetry tokens once for a generator's steps and close."""
    if has_telemetry_context(Dimension.METHOD):
        return get_telemetry_context()
    return manager.default_usage_tracker.collect_tokens_map(
        call_scoped_only=True, method=method_name
    ) or {Dimension.METHOD.value: method_name}


def _gcs_async_wrapper(
    func: Callable,
    super_cls: type | None = None,
    method_name: str | None = None,
) -> Callable:
    """
    GCS-specific async wrapper for coroutines and async generators that scopes telemetry
    context during native async execution, avoiding repetitive stack-walking across
    internal coroutines, chunk fetches, or generator steps.
    """
    if getattr(func, "_is_gcs_async_wrapped", False):
        return func

    if method_name is None:
        method_name = _tracked_method_name(func)

    if inspect.isasyncgenfunction(func):

        @functools.wraps(func)
        async def gen_wrapper(*args, **kwargs):
            target, pass_args = _resolve_super_method(func, super_cls, args)
            tokens_map = _snapshot_generator_tokens(method_name)
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
            call_scoped_only=True, method=method_name
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
    method_name: str | None = None,
) -> Callable:
    """
    GCS-specific sync wrapper for coroutines, async generators, and synchronous methods
    that captures caller thread telemetry and bridges it into execution.
    """
    if getattr(func, "_is_gcs_sync_wrapped", False):
        return func

    if method_name is None:
        method_name = _tracked_method_name(func)

    if inspect.isasyncgenfunction(func):

        @functools.wraps(func)
        def gen_wrapper(*args, **kwargs):
            self, actual_func, pass_args = _resolve_target_method(func, args)
            tokens_map = _snapshot_generator_tokens(method_name)

            def _iter():
                gen = actual_func(*pass_args, **kwargs)
                try:
                    while True:
                        try:
                            yield _run_sync(
                                self.loop, gen.__anext__, tokens_map=tokens_map
                            )
                        except StopAsyncIteration:
                            break
                finally:
                    if hasattr(gen, "aclose"):
                        with contextlib.suppress(Exception):
                            _run_sync(self.loop, gen.aclose, tokens_map=tokens_map)

            return _iter()

        gen_wrapper._is_gcs_sync_wrapped = True
        return gen_wrapper

    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self, actual_func, pass_args = _resolve_target_method(func, args)
            return _run_sync(
                self.loop,
                actual_func,
                *pass_args,
                method_name=method_name,
                **kwargs,
            )

        wrapper._is_gcs_sync_wrapped = True
        return wrapper

    @functools.wraps(func)
    def sync_fn_wrapper(*args, **kwargs):
        target, pass_args = _resolve_super_method(func, super_cls, args)
        if has_telemetry_context(Dimension.METHOD):
            return target(*pass_args, **kwargs)
        tokens_map = manager.default_usage_tracker.collect_tokens_map(
            call_scoped_only=True, method=method_name
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
        tracked_name = smethod if smethod in _TRACKED_METHODS else ""

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
            if is_public_sync and not was_already_async_wrapped:
                mth = _gcs_sync_wrapper(async_fn, method_name=tracked_name)
                mth.__dict__.pop("_is_gcs_async_wrapped", None)
                if not getattr(mth, "__doc__", None):
                    mth.__doc__ = getattr(
                        getattr(AbstractFileSystem, smethod, None), "__doc__", ""
                    )
                setattr(obj, smethod, mth)
                sync_method_names.add(smethod)
    obj._gcs_sync_method_names = frozenset(sync_method_names)

    # Wrap synchronous public entry points and properties (e.g. buckets, read_bytes)
    for sync_name in _TRACKED_METHODS:
        raw_attr = inspect.getattr_static(obj, sync_name, None)
        if isinstance(raw_attr, property):
            if raw_attr.fget is not None and not getattr(
                raw_attr.fget, "_is_gcs_sync_wrapped", False
            ):
                wrapped_fget = _gcs_sync_wrapper(
                    raw_attr.fget, super_cls=None, method_name=sync_name
                )
                setattr(obj, sync_name, raw_attr.getter(wrapped_fget))
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
                _gcs_sync_wrapper(sync_fn, super_cls=super_cls, method_name=sync_name),
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


def _setup_file_telemetry(file_obj, file_method: str = "") -> contextvars.Token | None:
    """Apply a file handle's cached telemetry context when needed and return its reset token."""
    fw = getattr(file_obj, "caller_framework", None)
    if not isinstance(fw, str):
        return None

    current = get_telemetry_context()

    saved_method = getattr(file_obj, "caller_method", "open")
    current_method = current.get(Dimension.METHOD.value)
    if saved_method != "open":
        method = saved_method
    elif getattr(file_obj, "_initializing_telemetry", False):
        method = "open"
    elif current_method and current_method.startswith("file_"):
        method = current_method
    elif file_method:
        method = file_method
    else:
        method = current_method

    # Fast path: telemetry already active (e.g. nested file method calls), skip a redundant set/reset
    if current.get(Dimension.FRAMEWORK.value) == fw and current_method == method:
        return None

    current[Dimension.FRAMEWORK.value] = fw
    if method is not None:
        current[Dimension.METHOD.value] = method
    return set_telemetry_context(current)


@contextlib.contextmanager
def _file_telemetry_context(file_obj, file_method: str = ""):
    """Context manager to safely handle file telemetry token setup and teardown."""
    token = _setup_file_telemetry(file_obj, file_method)
    try:
        yield
    finally:
        if token is not None:
            reset_telemetry_context(token)


def _file_telemetry_wrapper(func: Callable, file_method: str = "") -> Callable:
    """Wraps a File sync or async method to propagate caller telemetry."""
    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            with _file_telemetry_context(self, file_method):
                return await func(self, *args, **kwargs)

        return async_wrapper

    @functools.wraps(func)
    def sync_wrapper(self, *args, **kwargs):
        with _file_telemetry_context(self, file_method):
            return func(self, *args, **kwargs)

    return sync_wrapper


def wrap_file_methods(cls: type) -> None:
    """Wrap `__init__` and non-dunder instance methods and properties defined directly on a File class."""
    from gcsfs.core import GCSFile

    is_gcs_file = issubclass(cls, GCSFile)

    orig_init = cls.__dict__.get("__init__", cls.__init__)
    if not getattr(orig_init, "_is_telemetry_init_wrapped", False):

        @functools.wraps(orig_init)
        def init_wrapper(*args, **kwargs):
            self = args[0]
            if not hasattr(self, "caller_method"):
                self.caller_method = get_telemetry_context().get(
                    Dimension.METHOD.value, "open"
                )
            need_scope = is_gcs_file and not has_telemetry_context(Dimension.METHOD)
            was_init = getattr(self, "_initializing_telemetry", False)
            self._initializing_telemetry = True
            try:
                if need_scope:
                    tokens_map = manager.default_usage_tracker.collect_tokens_map(
                        call_scoped_only=True, method=self.caller_method
                    )
                    with _telemetry_context(tokens_map):
                        orig_init(*args, **kwargs)
                else:
                    orig_init(*args, **kwargs)
            finally:
                if not was_init:
                    self.__dict__.pop("_initializing_telemetry", None)

        init_wrapper._is_telemetry_init_wrapped = True
        setattr(cls, "__init__", init_wrapper)
    for name, attr in list(cls.__dict__.items()):
        if name.startswith("__") or isinstance(attr, (classmethod, staticmethod)):
            continue
        is_prop = isinstance(attr, property)
        func = attr.fget if is_prop else (attr if callable(attr) else None)
        if func is not None and not getattr(func, "_is_telemetry_wrapped", False):
            wrapped = _file_telemetry_wrapper(func, _FILE_METHOD_TOKENS.get(name, ""))
            wrapped._is_telemetry_wrapped = True
            setattr(cls, name, attr.getter(wrapped) if is_prop else wrapped)
    if cls is GCSFile and "flush" not in cls.__dict__:
        base_flush = cls.flush

        @functools.wraps(base_flush)
        def _gcs_file_flush(self, force=False, *args, **kwargs):
            if force:
                with _file_telemetry_context(self, "file_flush"):
                    return base_flush(self, force=force, *args, **kwargs)
            return base_flush(self, force=force, *args, **kwargs)

        _gcs_file_flush._is_telemetry_wrapped = True
        cls.flush = _gcs_file_flush
