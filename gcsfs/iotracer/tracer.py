"""Core Runtime Interception & Logging Engine for gcsfs.iotracer."""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import tempfile
import threading
import time
import warnings
from pathlib import Path
from typing import Any, Generator

DEFAULT_MAX_EVENTS: int = 500_000
DEFAULT_MAX_SIZE_MB: int = 100
DEFAULT_BUFFER_SIZE: int = 65_536

_TLS = threading.local()
_hooks_installed = False
_ORIGINAL_METHODS: dict[str, Any] = {}
_FILE_WIRE_COUNTS: dict[str, int] = {}
_GLOBAL_WIRE_COUNT = 0


def is_tracing_enabled() -> bool:
    """Check if an active trace directory or environment flag is present."""
    return bool(os.environ.get("GCSFS_IO_TRACE_DIR") or os.environ.get("GCSFS_IO_TRACE"))


def _normalize_path(path: Any) -> str:
    if not isinstance(path, str):
        path = str(path)
    for prefix in ("gs://", "gcs://"):
        if path.startswith(prefix):
            path = path[len(prefix) :]
    return path


class _ProcessEventLogger:
    """Fork-safe, per-PID JSONL event writer with sequential cache-hit coalescing."""

    def __init__(self) -> None:
        self._pid: int | None = None
        self._fp: Any = None
        self._trace_dir: str | None = None
        self._t0: float = 0.0
        self._lock = threading.Lock()
        self._pending_hit: dict[tuple[int, str], dict[str, Any]] = {}
        self._event_count: int = 0
        self._byte_count: int = 0
        self._truncated: bool = False
        self._max_events: int = DEFAULT_MAX_EVENTS
        self._max_size_bytes: int = DEFAULT_MAX_SIZE_MB * 1024 * 1024
        self._logical_unflushed: int = 0

    def configure(self, max_events: int, max_size_mb: int) -> None:
        with self._lock:
            self._max_events = max_events
            self._max_size_bytes = max_size_mb * 1024 * 1024

    def _ensure_open(self) -> bool:
        trace_dir = os.environ.get("GCSFS_IO_TRACE_DIR")
        if not trace_dir:
            return False
        pid = os.getpid()
        if self._pid != pid or self._fp is None or self._trace_dir != trace_dir:
            if self._fp is not None:
                with contextlib.suppress(Exception):
                    if self._pid == pid:
                        self._flush_pending_locked()
                        self._fp.close()
            self._pending_hit.clear()
            self._trace_dir = trace_dir
            self._pid = pid
            self._event_count = 0
            self._byte_count = 0
            self._truncated = False
            self._logical_unflushed = 0
            try:
                self._t0 = float(os.environ.get("GCSFS_IO_TRACE_T0") or time.monotonic())
            except ValueError:
                self._t0 = time.monotonic()
            os.makedirs(trace_dir, exist_ok=True)
            log_path = os.path.join(trace_dir, f"events_{pid}_{id(self)}.jsonl")
            self._fp = open(log_path, "a", buffering=DEFAULT_BUFFER_SIZE, encoding="utf-8")
        return True

    def _write_rec_locked(self, rec: dict[str, Any]) -> None:
        if self._truncated:
            return
        if self._event_count >= self._max_events or self._byte_count >= self._max_size_bytes:
            self._truncated = True
            sentinel = {
                "t": round(time.monotonic() - self._t0, 6),
                "dur": 0.0,
                "layer": "control",
                "op": "trace_truncated",
                "path": "system",
                "off": 0,
                "len": 0,
                "pid": self._pid,
                "tid": threading.get_ident(),
                "reason": "max_events" if self._event_count >= self._max_events else "max_size",
            }
            try:
                line = json.dumps(sentinel, separators=(",", ":")) + "\n"
                self._fp.write(line)
                self._fp.flush()
            except Exception:
                pass
            warnings.warn(
                f"[gcsfs.iotracer] Trace limit reached for PID {self._pid}; logging suspended."
            )
            return

        try:
            line = json.dumps(rec, separators=(",", ":")) + "\n"
            self._fp.write(line)
            self._event_count += 1
            self._byte_count += len(line.encode("utf-8"))
        except Exception:
            pass

    def _flush_pending_locked(self, key: tuple[int, str] | None = None) -> None:
        if key is not None:
            prev = self._pending_hit.pop(key, None)
            if prev is not None and self._fp is not None:
                self._write_rec_locked(prev)
            return
        if self._fp is not None:
            for prev in self._pending_hit.values():
                self._write_rec_locked(prev)
        self._pending_hit.clear()

    def log(
        self,
        *,
        layer: str,
        op: str,
        file: str,
        offset: int,
        length: int,
        t_start: float,
        t_end: float,
        hit_state: str = "miss",
        cached_len: int = 0,
        wire_origin: str | None = None,
        transport: str | None = None,
        req_id: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            if not self._ensure_open():
                return
            tid = threading.get_ident()
            norm_file = _normalize_path(file)
            ts = round(t_start - self._t0, 6)
            dur = max(0.0, round(t_end - t_start, 6))
            off = int(offset or 0)
            ln = int(length or 0)

            key = (tid, norm_file)
            # Coalesce contiguous sequential cache-hit reads on the same thread & file within 2 ms
            if layer == "logical" and hit_state == "full" and ln < 262144:
                prev = self._pending_hit.get(key)
                if (
                    prev is not None
                    and prev["off"] + prev["len"] == off
                    and (ts - (prev["t"] + prev["dur"])) <= 0.002
                ):
                    prev["len"] += ln
                    new_te = ts + dur
                    prev["dur"] = max(prev["dur"], round(new_te - prev["t"], 6))
                    prev["cnt"] = prev.get("cnt", 1) + 1
                    return
                self._flush_pending_locked(key)

            rec: dict[str, Any] = {
                "t": ts,
                "dur": dur,
                "layer": layer,
                "op": op,
                "path": norm_file,
                "off": off,
                "len": ln,
                "pid": self._pid,
                "tid": tid,
            }
            if layer == "logical":
                rec["hit_state"] = hit_state
                rec["cached_len"] = cached_len
            if wire_origin:
                rec["wire_origin"] = wire_origin
            if transport:
                rec["transport"] = transport
            if req_id:
                rec["req_id"] = req_id
            if extra:
                rec.update(extra)

            if layer == "logical" and hit_state == "full" and ln < 262144:
                self._pending_hit[key] = rec
            else:
                if layer == "logical":
                    self._flush_pending_locked(key)
                self._write_rec_locked(rec)

                # Durability flush triggers
                if layer == "wire":
                    with contextlib.suppress(Exception):
                        self._fp.flush()
                elif layer == "logical":
                    self._logical_unflushed += 1
                    if self._logical_unflushed >= 1000:
                        self._logical_unflushed = 0
                        with contextlib.suppress(Exception):
                            self._fp.flush()

    def flush(self) -> None:
        with self._lock:
            if self._fp is not None and self._pid == os.getpid():
                with contextlib.suppress(Exception):
                    self._flush_pending_locked()
                    self._fp.flush()

    def close(self) -> None:
        with self._lock:
            if self._fp is not None:
                with contextlib.suppress(Exception):
                    if self._pid == os.getpid():
                        self._flush_pending_locked()
                    self._fp.close()
                self._fp = None


_LOGGER = _ProcessEventLogger()


def _inc_wire(file_path: str) -> None:
    global _GLOBAL_WIRE_COUNT
    _GLOBAL_WIRE_COUNT += 1
    _TLS.wire_count = getattr(_TLS, "wire_count", 0) + 1
    norm = _normalize_path(file_path)
    _FILE_WIRE_COUNTS[norm] = _FILE_WIRE_COUNTS.get(norm, 0) + 1


def install_hooks_in_process() -> None:
    """Install interception monkey-patches on fsspec and gcsfs."""
    global _hooks_installed
    if _hooks_installed:
        return
    _hooks_installed = True

    import atexit
    atexit.register(_LOGGER.flush)

    # Re-initialize logger in forked child processes
    if hasattr(os, "register_at_fork"):
        os.register_at_fork(
            after_in_child=lambda: setattr(_LOGGER, "_fp", None) or setattr(_LOGGER, "_pid", None)
        )

    from fsspec.spec import AbstractBufferedFile

    # 1. Hook AbstractBufferedFile.read (Logical layer)
    orig_abf_read = AbstractBufferedFile.read
    _ORIGINAL_METHODS["abf_read"] = orig_abf_read

    def traced_abf_read(self: Any, length: int = -1) -> bytes:
        if not os.environ.get("GCSFS_IO_TRACE_DIR"):
            return orig_abf_read(self, length)

        if getattr(_TLS, "in_logical_read", False):
            return orig_abf_read(self, length)

        _TLS.in_logical_read = True
        loc_before = getattr(self, "loc", 0)
        file_path = getattr(self, "path", "unknown")
        norm_path = _normalize_path(file_path)
        wire_before = _FILE_WIRE_COUNTS.get(norm_path, 0)
        t0 = time.monotonic()
        try:
            out = orig_abf_read(self, length)
        finally:
            t1 = time.monotonic()
            _TLS.in_logical_read = False

        wire_after = _FILE_WIRE_COUNTS.get(norm_path, 0)
        actual_len = len(out) if out is not None else 0
        if actual_len > 0 or length > 0:
            is_hit = (wire_after == wire_before)
            _LOGGER.log(
                layer="logical",
                op="read",
                file=file_path,
                offset=loc_before,
                length=actual_len,
                t_start=t0,
                t_end=t1,
                hit_state="full" if is_hit else "miss",
                cached_len=actual_len if is_hit else 0,
            )
        return out

    AbstractBufferedFile.read = traced_abf_read

    # 2. Hook AbstractBufferedFile.readinto (Logical layer)
    if hasattr(AbstractBufferedFile, "readinto"):
        orig_abf_readinto = AbstractBufferedFile.readinto
        _ORIGINAL_METHODS["abf_readinto"] = orig_abf_readinto

        def traced_abf_readinto(self: Any, b: Any) -> int:
            if not os.environ.get("GCSFS_IO_TRACE_DIR") or getattr(_TLS, "in_logical_read", False):
                return orig_abf_readinto(self, b)

            _TLS.in_logical_read = True
            loc_before = getattr(self, "loc", 0)
            file_path = getattr(self, "path", "unknown")
            norm_path = _normalize_path(file_path)
            wire_before = _FILE_WIRE_COUNTS.get(norm_path, 0)
            t0 = time.monotonic()
            try:
                n = orig_abf_readinto(self, b)
            finally:
                t1 = time.monotonic()
                _TLS.in_logical_read = False

            wire_after = _FILE_WIRE_COUNTS.get(norm_path, 0)
            actual_len = n or 0
            if actual_len > 0:
                is_hit = (wire_after == wire_before)
                _LOGGER.log(
                    layer="logical",
                    op="readinto",
                    file=file_path,
                    offset=loc_before,
                    length=actual_len,
                    t_start=t0,
                    t_end=t1,
                    hit_state="full" if is_hit else "miss",
                    cached_len=actual_len if is_hit else 0,
                )
            return n

        AbstractBufferedFile.readinto = traced_abf_readinto

    # 3. Hook AbstractBufferedFile.readline (Logical layer)
    if hasattr(AbstractBufferedFile, "readline"):
        orig_abf_readline = AbstractBufferedFile.readline
        _ORIGINAL_METHODS["abf_readline"] = orig_abf_readline

        def traced_abf_readline(self: Any, *args: Any, **kwargs: Any) -> bytes:
            if not os.environ.get("GCSFS_IO_TRACE_DIR") or getattr(_TLS, "in_logical_read", False):
                return orig_abf_readline(self, *args, **kwargs)

            _TLS.in_logical_read = True
            loc_before = getattr(self, "loc", 0)
            file_path = getattr(self, "path", "unknown")
            norm_path = _normalize_path(file_path)
            wire_before = _FILE_WIRE_COUNTS.get(norm_path, 0)
            t0 = time.monotonic()
            try:
                line = orig_abf_readline(self, *args, **kwargs)
            finally:
                t1 = time.monotonic()
                _TLS.in_logical_read = False

            wire_after = _FILE_WIRE_COUNTS.get(norm_path, 0)
            actual_len = len(line) if line is not None else 0
            if actual_len > 0:
                is_hit = (wire_after == wire_before)
                _LOGGER.log(
                    layer="logical",
                    op="readline",
                    file=file_path,
                    offset=loc_before,
                    length=actual_len,
                    t_start=t0,
                    t_end=t1,
                    hit_state="full" if is_hit else "miss",
                    cached_len=actual_len if is_hit else 0,
                )
            return line

        AbstractBufferedFile.readline = traced_abf_readline

    # 4. Hook close & flush
    orig_abf_close = AbstractBufferedFile.close
    _ORIGINAL_METHODS["abf_close"] = orig_abf_close

    def traced_abf_close(self: Any) -> None:
        try:
            orig_abf_close(self)
        finally:
            if os.environ.get("GCSFS_IO_TRACE_DIR"):
                _LOGGER.flush()

    AbstractBufferedFile.close = traced_abf_close

    # 5. Hook AbstractBufferedFile._fetch_range
    orig_abf_fetch = AbstractBufferedFile._fetch_range
    _ORIGINAL_METHODS["abf_fetch"] = orig_abf_fetch

    def _wrap_fetch_range(orig_fn: Any) -> Any:
        def traced_fetch_range(
            self: Any,
            start: int | None = None,
            end: int | None = None,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            if not os.environ.get("GCSFS_IO_TRACE_DIR"):
                return orig_fn(self, start, end, *args, **kwargs)

            file_path = getattr(self, "path", "unknown")
            _inc_wire(file_path)
            depth = getattr(_TLS, "in_fetch_range", 0)
            _TLS.in_fetch_range = depth + 1
            t0 = time.monotonic()
            try:
                out = orig_fn(self, start, end, *args, **kwargs)
            finally:
                t1 = time.monotonic()
                _TLS.in_fetch_range = depth

            if depth == 0 and not getattr(_TLS, "logged_by_cat_file", False):
                s = int(start or 0)
                actual_len = (
                    len(out)
                    if isinstance(out, (bytes, bytearray, memoryview))
                    else int((end or s) - s)
                )
                _LOGGER.log(
                    layer="wire",
                    op="fetch_range",
                    file=file_path,
                    offset=s,
                    length=actual_len,
                    t_start=t0,
                    t_end=t1,
                    transport="http",
                    wire_origin="direct_miss",
                )
            _TLS.logged_by_cat_file = False
            return out

        return traced_fetch_range

    AbstractBufferedFile._fetch_range = _wrap_fetch_range(orig_abf_fetch)

    # 6. Hook GCSFileSystem & GCSFile
    with contextlib.suppress(ImportError):
        import gcsfs.core

        if hasattr(gcsfs.core.GCSFile, "_fetch_range"):
            orig_gcs_fetch = gcsfs.core.GCSFile._fetch_range
            _ORIGINAL_METHODS["gcs_fetch"] = orig_gcs_fetch
            gcsfs.core.GCSFile._fetch_range = _wrap_fetch_range(orig_gcs_fetch)

        orig_cat_seq = gcsfs.core.GCSFileSystem._cat_file_sequential
        _ORIGINAL_METHODS["gcs_cat_seq"] = orig_cat_seq

        async def traced_cat_file_sequential(
            self: Any,
            path: str,
            start: int | None = None,
            end: int | None = None,
            **kwargs: Any,
        ) -> bytes:
            if not os.environ.get("GCSFS_IO_TRACE_DIR"):
                return await orig_cat_seq(self, path, start=start, end=end, **kwargs)
            _inc_wire(path)
            t0 = time.monotonic()
            out = await orig_cat_seq(self, path, start=start, end=end, **kwargs)
            t1 = time.monotonic()
            _TLS.logged_by_cat_file = True
            s = int(start or 0)
            actual_len = len(out) if out is not None else int((end or s) - s)
            _LOGGER.log(
                layer="wire",
                op="http_range_get",
                file=path,
                offset=s,
                length=actual_len,
                t_start=t0,
                t_end=t1,
                transport="http",
                wire_origin="direct_miss",
            )
            return out

        gcsfs.core.GCSFileSystem._cat_file_sequential = traced_cat_file_sequential

    # 7. Hook ZonalFile._fetch_range (gRPC transfers)
    with contextlib.suppress(ImportError):
        import gcsfs.zonal_file

        if hasattr(gcsfs.zonal_file.ZonalFile, "_fetch_range"):
            orig_zonal_fetch = gcsfs.zonal_file.ZonalFile._fetch_range
            _ORIGINAL_METHODS["zonal_fetch"] = orig_zonal_fetch

            def traced_zonal_fetch_range(
                self: Any,
                start: int | None = None,
                end: int | None = None,
                chunk_lengths: list[int] | None = None,
            ) -> Any:
                if not os.environ.get("GCSFS_IO_TRACE_DIR"):
                    return orig_zonal_fetch(self, start=start, end=end, chunk_lengths=chunk_lengths)

                file_path = getattr(self, "path", "unknown")
                _inc_wire(file_path)
                t0 = time.monotonic()
                try:
                    out = orig_zonal_fetch(self, start=start, end=end, chunk_lengths=chunk_lengths)
                finally:
                    t1 = time.monotonic()

                s = int(start or 0)
                if chunk_lengths is not None:
                    actual_len = sum(chunk_lengths)
                elif isinstance(out, (bytes, bytearray, memoryview)):
                    actual_len = len(out)
                else:
                    actual_len = int((end or s) - s)

                _LOGGER.log(
                    layer="wire",
                    op="grpc_fetch_range",
                    file=file_path,
                    offset=s,
                    length=actual_len,
                    t_start=t0,
                    t_end=t1,
                    transport="grpc",
                    wire_origin="direct_miss",
                )
                return out

            gcsfs.zonal_file.ZonalFile._fetch_range = traced_zonal_fetch_range


@contextlib.contextmanager
def trace_io(
    output_html: str | Path | None = "io_trace.html",
    export_trace: str | Path | None = None,
    trace_dir: str | Path | None = None,
    max_events: int = DEFAULT_MAX_EVENTS,
    max_size_mb: int = DEFAULT_MAX_SIZE_MB,
    title: str = "Workload I/O Pattern Trace",
) -> Generator[Path, None, None]:
    """Context manager to trace logical and wire I/O and render reports."""
    from .visualizer import render_reports

    cleanup_dir = False
    if trace_dir is None:
        target_base = Path("/tmp")
        try:
            usage = shutil.disk_usage(target_base)
            if usage.free < 2 * 1024 * 1024 * 1024:
                target_base = Path("./.gcsfs_traces")
                target_base.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        run_dir = tempfile.mkdtemp(prefix="gcsfs_iotrace_", dir=str(target_base))
        cleanup_dir = True
    else:
        run_dir = str(trace_dir)
        os.makedirs(run_dir, exist_ok=True)

    _LOGGER.configure(max_events=max_events, max_size_mb=max_size_mb)

    old_env = {
        k: os.environ.get(k)
        for k in (
            "GCSFS_IO_TRACE",
            "GCSFS_IO_TRACE_DIR",
            "GCSFS_IO_TRACE_T0",
        )
    }

    t0 = time.monotonic()
    os.environ["GCSFS_IO_TRACE"] = "1"
    os.environ["GCSFS_IO_TRACE_DIR"] = run_dir
    os.environ["GCSFS_IO_TRACE_T0"] = f"{t0:.6f}"

    install_hooks_in_process()

    try:
        yield Path(run_dir)
    finally:
        _LOGGER.flush()
        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

        render_reports(
            run_dir,
            output_html=output_html,
            export_trace=export_trace,
            title=title,
            main_pid=os.getpid(),
        )
        if cleanup_dir:
            shutil.rmtree(run_dir, ignore_errors=True)
