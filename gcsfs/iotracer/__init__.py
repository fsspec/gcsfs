"""Public API for GCSFS I/O Tracer (gcsfs.iotracer)."""

from __future__ import annotations

from .tracer import (
    DEFAULT_BUFFER_SIZE,
    DEFAULT_MAX_EVENTS,
    DEFAULT_MAX_SIZE_MB,
    install_hooks_in_process,
    is_tracing_enabled,
    trace_io,
)

__all__ = [
    "DEFAULT_BUFFER_SIZE",
    "DEFAULT_MAX_EVENTS",
    "DEFAULT_MAX_SIZE_MB",
    "install_hooks_in_process",
    "is_tracing_enabled",
    "trace_io",
]
