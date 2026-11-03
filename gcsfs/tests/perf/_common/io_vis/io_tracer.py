"""Backward-compatibility shim aliasing io_tracer to gcsfs.iotracer."""

from __future__ import annotations

from gcsfs.iotracer import (
    DEFAULT_BUFFER_SIZE,
    DEFAULT_MAX_EVENTS,
    DEFAULT_MAX_SIZE_MB,
    install_hooks_in_process,
    is_tracing_enabled,
    trace_io,
)
from gcsfs.iotracer.template import _HTML_TEMPLATE
from gcsfs.iotracer.tracer import (
    _LOGGER,
    _ORIGINAL_METHODS,
    _TLS,
    _ProcessEventLogger,
)
from gcsfs.iotracer.visualizer import (
    decimate_events_for_canvas,
    export_chrome_trace,
    render_html_dashboard,
    render_reports,
)

__all__ = [
    "DEFAULT_BUFFER_SIZE",
    "DEFAULT_MAX_EVENTS",
    "DEFAULT_MAX_SIZE_MB",
    "install_hooks_in_process",
    "is_tracing_enabled",
    "trace_io",
    "render_html_dashboard",
    "render_reports",
    "export_chrome_trace",
    "decimate_events_for_canvas",
    "_HTML_TEMPLATE",
    "_LOGGER",
    "_ORIGINAL_METHODS",
    "_TLS",
    "_ProcessEventLogger",
]
