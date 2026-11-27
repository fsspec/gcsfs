"""Internal thread-safe and coroutine-safe context management for telemetry state."""

from __future__ import annotations

import contextvars
import os
from collections.abc import Mapping
from enum import StrEnum


class Dimension(StrEnum):
    """Standard telemetry dimension keys."""

    FRAMEWORK = "fw"
    ENVIRONMENT = "env"
    METHOD = "m"
    METHOD_PARAMS = "mp"


GRPC_METADATA_KEY = "x-goog-storage-gcsfs-features"
HTTP_HEADER_KEY = "X-Goog-Storage-Gcsfs-Features"


# ContextVar for propagating multi-dimensional telemetry tokens across async event loop and thread boundaries
_current_telemetry: contextvars.ContextVar[dict[str, str] | None] = (
    contextvars.ContextVar("gcsfs_current_telemetry", default=None)
)


if hasattr(os, "register_at_fork"):

    def _reset_telemetry_in_child():
        _current_telemetry.set(None)

    os.register_at_fork(after_in_child=_reset_telemetry_in_child)


def has_telemetry_context(dimension: Dimension | None = None) -> bool:
    """Check if any telemetry context is active, or if `dimension` is present when not `None`."""
    val = _current_telemetry.get()
    if val is None:
        return False
    return True if dimension is None else dimension.value in val


def get_telemetry_context() -> dict[str, str]:
    """Retrieve active telemetry tokens mapping from context."""
    val = _current_telemetry.get()
    return dict(val) if val is not None else {}


def set_telemetry_context(
    tokens: Mapping[str, str] | Dimension,
    value: str | None = None,
) -> contextvars.Token:
    """Set telemetry tokens mapping (or update a single dimension) in context."""
    if isinstance(tokens, Dimension):
        current = get_telemetry_context()
        current[tokens.value] = str(value) if value is not None else ""
        return _current_telemetry.set(current)
    return _current_telemetry.set(dict(tokens))


def reset_telemetry_context(token: contextvars.Token) -> None:
    """Reset telemetry context to the state associated with the given token."""
    _current_telemetry.reset(token)
