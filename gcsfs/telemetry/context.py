"""Internal thread-safe and coroutine-safe context management for telemetry state."""

from __future__ import annotations

import contextvars
import os
from enum import Enum
from typing import Dict, Optional


class Dimension(str, Enum):
    """Standard telemetry dimension keys."""

    FRAMEWORK = "fw"


# ContextVar for propagating multi-dimensional telemetry tokens across async event loop and thread boundaries
_current_telemetry: contextvars.ContextVar[Dict[str, str]] = contextvars.ContextVar(
    "gcsfs_current_telemetry", default={}
)


if hasattr(os, "register_at_fork"):

    def _reset_telemetry_in_child():
        _current_telemetry.set({})

    os.register_at_fork(after_in_child=_reset_telemetry_in_child)


def get_telemetry_context() -> Dict[str, str]:
    """Retrieve active telemetry tokens mapping from context."""
    return dict(_current_telemetry.get())


def set_telemetry_context(tokens: Dict[str, str]) -> contextvars.Token:
    """Set telemetry tokens mapping in context and return contextvars.Token for resetting."""
    return _current_telemetry.set(dict(tokens))


def reset_telemetry_context(token: contextvars.Token) -> None:
    """Reset telemetry context to the state associated with the given token."""
    _current_telemetry.reset(token)


def get_dimension_context(dimension: Dimension) -> Optional[str]:
    """Retrieve active telemetry token for a specific dimension from context."""
    return get_telemetry_context().get(dimension.value)


def set_dimension_context(dimension: Dimension, value: str) -> contextvars.Token:
    """Set or update a specific dimension in context and return contextvars.Token for resetting."""
    current = get_telemetry_context()
    current[dimension.value] = str(value)
    return set_telemetry_context(current)
