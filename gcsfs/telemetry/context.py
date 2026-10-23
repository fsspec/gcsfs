"""Internal thread-safe and coroutine-safe context management for telemetry state."""

from __future__ import annotations

import contextvars
import os
import re
from enum import Enum
from typing import Dict, Optional, Union

# Allowed characters in RFC 9110 product-version tokens: ASCII alphanumeric, '_', '.', '-'
_TOKEN_SANITIZE_REGEX = re.compile(r"[^a-zA-Z0-9_.-]")


class Dimension(str, Enum):
    """Standard telemetry dimension keys."""

    FRAMEWORK = "fw"


def sanitize_token(token: Optional[str], max_len: int = 64) -> Optional[str]:
    """Sanitize a bare token value to comply with RFC 9110 product-version grammar."""
    if not token or not isinstance(token, str):
        return None

    clean = _TOKEN_SANITIZE_REGEX.sub("_", token.strip())[:max_len]
    return clean if clean.strip("_.-") else None


# ContextVar for propagating multi-dimensional telemetry tokens across async event loop and thread boundaries
_current_telemetry: contextvars.ContextVar[Optional[Dict[str, str]]] = (
    contextvars.ContextVar("gcsfs_current_telemetry", default=None)
)


if hasattr(os, "register_at_fork"):

    def _reset_telemetry_in_child():
        _current_telemetry.set(None)

    os.register_at_fork(after_in_child=_reset_telemetry_in_child)


def has_telemetry_context() -> bool:
    """Check if a telemetry context is currently active without copying the dict."""
    return _current_telemetry.get() is not None


def get_telemetry_context() -> Dict[str, str]:
    """Retrieve active telemetry tokens mapping from context."""
    val = _current_telemetry.get()
    return dict(val) if val is not None else {}


def set_telemetry_context(
    tokens: Union[Dict[str, str], Dimension], value: Optional[str] = None
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
