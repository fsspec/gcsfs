"""Internal thread-safe and coroutine-safe context management for telemetry state."""

from __future__ import annotations

import contextvars
import itertools
import os
import re
from collections.abc import Mapping
from enum import StrEnum

# Allowed characters in RFC 9110 product-version tokens: ASCII alphanumeric, '_', '.', '-'
_TOKEN_SANITIZE_REGEX = re.compile(r"[^a-zA-Z0-9_.-]")

TRACKED_METHOD_PARAMS = frozenset(
    {
        "cache_options",
        "detail",
        "prefix",
        "recursive",
        "withdirs",
    }
)
_SORTED_TRACKED_METHOD_PARAMS = tuple(sorted(TRACKED_METHOD_PARAMS))
_CANONICAL_METHOD_PARAMS = frozenset(
    ".".join(combo)
    for r in range(1, len(_SORTED_TRACKED_METHOD_PARAMS) + 1)
    for combo in itertools.combinations(_SORTED_TRACKED_METHOD_PARAMS, r)
)


class Dimension(StrEnum):
    """Standard telemetry dimension keys."""

    FRAMEWORK = "fw"
    ENVIRONMENT = "env"
    METHOD = "m"
    METHOD_PARAMS = "mp"


GRPC_METADATA_KEY = "x-goog-storage-gcsfs-features"
HTTP_HEADER_KEY = "X-Goog-Storage-Gcsfs-Features"
USER_AGENT_DIMENSIONS: frozenset[Dimension] = frozenset(
    {Dimension.ENVIRONMENT, Dimension.FRAMEWORK}
)


def canonicalize_method_params(raw: str | None) -> str:
    """Return `raw` if it is a canonical sorted `.`-joined flag string, else `""`."""
    return raw if isinstance(raw, str) and raw in _CANONICAL_METHOD_PARAMS else ""


def sanitize_token(token: str | None, max_len: int = 64) -> str | None:
    """Sanitize a bare token value to comply with RFC 9110 product-version grammar."""
    if not token or not isinstance(token, str):
        return None

    clean = _TOKEN_SANITIZE_REGEX.sub("_", token.strip())[:max_len]
    return clean if clean.strip("_.-") else None


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
        str_val = str(value) if value is not None else ""
        if tokens is Dimension.METHOD_PARAMS:
            str_val = canonicalize_method_params(str_val)
            if str_val and current.get(Dimension.METHOD.value):
                current[tokens.value] = str_val
            else:
                current.pop(tokens.value, None)
        else:
            current[tokens.value] = str_val
            if tokens is Dimension.METHOD and not str_val:
                current.pop(Dimension.METHOD_PARAMS.value, None)
        return _current_telemetry.set(current)
    normalized = dict(tokens)
    mp = normalized.get(Dimension.METHOD_PARAMS.value)
    if mp is not None:
        canon = canonicalize_method_params(mp)
        if canon and normalized.get(Dimension.METHOD.value):
            normalized[Dimension.METHOD_PARAMS.value] = canon
        else:
            normalized.pop(Dimension.METHOD_PARAMS.value, None)
    return _current_telemetry.set(normalized)


def reset_telemetry_context(token: contextvars.Token) -> None:
    """Reset telemetry context to the state associated with the given token."""
    _current_telemetry.reset(token)
