"""RFC 9110 compliant token sanitization helper."""
from __future__ import annotations

import re
from typing import Optional

# Allowed characters in RFC 9110 product-version tokens: ASCII alphanumeric, '_', '.', '-'
# Disallows delimiters including '/', ':', and whitespace.
_TOKEN_SANITIZE_REGEX = re.compile(r"[^a-zA-Z0-9_.-]")


def sanitize_framework(value: Optional[str], max_len: int = 64) -> str:
    """
    Sanitize framework string to comply with RFC 9110 token grammar and strip CRLF / control chars.

    Parameters
    ----------
    value: Optional[str]
        Raw framework string to sanitize.
    max_len: int, default 64
        Maximum allowed length.

    Returns
    -------
    str: Cleaned, ASCII-safe framework string. Returns empty string if input is None or empty.
    """
    if not value or not isinstance(value, str):
        return ""

    # Strip leading/trailing whitespace and control chars (including \r, \n)
    cleaned = value.strip()
    if not cleaned:
        return ""

    # Replace forbidden characters with '_' and truncate
    sanitized = _TOKEN_SANITIZE_REGEX.sub("_", cleaned)
    return sanitized[:max_len]
