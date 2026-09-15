"""Base class for telemetry detectors following gcsfs/fsspec conventions."""

from __future__ import annotations

import os
from typing import Optional

from gcsfs.telemetry.context import Dimension


class BaseDetector:
    """Base interface for extracting telemetry tokens."""

    @property
    def name(self) -> Dimension | str:
        """Unique telemetry dimension identifier for this detector."""
        raise NotImplementedError

    def detect(self) -> Optional[str]:
        """
        Execute detection logic and return an RFC 9110 compliant telemetry token.

        Returns
        -------
        Optional[str]: Formatted token string (e.g. 'fw/pandas') or None if not detected.
        """
        raise NotImplementedError

    def is_enabled(self) -> bool:
        """Check if telemetry opt-out environment variable is active."""
        return os.environ.get("GCSFS_NO_TELEMETRY", "").lower() not in (
            "1",
            "true",
            "yes",
        )
