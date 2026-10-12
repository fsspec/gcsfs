"""Base class for telemetry detectors following gcsfs/fsspec conventions."""

from __future__ import annotations

from typing import Optional

from gcsfs.telemetry.context import Dimension


class BaseDetector:
    """Base interface for extracting telemetry tokens."""

    name: Dimension | str = ""

    def detect(self) -> Optional[str]:
        """
        Execute detection logic and return an RFC 9110 compliant telemetry token.

        Returns
        -------
        Optional[str]: Formatted token string (e.g. 'fw/pandas') or None if not detected.
        """
        raise NotImplementedError
