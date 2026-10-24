"""Telemetry detectors for extracting caller context tokens."""

from __future__ import annotations

import sys
from typing import Dict, Optional

from gcsfs.telemetry.context import Dimension


class BaseDetector:
    """Base interface for extracting telemetry tokens."""

    name: Dimension | str = ""

    def detect(self) -> Optional[str]:
        """
        Execute detection logic and return a bare telemetry token value.

        Returns
        -------
        Optional[str]: Bare token value (e.g. 'pandas') or None if not detected.
        """
        raise NotImplementedError


class FrameworkDetector(BaseDetector):
    """
    Detects the top-most initiating framework name on the call stack.

    Traverses frames from innermost (gcsfs) to outermost (closest to __main__),
    retaining the outermost match to attribute high-level orchestrators (e.g., Dask, Ray)
    over lower-level serialization layers (PyArrow).
    """

    name = Dimension.FRAMEWORK

    # Canonical mapping of top-level package name -> standard brand token
    KNOWN_FRAMEWORKS: Dict[str, str] = {
        # Data & Analytics
        "pandas": "pandas",
        "duckdb": "duckdb",
        "pyspark": "spark",
        "dask": "dask",
        "ray": "ray",
        "pyarrow": "pyarrow",
        "xarray": "xarray",
        "fastparquet": "fastparquet",
        "polars": "polars",
        # ML & Deep Learning
        "torch": "torch",
        "torchdata": "torchdata",
        "lightning": "lightning",
        "pytorch_lightning": "lightning",
        "datasets": "datasets",
        "transformers": "transformers",
        "orbax": "orbax",
        "tensorstore": "tensorstore",
        "tensorflow": "tensorflow",
        "jax": "jax",
    }

    def __init__(self, max_depth: int = 64):
        self.max_depth = max_depth

    def detect(self) -> Optional[str]:
        try:
            start_frame = sys._getframe()
        except (ValueError, AttributeError, RuntimeError, PermissionError):
            return None

        try:
            return self._walk_stack(start_frame)
        finally:
            del start_frame

    def _walk_stack(self, start_frame) -> Optional[str]:
        """
        Traverses the frame chain in a single pass from innermost (gcsfs) to outermost (root).
        Returns the outermost recognized framework encountered.
        """
        frame = start_frame
        depth = 0
        detected_framework = None

        try:
            while frame is not None and depth < self.max_depth:
                f_globals = getattr(frame, "f_globals", None) or {}
                mod_name = f_globals.get("__name__", "")
                if mod_name:
                    top_pkg = mod_name.partition(".")[0]
                    if top_pkg in self.KNOWN_FRAMEWORKS:
                        detected_framework = self.KNOWN_FRAMEWORKS[top_pkg]
                frame = frame.f_back
                depth += 1
        finally:
            del frame
            del start_frame

        return detected_framework
