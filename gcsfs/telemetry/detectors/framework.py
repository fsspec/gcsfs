"""Top-to-bottom stack frame detector for upstream caller frameworks (name-only)."""

from __future__ import annotations

import sys
from typing import Dict, Optional

from gcsfs.telemetry.context import Dimension
from gcsfs.telemetry.detectors.base import BaseDetector
from gcsfs.telemetry.sanitizer import sanitize_framework


class FrameworkDetector(BaseDetector):
    """
    Detects the top-most initiating framework name on the call stack.

    Traverses frames from outermost (closest to __main__) down to innermost (gcsfs)
    to attribute high-level orchestrators (e.g., Dask, Ray) over lower-level serialization layers (PyArrow).
    """

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

    @property
    def name(self) -> Dimension:
        return Dimension.FRAMEWORK

    def detect(self) -> Optional[str]:
        if not self.is_enabled():
            return None

        try:
            start_frame = sys._getframe()
        except (ValueError, AttributeError):
            return None

        return self._walk_top_down(start_frame)

    def _walk_top_down(self, start_frame) -> Optional[str]:
        """
        Traverses the frame chain from top (outermost / root) to bottom (innermost / gcsfs).
        Returns the first recognized framework encountered from the top.
        """
        module_names = []
        frame = start_frame
        depth = 0

        while frame is not None and depth < self.max_depth:
            f_globals = getattr(frame, "f_globals", None) or {}
            mod_name = f_globals.get("__name__", "")
            if mod_name.startswith("gcsfs"):
                # If any gcsfs frame has a file instance with cached _caller_framework, use it immediately
                try:
                    f_locals = getattr(frame, "f_locals", None)
                    if f_locals is not None:
                        instance = f_locals.get("self")
                        cached_fw = getattr(instance, "_caller_framework", None)
                        if isinstance(cached_fw, str) and cached_fw:
                            return cached_fw
                except Exception:
                    pass

            if mod_name:
                module_names.append(mod_name)
            frame = frame.f_back
            depth += 1

        # Scan in call order (outermost root -> innermost) to match the initiating framework first
        for mod_name in reversed(module_names):
            top_pkg = mod_name.partition(".")[0]
            if top_pkg in self.KNOWN_FRAMEWORKS:
                framework_name = self.KNOWN_FRAMEWORKS[top_pkg]
                clean_name = sanitize_framework(framework_name)
                if clean_name:
                    return f"fw/{clean_name}"

        return None
