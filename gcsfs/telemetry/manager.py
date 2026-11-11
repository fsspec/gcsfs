"""Central Telemetry Coordinator managing detectors and context propagation."""

from __future__ import annotations

import os
from typing import Dict, List, Optional

from gcsfs.telemetry.context import Dimension, get_telemetry_context, sanitize_token
from gcsfs.telemetry.detector import (
    BaseDetector,
    EnvironmentDetector,
    FrameworkDetector,
)


class UsageMetricsTracker:
    """Coordinates multi-dimensional detectors and context propagation."""

    def __init__(self, detectors: Optional[List[BaseDetector]] = None):
        self.detectors: List[BaseDetector] = (
            list(detectors) if detectors is not None else []
        )

    def is_enabled(self) -> bool:
        """Check if telemetry collection is enabled globally."""
        return not any(
            os.environ.get(env, "").lower() in ("1", "true", "yes")
            for env in ("DO_NOT_TRACK", "GCSFS_NO_TELEMETRY")
        )

    def warm_up(self) -> None:
        """Start non-call-scoped detectors (e.g. background environment probe)."""
        if not self.is_enabled():
            return
        for detector in self.detectors:
            if getattr(detector, "call_scoped", True):
                continue
            try:
                detector.detect()
            except Exception:
                pass

    def collect_tokens_map(self, *, call_scoped_only: bool = False) -> Dict[str, str]:
        """
        Collect active telemetry tokens across registered detectors.

        Parameters
        ----------
        call_scoped_only : bool, default False
            When True, evaluate only detectors with ``call_scoped = True`` so
            background process dimensions are not frozen in per-call context.

        Returns
        -------
        Dict[str, str]: Mapping of dimension key to bare token value
            (or empty string "" as a negative-cache sentinel when undetected).
        """
        if not self.is_enabled():
            return {}

        tokens = get_telemetry_context()

        # Run registered detectors for any dimensions not already set
        for detector in self.detectors:
            if call_scoped_only and not getattr(detector, "call_scoped", True):
                continue
            dim_key = (
                detector.name.value
                if isinstance(detector.name, Dimension)
                else str(detector.name)
            )
            if dim_key not in tokens:
                try:
                    detected_val = detector.detect()
                    tokens[dim_key] = detected_val or ""
                except Exception:
                    tokens[dim_key] = ""

        return tokens

    def get_tokens(self) -> List[str]:
        """
        Return a list of all active telemetry tokens to be included in HTTP User-Agent.

        Returns
        -------
        List[str]: List of sanitized formatted token strings, e.g. ['fw/pandas', 'env/gke'].
        """
        token_map = self.collect_tokens_map()
        out = []
        for dim_key, val in token_map.items():
            clean_val = sanitize_token(val)
            if clean_val:
                out.append(f"{dim_key}/{clean_val}")
        return out

    def get_dimension(self, dimension: Dimension) -> Optional[str]:
        """
        Resolve a specific dimension's sanitized bare token value (e.g. Dimension.FRAMEWORK -> 'pandas').
        """
        if not self.is_enabled():
            return None

        target_key = dimension.value
        tokens = get_telemetry_context()
        if target_key in tokens:
            return sanitize_token(tokens[target_key])

        for detector in self.detectors:
            dim_key = (
                detector.name.value
                if isinstance(detector.name, Dimension)
                else str(detector.name)
            )
            if dim_key == target_key:
                try:
                    return sanitize_token(detector.detect())
                except Exception:
                    return None

        return None


# Default singleton instance pre-configured with standard detectors
default_usage_tracker = UsageMetricsTracker(
    detectors=[
        FrameworkDetector(),
        EnvironmentDetector(),
    ]
)
