"""Central Telemetry Coordinator managing detectors and context propagation."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Dict, List, Optional

from gcsfs.telemetry.context import (
    ALL_DIMENSIONS,
    Dimension,
    get_telemetry_context,
    sanitize_token,
)
from gcsfs.telemetry.detector import BaseDetector, FrameworkDetector


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

    def collect_tokens_map(self) -> Dict[str, str]:
        """
        Collect active telemetry tokens across all registered detectors.

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

    def get_tokens(
        self,
        dimensions: frozenset[Dimension] = ALL_DIMENSIONS,
        tokens_map: Optional[Mapping[str, str]] = None,
    ) -> List[str]:
        """
        Return sanitized formatted telemetry tokens for the requested dimensions.

        Parameters
        ----------
        dimensions:
            Dimensions to include in the returned token list.
        tokens_map:
            Optional pre-collected dimension-to-token mapping to reuse without
            re-running detectors.

        Returns
        -------
        List[str]: List of sanitized formatted token strings, e.g. ['fw/pandas', 'env/gke'].
        """
        if not self.is_enabled():
            return []

        if tokens_map is None:
            tokens_map = self.collect_tokens_map()

        include_custom = dimensions == ALL_DIMENSIONS
        out = []
        for dim_key, val in tokens_map.items():
            if dim_key in dimensions or (
                include_custom and dim_key not in ALL_DIMENSIONS
            ):
                clean_val = sanitize_token(val)
                if clean_val:
                    out.append(f"{dim_key}/{clean_val}")
        return out

    def get_dimension(self, dimension: Dimension) -> Optional[str]:
        """
        Resolve a specific dimension's sanitized bare token value (e.g. Dimension.FRAMEWORK -> 'pandas').
        """
        token_map = self.collect_tokens_map()
        return sanitize_token(token_map.get(dimension.value))

    def encode_features(self) -> Optional[str]:
        """Return web-safe base64 encoded GcsfsFeatures for active telemetry state."""
        tokens_map = self.collect_tokens_map()
        if not tokens_map:
            return None

        from gcsfs.telemetry import schema as telemetry_schema

        return telemetry_schema.encode_features(tokens_map)


# Default singleton instance pre-configured with standard framework detector
default_usage_tracker = UsageMetricsTracker(
    detectors=[
        FrameworkDetector(),
    ]
)
