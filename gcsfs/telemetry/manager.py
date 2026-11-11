"""Central Telemetry Coordinator managing detectors and context propagation."""

from __future__ import annotations

import os

from gcsfs.telemetry.context import (
    ALL_DIMENSIONS,
    Dimension,
    get_telemetry_context,
    sanitize_token,
)
from gcsfs.telemetry.detector import BaseDetector, FrameworkDetector


class UsageMetricsTracker:
    """Coordinates multi-dimensional detectors and context propagation."""

    def __init__(self, detectors: list[BaseDetector] | None = None):
        self.detectors: list[BaseDetector] = (
            list(detectors) if detectors is not None else []
        )

    def is_enabled(self) -> bool:
        """Check if telemetry collection is enabled globally."""
        return not any(
            os.environ.get(env, "").strip().lower() in ("1", "true", "yes")
            for env in ("DO_NOT_TRACK", "GCSFS_NO_TELEMETRY")
        )

    def collect_tokens_map(
        self,
        dimensions: frozenset[Dimension] = ALL_DIMENSIONS,
        *,
        method: str | None = None,
    ) -> dict[str, str]:
        """
        Collect active telemetry tokens across registered detectors.

        Parameters
        ----------
        dimensions : frozenset[Dimension], optional
            Dimensions to collect and return. Defaults to ALL_DIMENSIONS.
        method : str | None, optional
            Top-level public method name to attach under Dimension.METHOD
            (or "" as a negative-cache sentinel for untracked entry points).

        Returns
        -------
        dict[str, str]
            Mapping of dimension key to bare token value
            (or empty string "" as a negative-cache sentinel when undetected).
        """
        if not self.is_enabled():
            return {}

        tokens = get_telemetry_context()
        if method is not None and Dimension.METHOD in dimensions:
            tokens.setdefault(Dimension.METHOD.value, method)

        include_custom = dimensions == ALL_DIMENSIONS

        # Run registered detectors for any requested dimensions not already set
        for detector in self.detectors:
            dim_key = (
                detector.name.value
                if isinstance(detector.name, Dimension)
                else str(detector.name)
            )
            if dim_key in ALL_DIMENSIONS:
                if dim_key not in dimensions:
                    continue
            elif not include_custom:
                continue
            if dim_key not in tokens:
                try:
                    detected_val = detector.detect()
                    tokens[dim_key] = detected_val or ""
                except Exception:
                    tokens[dim_key] = ""

        return {
            k: v
            for k, v in tokens.items()
            if k in dimensions or (include_custom and k not in ALL_DIMENSIONS)
        }

    def get_tokens(
        self,
        dimensions: frozenset[Dimension] = ALL_DIMENSIONS,
    ) -> list[str]:
        """
        Return formatted `<dim>/<val>` telemetry tokens for `dimensions`.

        Parameters
        ----------
        dimensions : frozenset[Dimension], optional
            Dimensions to format and return. Defaults to ALL_DIMENSIONS.

        Returns
        -------
        list[str]
            List of sanitized formatted token strings, e.g. ['fw/pandas', 'env/gke'].
        """
        token_map = self.collect_tokens_map(dimensions=dimensions)
        out = []
        for dim_key, val in token_map.items():
            clean_val = sanitize_token(val)
            if clean_val:
                out.append(f"{dim_key}/{clean_val}")
        return out

    def get_dimension(self, dimension: Dimension) -> str | None:
        """
        Resolve a specific dimension's sanitized bare token value (e.g. Dimension.FRAMEWORK -> 'pandas').
        """
        token_map = self.collect_tokens_map(dimensions=frozenset({dimension}))
        return sanitize_token(token_map.get(dimension.value))


# Default singleton instance pre-configured with standard framework detector
default_usage_tracker = UsageMetricsTracker(
    detectors=[
        FrameworkDetector(),
    ]
)
