"""Central Telemetry Coordinator managing detectors and context propagation."""

from __future__ import annotations

import os

from gcsfs.telemetry.context import Dimension, get_telemetry_context
from gcsfs.telemetry.detector import (
    BaseDetector,
    EnvironmentDetector,
    FrameworkDetector,
)


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

    def collect_tokens_map(
        self,
        dimensions: frozenset[Dimension | str] | None = None,
        *,
        call_scoped_only: bool,
        method: str | None = None,
    ) -> dict[str, str]:
        """
        Collect telemetry tokens. For each dimension, the value already in the
        telemetry context is used if present; otherwise its detector is called.

        Parameters
        ----------
        dimensions : frozenset[Dimension | str] | None, optional
            Dimensions to collect and return, or None to include all active
            dimensions.
        call_scoped_only : bool
            Required. True leaves out process-wide dimensions such as ``env``
            unless they are already set in the context (use it for the per-call
            snapshot); False includes them (use it for request headers).
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
        if method is not None and Dimension.METHOD.value not in tokens:
            tokens[Dimension.METHOD.value] = method
            tokens.pop(Dimension.METHOD_PARAMS.value, None)

        # Run registered detectors for any requested dimensions not already set
        for detector in self.detectors:
            if call_scoped_only and not getattr(detector, "call_scoped", True):
                continue
            dim_key = (
                detector.name.value
                if isinstance(detector.name, Dimension)
                else str(detector.name)
            )
            if dimensions is not None and dim_key not in dimensions:
                continue
            if dim_key not in tokens:
                try:
                    detected_val = detector.detect()
                    tokens[dim_key] = detected_val or ""
                except Exception:
                    tokens[dim_key] = ""

        if dimensions is None:
            return tokens
        return {k: v for k, v in tokens.items() if k in dimensions}

    def get_dimension(self, dimension: Dimension) -> str | None:
        """
        Resolve a specific dimension's bare token value (e.g. Dimension.FRAMEWORK -> 'pandas').

        The returned string is only stripped (not HTTP-header sanitized) and must not
        be interpolated directly into raw HTTP headers.
        """
        token_map = self.collect_tokens_map(
            dimensions=frozenset({dimension}), call_scoped_only=False
        )
        val = token_map.get(dimension.value)
        if not isinstance(val, str):
            return None
        stripped = val.strip()
        return stripped or None

    def encode_features(self) -> str | None:
        """Return web-safe base64 encoded GcsfsFeatures for active telemetry state."""
        tokens_map = self.collect_tokens_map(call_scoped_only=False)
        if not any(tokens_map.values()):
            return None

        from gcsfs.telemetry import schema as telemetry_schema

        return telemetry_schema.encode_features(tokens_map)


# Default singleton instance pre-configured with standard detectors
default_usage_tracker = UsageMetricsTracker(
    detectors=[
        FrameworkDetector(),
        EnvironmentDetector(),
    ]
)
