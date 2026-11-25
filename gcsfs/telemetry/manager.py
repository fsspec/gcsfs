"""Central Telemetry Coordinator managing detectors and context propagation."""

from __future__ import annotations

import os
from collections.abc import Mapping

from gcsfs.telemetry.context import (
    USER_AGENT_DIMENSIONS,
    Dimension,
    canonicalize_method_params,
    get_telemetry_context,
    sanitize_token,
)
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
        method_params: str = "",
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
        method_params : str, optional
            `.`-joined tracked parameter names to attach under Dimension.METHOD_PARAMS.

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
            if method and (canon_mp := canonicalize_method_params(method_params)):
                tokens[Dimension.METHOD_PARAMS.value] = canon_mp

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

    def get_tokens(
        self,
        dimensions: frozenset[Dimension | str] | None = None,
        tokens_map: Mapping[str, str] | None = None,
    ) -> list[str]:
        """
        Return sanitized formatted telemetry tokens for the requested dimensions.

        Parameters
        ----------
        dimensions : frozenset[Dimension | str] | None, optional
            Dimensions to include, or None to include all active dimensions.
        tokens_map : Mapping[str, str] | None, optional
            Optional mapping already returned by ``collect_tokens_map()``; used
            as is without re-running detectors or re-checking opt-out.

        Returns
        -------
        list[str]
            List of sanitized formatted token strings, e.g. ['fw/pandas', 'env/gke'].
        """
        if tokens_map is None:
            # Include process-wide dimensions (e.g. env), read fresh per request.
            tokens_map = self.collect_tokens_map(
                dimensions=dimensions, call_scoped_only=False
            )

        out = []
        for dim_key, val in tokens_map.items():
            if dimensions is None or dim_key in dimensions:
                clean_val = sanitize_token(val)
                if clean_val:
                    out.append(f"{dim_key}/{clean_val}")
        return out

    def get_dimension(self, dimension: Dimension) -> str | None:
        """
        Resolve a specific dimension's sanitized bare token value (e.g. Dimension.FRAMEWORK -> 'pandas').
        """
        token_map = self.collect_tokens_map(
            dimensions=frozenset({dimension}), call_scoped_only=False
        )
        return sanitize_token(token_map.get(dimension.value))

    def encode_features(
        self, tokens_map: Mapping[str, str] | None = None
    ) -> str | None:
        """Return web-safe base64 encoded GcsfsFeatures for active telemetry state."""
        if tokens_map is None:
            tokens_map = self.collect_tokens_map(call_scoped_only=False)
        if not any(tokens_map.values()):
            return None

        from gcsfs.telemetry import schema as telemetry_schema

        return telemetry_schema.encode_features(tokens_map)

    def get_http_telemetry(
        self,
        *,
        include_tokens: bool = True,
        include_features: bool = True,
    ) -> tuple[list[str], str | None]:
        """Collect HTTP User-Agent tokens and encoded GcsfsFeatures in one lookup."""
        if not (include_tokens or include_features):
            return [], None

        tokens_map = self.collect_tokens_map(call_scoped_only=False)
        ua_tokens = (
            self.get_tokens(dimensions=USER_AGENT_DIMENSIONS, tokens_map=tokens_map)
            if include_tokens
            else []
        )
        encoded = (
            self.encode_features(tokens_map=tokens_map) if include_features else None
        )
        return ua_tokens, encoded


# Default singleton instance pre-configured with standard detectors
default_usage_tracker = UsageMetricsTracker(
    detectors=[
        FrameworkDetector(),
        EnvironmentDetector(),
    ]
)
