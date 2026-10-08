"""Central Telemetry Coordinator managing detectors and context propagation."""

from __future__ import annotations

from typing import Dict, List, Optional

from gcsfs.telemetry.context import Dimension, get_telemetry_context, sanitize_token
from gcsfs.telemetry.detectors.base import BaseDetector
from gcsfs.telemetry.detectors.framework import FrameworkDetector


class UsageMetricsTracker:
    """Coordinates multi-dimensional detectors and context propagation."""

    def __init__(self, detectors: Optional[List[BaseDetector]] = None):
        self.detectors: List[BaseDetector] = (
            list(detectors) if detectors is not None else []
        )

    def collect_tokens_map(self) -> Dict[str, str]:
        """
        Collect active telemetry tokens across all registered detectors.

        Returns
        -------
        Dict[str, str]: Mapping of dimension name to formatted token string.
        """
        tokens = get_telemetry_context()

        # Run registered detectors for any dimensions not already set
        for detector in self.detectors:
            dim_key = (
                detector.name.value
                if isinstance(detector.name, Dimension)
                else str(detector.name)
            )
            if detector.is_enabled() and dim_key not in tokens:
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
        List[str]: List of formatted token strings, e.g. ['fw/pandas', 'env/gke'].
        """
        token_map = self.collect_tokens_map()
        sanitized_tokens = []
        for token in token_map.values():
            clean = sanitize_token(token)
            if clean:
                sanitized_tokens.append(clean)
        return sanitized_tokens

    def get_dimension(self, dimension: Dimension) -> Optional[str]:
        """
        Resolve a specific dimension token (e.g. Dimension.FRAMEWORK).
        """
        token_map = self.collect_tokens_map()
        return sanitize_token(token_map.get(dimension.value))


# Default singleton instance pre-configured with standard framework detector
default_usage_tracker = UsageMetricsTracker(
    detectors=[
        FrameworkDetector(),
    ]
)
