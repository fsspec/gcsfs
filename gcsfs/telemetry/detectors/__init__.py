"""Telemetry detectors package."""
from gcsfs.telemetry.detectors.base import BaseDetector
from gcsfs.telemetry.detectors.framework import FrameworkDetector

__all__ = [
    "BaseDetector",
    "FrameworkDetector",
]
