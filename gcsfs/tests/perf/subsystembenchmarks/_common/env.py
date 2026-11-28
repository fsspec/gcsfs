import os
import platform


def python_version() -> str:
    return ".".join(platform.python_version_tuple()[:2])


def machine_type() -> str:
    """Best-effort GCE machine type; empty string off-GCE."""
    return os.environ.get("MACHINE_TYPE", "")


def benchmark_source_commit_sha() -> str:
    """Commit containing the benchmark harness and configuration."""
    return os.environ.get("COMMIT_SHA", "unknown")
