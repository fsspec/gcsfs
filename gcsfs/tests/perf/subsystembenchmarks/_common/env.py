import os
import platform


def _cuda_available() -> bool:
    try:
        import torch

        return bool(torch.cuda.is_available())
    except Exception:
        return False


def detect_accelerator() -> str:
    """Auto-detect the accelerator. Never a benchmark parameter — an environment fact."""
    return "gpu" if _cuda_available() else "cpu"


def detect_backend() -> str:
    return "nccl" if detect_accelerator() == "gpu" else "gloo"


def python_version() -> str:
    return ".".join(platform.python_version_tuple()[:2])


def gpu_model() -> str:
    try:
        import torch

        if torch.cuda.is_available():
            return torch.cuda.get_device_name(0)
    except Exception:
        pass
    return "none"


def machine_type() -> str:
    """Best-effort GCE machine type; empty string off-GCE."""
    return os.environ.get("MACHINE_TYPE", "")


def benchmark_source_commit_sha() -> str:
    """Commit containing the benchmark harness and configuration."""
    return os.environ.get("COMMIT_SHA", "unknown")
