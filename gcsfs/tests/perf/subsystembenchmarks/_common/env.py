import os
import platform
import subprocess


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


def gce_machine_type() -> str:
    """Best-effort GCE machine type; empty string off-GCE."""
    return os.environ.get("GCE_MACHINE_TYPE", "")


def git_commit_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return os.environ.get("COMMIT_SHA", "unknown")
