"""Shared CPU, CUDA GPU, and TorchTPU accelerator detection and lifecycle helpers."""

from __future__ import annotations

import contextlib
import dataclasses
import functools
import importlib.util
import json
import math
import os
import random
import socket
import stat
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import torch

DEVICE_ENV = "GCSFS_SUBSYSTEM_DEVICE"
DEVICES = ("auto", "cpu", "cuda", "tpu")
CPU_MAX_RANKS = 16
TPU_SLICE_TOPOLOGY_ENV = "GCSFS_SUBSYSTEM_TPU_SLICE_TOPOLOGY"
TPU_SLICE_ADDRESSES_ENV = "GCSFS_SUBSYSTEM_TPU_SLICE_ADDRESSES"


class AcceleratorSetupError(RuntimeError):
    """Raised when accelerator selection or runtime setup is incomplete."""


@dataclasses.dataclass(frozen=True)
class _TpuSpec:
    slug: str
    topologies: dict[int, str]


_GOOGLE_PCI_VENDOR = "0x1ae0"
_PCI_DEVICES_ROOT = Path("/sys/bus/pci/devices")
_NVIDIA_CTL_PATH = Path("/dev/nvidiactl")
_VFIO_DEV_ROOT = Path("/dev/vfio")
_SHM_ROOT = Path("/dev/shm")
_PROC_ROOT = Path("/proc")
_TPU_PG_STORE_PREFIX = "torch_tpu_pg_store_"
_NON_EPHEMERAL_PORT_MIN = 15000
_NON_EPHEMERAL_PORT_MAX = 31999
_RAY_RESOURCE_DECIMALS = 4

# Single-host TPU topologies mapped by PCI device ID (matches torch_tpu TpuSpec).
_TPU_PCI_SPECS: dict[str, _TpuSpec] = {
    "0x0063": _TpuSpec("v5e", {1: "1,1,1", 4: "2,2,1", 8: "2,4,1"}),
    "0x0062": _TpuSpec("v5p", {1: "1,1,1", 2: "1,2,1", 4: "2,2,1", 8: "2,2,2"}),
    "0x006f": _TpuSpec("v6e", {1: "1,1,1", 4: "2,2,1", 8: "2,4,1"}),
    "0x0076": _TpuSpec("tpu7x", {2: "1,1,1,2", 4: "1,2,1,2", 8: "2,2,1,2"}),
    "0x007c": _TpuSpec("v8t", {1: "1,1,1", 2: "1,2,1", 4: "2,2,1"}),
    "0x0083": _TpuSpec("v8i", {2: "1,1,1,2", 4: "2,1,1,2", 8: "4,1,1,2"}),
}
_TOPOLOGIES_BY_SLUG: dict[str, dict[int, str]] = {
    spec.slug: spec.topologies for spec in _TPU_PCI_SPECS.values()
}

# Tracks env vars written by prepare_launch so sequential calls with different world_size
# values (including inside spawned driver.setup child processes) refresh managed settings
# while externally preset user env vars stay untouched.
_MANAGED_ENV_MARKER = "_GCSFS_SUBSYSTEM_MANAGED_TPU_ENV"
TPU_LAUNCH_ENV_VARS = (
    "TORCH_TPU_TOPOLOGY",
    "TORCH_TPU_SLICEBUILDER_ADDRESSES",
    "TORCH_TPU_XPROF_SESSION_ID",
    "MASTER_ADDR",
    "MASTER_PORT",
    _MANAGED_ENV_MARKER,
)


@functools.lru_cache(maxsize=4)
def _scan_tpu_pci_devices(pci_root: str) -> tuple[tuple[Path, _TpuSpec], ...]:
    root = Path(pci_root)
    if not root.is_dir():
        return ()
    found: list[tuple[Path, _TpuSpec]] = []
    for dev in sorted(root.iterdir()):
        try:
            vendor = (dev / "vendor").read_text().strip().lower()
            device = (dev / "device").read_text().strip().lower()
        except OSError:
            continue
        if vendor == _GOOGLE_PCI_VENDOR and device in _TPU_PCI_SPECS:
            found.append((dev, _TPU_PCI_SPECS[device]))
    return tuple(found)


def _iter_tpu_pci_devices() -> list[tuple[Path, _TpuSpec]]:
    return list(_scan_tpu_pci_devices(str(_PCI_DEVICES_ROOT)))


def _tpu_hardware_present() -> bool:
    return bool(_iter_tpu_pci_devices())


def _torch_tpu_installed() -> bool:
    return (
        "torch_tpu" in sys.modules or importlib.util.find_spec("torch_tpu") is not None
    )


def _tpu_device_count() -> int:
    vfio_count = 0
    for dev, _ in _iter_tpu_pci_devices():
        iommu = dev / "iommu_group"
        if not iommu.is_symlink():
            continue
        vfio_node = _VFIO_DEV_ROOT / os.path.basename(os.readlink(iommu))
        try:
            st = vfio_node.stat()
            if stat.S_ISCHR(st.st_mode) and os.access(vfio_node, os.R_OK | os.W_OK):
                vfio_count += 1
        except OSError:
            pass
    visible = [
        x for x in os.environ.get("TPU_VISIBLE_DEVICES", "").split(",") if x.strip()
    ]
    if visible:
        return min(vfio_count, len(visible))
    return vfio_count


def _tpu_type() -> str:
    devices = _iter_tpu_pci_devices()
    return devices[0][1].slug if devices else "none"


def _cuda_available() -> bool:
    try:
        import torch

        return bool(torch.cuda.is_available())
    except Exception:
        return False


def _cuda_hardware_present() -> bool:
    if "CUDA_VISIBLE_DEVICES" in os.environ:
        masked = os.environ["CUDA_VISIBLE_DEVICES"].strip().lower()
        if masked in ("", "-1", "nodevfiles"):
            return False
    try:
        return _NVIDIA_CTL_PATH.exists()
    except OSError:
        return False


def _validate_device_env(raw: str) -> str:
    if raw not in DEVICES:
        raise ValueError(f"Unsupported device {raw!r}; expected one of {DEVICES}")
    return raw


@functools.lru_cache(maxsize=8)
def _resolve_device_kind(requested: str) -> str:
    if requested == "cpu":
        return "cpu"
    if requested == "cuda":
        if not _cuda_available():
            raise AcceleratorSetupError(
                f"{DEVICE_ENV}=cuda but CUDA is not available on this host"
            )
        return "cuda"
    if requested == "tpu":
        if not _tpu_hardware_present():
            raise AcceleratorSetupError(
                f"{DEVICE_ENV}=tpu but no TPU hardware was detected on this host"
            )
        if not _torch_tpu_installed():
            raise AcceleratorSetupError(
                "TPU host detected but torch_tpu is not installed; pip install torch_tpu"
            )
        return "tpu"
    # requested == "auto"
    if _cuda_available():
        return "cuda"
    if _cuda_hardware_present():
        raise AcceleratorSetupError(
            "GPU hardware detected but CUDA is not available; pass --device=cpu to force CPU"
        )
    if _tpu_hardware_present():
        if not _torch_tpu_installed():
            raise AcceleratorSetupError(
                "TPU host detected but torch_tpu is not installed; pip install torch_tpu"
            )
        return "tpu"
    return "cpu"


def device_kind() -> str:
    """Return active device kind ('cpu', 'cuda', or 'tpu'), cached per requested mode."""
    requested = _validate_device_env(os.environ.get(DEVICE_ENV, "auto"))
    return _resolve_device_kind(requested)


def _clear_device_caches() -> None:
    _resolve_device_kind.cache_clear()
    _scan_tpu_pci_devices.cache_clear()


device_kind.cache_clear = _clear_device_caches  # type: ignore[attr-defined]


def accelerator_type() -> str:
    """Return reporting accelerator label ('cpu', 'gpu', or 'tpu')."""
    kind = device_kind()
    return "gpu" if kind == "cuda" else kind


def local_device_count() -> int:
    """Return usable rank capacity on this host without initializing TPU runtime."""
    kind = device_kind()
    if kind == "cpu":
        return CPU_MAX_RANKS
    if kind == "cuda":
        import torch

        count = int(torch.cuda.device_count())
        if count == 0:
            raise AcceleratorSetupError(
                f"{DEVICE_ENV}=cuda but torch.cuda.device_count() == 0"
            )
        return count
    return _tpu_device_count()


def process_group_backend(world_size: int | None = None) -> str:
    """Return PyTorch init_process_group backend string for the active device."""
    kind = device_kind()
    if kind == "cuda":
        return "cpu:gloo,cuda:nccl"
    if kind == "tpu":
        return "gloo" if world_size == 1 else "tpu_dist"
    return "gloo"


def backend_name(world_size: int | None = None) -> str:
    """Return concise distributed backend label ('gloo', 'nccl', or 'tpu_dist')."""
    kind = device_kind()
    if kind == "cuda":
        return "nccl"
    if kind == "tpu":
        return "gloo" if world_size == 1 else "tpu_dist"
    return "gloo"


def _try_bind_port(port: int) -> socket.socket | None:
    """Bind ``port`` across dual-stack IPv6/IPv4 (matching libtpu gRPC ``[::]:port``)."""
    sock6: socket.socket | None = None
    try:
        sock6 = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock6.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
    except OSError:
        if sock6 is not None:
            sock6.close()
        sock6 = None

    if sock6 is not None:
        try:
            sock6.bind(("::", port))
            return sock6
        except OSError:
            sock6.close()
            return None

    sock4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock4.bind(("", port))
        return sock4
    except OSError:
        sock4.close()
        return None


def _free_ports(count: int) -> list[int]:
    """Reserve ``count`` distinct free TCP ports outside Linux's ephemeral range."""
    sockets: list[socket.socket] = []
    ports: list[int] = []
    span = _NON_EPHEMERAL_PORT_MAX - _NON_EPHEMERAL_PORT_MIN + 1
    start = random.SystemRandom().randrange(span)
    try:
        for offset in range(span):
            if len(ports) >= count:
                break
            candidate = _NON_EPHEMERAL_PORT_MIN + ((start + offset) % span)
            sock = _try_bind_port(candidate)
            if sock is not None:
                ports.append(int(sock.getsockname()[1]))
                sockets.append(sock)
        while len(ports) < count:
            sock = _try_bind_port(0)
            if sock is None:
                raise OSError("Failed to bind an ephemeral port")
            ports.append(int(sock.getsockname()[1]))
            sockets.append(sock)
    finally:
        for sock in sockets:
            sock.close()
    return ports


def _cleanup_stale_tpu_pg_stores() -> None:
    """Remove stale TorchTPU ``/dev/shm/torch_tpu_pg_store_<ppid>`` FileStore files."""
    if not _SHM_ROOT.is_dir():
        return
    own_pid = os.getpid()
    proc_exists = _PROC_ROOT.is_dir()
    for path in _SHM_ROOT.glob(f"{_TPU_PG_STORE_PREFIX}*"):
        suffix = path.name[len(_TPU_PG_STORE_PREFIX) :]
        if not suffix.isdigit():
            continue
        pid = int(suffix)
        if pid == own_pid or (proc_exists and not (_PROC_ROOT / suffix).exists()):
            try:
                path.unlink()
            except OSError:
                pass


def _load_managed_env() -> dict[str, str]:
    raw = os.environ.get(_MANAGED_ENV_MARKER)
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except ValueError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {str(k): str(v) for k, v in parsed.items() if isinstance(v, str)}


def _save_managed_env(managed: dict[str, str]) -> None:
    if managed:
        os.environ[_MANAGED_ENV_MARKER] = json.dumps(managed, sort_keys=True)
    else:
        os.environ.pop(_MANAGED_ENV_MARKER, None)


def capture_launch_env(extra_keys: tuple[str, ...] = ()) -> dict[str, str | None]:
    """Snapshot managed TPU launch env vars (plus any ``extra_keys``) for later restore."""
    return {key: os.environ.get(key) for key in (*TPU_LAUNCH_ENV_VARS, *extra_keys)}


def restore_launch_env(snapshot: dict[str, str | None]) -> None:
    """Restore env vars captured by :func:`capture_launch_env`."""
    for key, value in snapshot.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _set_managed_env(key: str, value: str) -> None:
    managed = _load_managed_env()
    cur = os.environ.get(key)
    if cur is not None and managed.get(key) != cur:
        return
    os.environ[key] = value
    managed[key] = value
    _save_managed_env(managed)


def _clear_managed_env(key: str) -> None:
    managed = _load_managed_env()
    cur = os.environ.get(key)
    if cur is not None and managed.get(key) == cur:
        os.environ.pop(key, None)
    managed.pop(key, None)
    _save_managed_env(managed)


def prepare_launch(world_size: int) -> None:
    """Configure parent slice env vars before spawning ranks on TPU; no-op elsewhere."""
    if device_kind() != "tpu":
        return
    _cleanup_stale_tpu_pg_stores()
    tpu_type = _tpu_type()
    local_count = local_device_count()
    if world_size > local_count:
        slice_topo = os.environ.get(TPU_SLICE_TOPOLOGY_ENV, "").strip()
        slice_addrs = [
            addr.strip()
            for addr in os.environ.get(TPU_SLICE_ADDRESSES_ENV, "").split(",")
            if addr.strip()
        ]
        if not slice_topo or len(slice_addrs) != world_size:
            raise AcceleratorSetupError(
                f"Multi-host TPU world_size={world_size} requires "
                f"{TPU_SLICE_TOPOLOGY_ENV} and {world_size} entries in "
                f"{TPU_SLICE_ADDRESSES_ENV} (got {len(slice_addrs)})"
            )
        _set_managed_env("TORCH_TPU_TOPOLOGY", slice_topo)
        _set_managed_env("TORCH_TPU_SLICEBUILDER_ADDRESSES", ",".join(slice_addrs))
        _clear_managed_env("MASTER_ADDR")
        _clear_managed_env("MASTER_PORT")
    else:
        topology = _TOPOLOGIES_BY_SLUG.get(tpu_type, {}).get(world_size)
        if topology is not None:
            ports = _free_ports(world_size + 1)
            addrs = ",".join(f"localhost:{port}" for port in ports[:world_size])
            _set_managed_env("TORCH_TPU_TOPOLOGY", topology)
            _set_managed_env("TORCH_TPU_SLICEBUILDER_ADDRESSES", addrs)
            _set_managed_env("MASTER_ADDR", "127.0.0.1")
            _set_managed_env("MASTER_PORT", str(ports[world_size]))
        elif world_size == 1:
            _clear_managed_env("TORCH_TPU_TOPOLOGY")
            _clear_managed_env("TORCH_TPU_SLICEBUILDER_ADDRESSES")
            _clear_managed_env("MASTER_ADDR")
            _clear_managed_env("MASTER_PORT")
        else:
            raise AcceleratorSetupError(
                f"Unsupported TPU world_size={world_size} for {tpu_type}"
            )
    _set_managed_env("TORCH_TPU_XPROF_SESSION_ID", str(time.time_ns()))


def bind_rank(
    local_rank: int,
    *,
    global_rank: int | None = None,
    world_size: int | None = None,
) -> "torch.device":
    """Bind the current spawned rank to its device and warm up runtime state."""
    import torch

    kind = device_kind()
    if kind == "cpu":
        return torch.device("cpu")
    if kind == "cuda":
        idx = local_rank % local_device_count()
        torch.cuda.set_device(idx)
        dev = torch.device("cuda", idx)
        torch.zeros(1, device=dev)
        return dev

    rank_value = local_rank if global_rank is None else global_rank
    os.environ["RANK"] = str(rank_value)
    os.environ["LOCAL_RANK"] = str(local_rank)
    addrs = [
        a
        for a in os.environ.get("TORCH_TPU_SLICEBUILDER_ADDRESSES", "").split(",")
        if a.strip()
    ]
    ws = world_size if world_size is not None else (len(addrs) if addrs else None)
    if ws is not None:
        local_count = local_device_count()
        per_host = local_count if local_count > 0 and ws > local_count else ws
        os.environ["WORLD_SIZE"] = str(ws)
        os.environ["TORCH_TPU_DEVICES_PER_HOST"] = str(per_host)
    dev = torch.device("tpu")
    torch.zeros(1, device=dev)
    return dev


def synchronize(device: "torch.device | None") -> None:
    """Wait for queued work on ``device``; no-op on CPU or None."""
    if device is None or device.type == "cpu":
        return
    import torch

    torch.accelerator.synchronize(device)


def model_name() -> str:
    """Return GPU name, TPU type slug (e.g. 'tpu7x'), or 'none'."""
    kind = device_kind()
    if kind == "cuda":
        try:
            import torch

            return str(torch.cuda.get_device_name(0))
        except Exception:
            return "none"
    if kind == "tpu":
        return _tpu_type()
    return "none"


@contextlib.contextmanager
def loader_worker_env():
    """Temporarily disable Torch device backend autoload for DataLoader workers on TPU."""
    if device_kind() != "tpu":
        yield
        return
    key = "TORCH_DEVICE_BACKEND_AUTOLOAD"
    prev = os.environ.get(key)
    os.environ[key] = "0"
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = prev


def ray_resources(world_size: int) -> dict:
    """Return Ray resource kwargs for ``world_size`` tasks/actors on this host."""
    if world_size <= 0:
        raise ValueError(f"world_size must be positive, got {world_size}")
    kind = device_kind()
    if kind == "cpu":
        return {}
    count = local_device_count()
    if kind == "tpu":
        # Ray only accepts whole TPU chips (1, 2, 4 or 8), never a fraction.
        if world_size > count:
            raise ValueError(f"world_size {world_size} exceeds {count} tpu devices")
        return {"resources": {"TPU": 1.0}}
    scale = 10**_RAY_RESOURCE_DECIMALS
    share = min(1.0, math.floor(count / world_size * scale) / scale)
    return {"num_gpus": share}


if __name__ == "__main__":
    if sys.argv[1:] == ["--has-tpu-hardware"]:
        if "PCI_DEVICES_ROOT" in os.environ:
            _PCI_DEVICES_ROOT = Path(os.environ["PCI_DEVICES_ROOT"])
        if "VFIO_DEV_ROOT" in os.environ:
            _VFIO_DEV_ROOT = Path(os.environ["VFIO_DEV_ROOT"])
        sys.exit(0 if _tpu_device_count() > 0 else 1)
    raise SystemExit("Usage: accelerator.py --has-tpu-hardware")
