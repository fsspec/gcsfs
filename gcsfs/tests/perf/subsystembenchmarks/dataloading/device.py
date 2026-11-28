"""CPU/GPU/TPU batch delivery shared by data-loading read drivers.

On a GPU or TPU host each rank feeds its accelerator the way a training loop
does: the loader pins batches in host memory and the rank process copies them
to the device with ``non_blocking=True``. One synchronize per pass makes a
round's end timestamp include every copy without stalling the host per batch.
Accelerator tensors are never created in loader worker processes, and on TPU
loader iteration runs under ``accelerator.loader_worker_env()`` so spawned
DataLoader workers do not claim TPU devices. On a CPU host every helper is a
no-op and loaders are used unwrapped.
"""

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator


def rank_device(rank, *, world_size=None):
    """Bind this rank to its accelerator device; return None on CPU.

    Also creates the device context before timed rounds start.
    """
    if accelerator.device_kind() == "cpu":
        return None
    if world_size is None:
        return accelerator.bind_rank(rank)
    return accelerator.bind_rank(rank, global_rank=rank, world_size=world_size)


def pin_memory(device) -> bool:
    """Return True when the DataLoader should pin host batches for ``device``."""
    return device is not None


def _to_device(obj, device, tensor_cls):
    if isinstance(obj, tensor_cls):
        return obj if obj.device == device else obj.to(device, non_blocking=True)
    if isinstance(obj, dict):
        changed = False
        out = {}
        for key, value in obj.items():
            moved = _to_device(value, device, tensor_cls)
            out[key] = moved
            if moved is not value:
                changed = True
        return out if changed else obj
    if isinstance(obj, (list, tuple)):
        changed = False
        out_items = []
        for value in obj:
            moved = _to_device(value, device, tensor_cls)
            out_items.append(moved)
            if moved is not value:
                changed = True
        if not changed:
            return obj
        return tuple(out_items) if isinstance(obj, tuple) else out_items
    return obj


def to_device(batch, device):
    """Copy tensors nested in dicts/lists/tuples; pass other values through."""
    import torch

    return _to_device(batch, device, torch.Tensor)


class _DeviceFeed:
    """Iterable yielding ``loader`` batches delivered to ``device``."""

    def __init__(self, loader, device):
        self.loader = loader
        self.device = device

    def __iter__(self):
        with accelerator.loader_worker_env():
            try:
                for batch in self.loader:
                    yield (
                        batch if self.device is None else to_device(batch, self.device)
                    )
            finally:
                # Also runs when the consumer stops early and closes the iterator.
                # Host-only feeds (device None) queue no accelerator work.
                if self.device is not None:
                    accelerator.synchronize(self.device)


def feed(loader, device):
    """Wrap ``loader`` for accelerator delivery; unchanged on CPU."""
    if device is None and accelerator.device_kind() != "tpu":
        return loader
    return _DeviceFeed(loader, device)


def ray_task_resources(world_size: int, has_features: bool = False) -> dict:
    """Ray task resource kwargs for ``world_size`` consumers; empty for CPU or feature cases."""
    if has_features:
        return {}
    return accelerator.ray_resources(world_size)
