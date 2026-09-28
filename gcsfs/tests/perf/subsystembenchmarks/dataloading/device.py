"""CPU/GPU batch delivery shared by data-loading read drivers.

On a CUDA host each rank feeds its GPU the way a training loop does: the
loader pins batches in host memory and the rank process copies them to the
device with ``non_blocking=True``. One synchronize per pass makes a round's
end timestamp include every copy without stalling the host per batch. CUDA
tensors are never created in loader worker processes, as PyTorch advises.
On a CPU host every helper is a no-op and loaders are used unwrapped.
"""

import math

from gcsfs.tests.perf.subsystembenchmarks._common import env

# Ray accepts fractional resource quantities to four decimal places.
_RAY_RESOURCE_DECIMALS = 4


def rank_device(rank):
    """Bind this rank to GPU ``rank % device_count``; return None on CPU.

    Also creates the CUDA context, which a training job has already paid for
    by the time its data loop starts, so timed rounds never include it.
    """
    if env.detect_accelerator() != "gpu":
        return None
    import torch

    index = rank % torch.cuda.device_count()
    torch.cuda.set_device(index)
    device = torch.device("cuda", index)
    torch.zeros(1, device=device)
    return device


def to_device(batch, device):
    """Copy tensors nested in dicts/lists/tuples; pass other values through."""
    import torch

    if isinstance(batch, torch.Tensor):
        return batch.to(device, non_blocking=True)
    if isinstance(batch, dict):
        return {key: to_device(value, device) for key, value in batch.items()}
    if isinstance(batch, list):
        return [to_device(value, device) for value in batch]
    if isinstance(batch, tuple):
        return tuple(to_device(value, device) for value in batch)
    return batch


def synchronize(device):
    """Wait for queued copies to ``device``; no-op on CPU."""
    if device is None:
        return
    import torch

    torch.cuda.synchronize(device)


class _DeviceFeed:
    """Iterable yielding ``loader`` batches copied to ``device``."""

    def __init__(self, loader, device):
        self.loader = loader
        self.device = device

    def __iter__(self):
        try:
            for batch in self.loader:
                yield to_device(batch, self.device)
        finally:
            # Also runs when the consumer stops early and closes the iterator.
            synchronize(self.device)


def feed(loader, device):
    """Wrap ``loader`` to deliver batches to ``device``; unchanged on CPU."""
    return loader if device is None else _DeviceFeed(loader, device)


def ray_task_num_gpus(world_size):
    """GPUs each Ray consumer task requests: 0 on CPU, else a fair share <= 1."""
    if env.detect_accelerator() != "gpu":
        return 0
    import torch

    scale = 10**_RAY_RESOURCE_DECIMALS
    share = math.floor(torch.cuda.device_count() / world_size * scale) / scale
    return min(1.0, share)
