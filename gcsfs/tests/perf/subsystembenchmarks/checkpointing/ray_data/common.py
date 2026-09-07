"""Shared utilities, models, and execution harness for Ray checkpointing benchmarks."""

import logging
import os
import shutil
import socket
import tempfile

import fsspec
import pyarrow.fs
import ray
import torch
import torch.distributed as dist
import torch.distributed.checkpoint as dcp
from torch.distributed.checkpoint.state_dict import StateDictOptions, get_state_dict

from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import assert_fsspec_gcsfs


def ensure_ray_initialized():
    """Initializes local Ray cluster on CPU if not already running."""
    if not ray.is_initialized():
        ray.init(
            include_dashboard=False,
            ignore_reinit_error=True,
            logging_level=logging.WARNING,
        )


def find_free_port():
    """Finds an available TCP port for the Gloo distributed backend."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def setup_distributed_env(rank, world_size, port):
    """Initializes the CPU distributed environment (gloo) for a worker."""
    os.environ["MASTER_ADDR"] = "localhost"
    os.environ["MASTER_PORT"] = str(port)
    os.environ["GLOO_SOCKET_IFNAME"] = "lo"
    os.environ["WORLD_SIZE"] = str(world_size)
    os.environ["RANK"] = str(rank)
    os.environ["LOCAL_RANK"] = str(rank)

    dist.init_process_group("gloo", rank=rank, world_size=world_size)


def is_distributed_strategy(strategy: str) -> bool:
    """Returns True if the strategy requires multi-process coordination."""
    return strategy != "single"


def _llama_tp_plan():
    from torch.distributed.tensor.parallel import ColwiseParallel, RowwiseParallel

    return {
        "self_attn.q_proj": ColwiseParallel(),
        "self_attn.k_proj": ColwiseParallel(),
        "self_attn.v_proj": ColwiseParallel(),
        "self_attn.o_proj": RowwiseParallel(),
        "mlp.gate_proj": ColwiseParallel(),
        "mlp.up_proj": ColwiseParallel(),
        "mlp.down_proj": RowwiseParallel(),
    }


class BenchmarkModel(torch.nn.Module):
    """CPU-simulated model combining a frozen large payload and a trainable probe."""

    def __init__(self, payload, probe):
        super().__init__()
        self.payload = payload
        self.probe = probe

    def forward(self, x):
        return self.probe(x)


def load_benchmark_model(params):
    """Loads the benchmark model, freezing the large payload for CPU simulation."""
    import transformers

    model_id = params.model_id
    use_local_files_only = False
    if model_id.startswith("gs://"):
        use_local_files_only = True
        dir_name = os.path.basename(model_id.rstrip("/"))
        model_id = os.path.join("/tmp", dir_name)

    payload = transformers.AutoModelForCausalLM.from_pretrained(
        model_id,
        torch_dtype=torch.bfloat16,
        local_files_only=use_local_files_only,
        use_cache=False,
    )

    for p in payload.parameters():
        p.requires_grad = False

    probe = torch.nn.Linear(8, 8, dtype=torch.bfloat16)
    probe.weight.requires_grad = True

    return BenchmarkModel(payload=payload, probe=probe)


def materialize_adamw_states(optimizer):
    """Eagerly allocate AdamW moments so checkpoint size is realistic on CPU."""
    for group in optimizer.param_groups:
        for p in group["params"]:
            state = optimizer.state[p]
            if state:
                continue
            state["step"] = torch.zeros((), dtype=torch.float32)
            state["exp_avg"] = torch.empty_like(p, memory_format=torch.preserve_format)
            state["exp_avg_sq"] = torch.empty_like(
                p, memory_format=torch.preserve_format
            )


def parallelize_model(model, params):
    """Parallelizes the model according to the benchmark strategy."""
    strategy = params.strategy
    if strategy == "single":
        return model

    if strategy == "ddp":
        return torch.nn.parallel.DistributedDataParallel(
            model, find_unused_parameters=False
        )

    from torch.distributed.device_mesh import init_device_mesh
    from torch.distributed.fsdp import MixedPrecisionPolicy, fully_shard
    from torch.distributed.tensor.parallel import parallelize_module

    mp_policy = MixedPrecisionPolicy(
        param_dtype=torch.bfloat16,
        reduce_dtype=torch.bfloat16,
    )

    if strategy in ("fsdp_sharded", "fsdp_full"):
        mesh = init_device_mesh("cpu", (params.world_size,))
        for layer in model.payload.model.layers:
            fully_shard(layer, mesh=mesh, mp_policy=mp_policy)
        fully_shard(model.payload, mesh=mesh, mp_policy=mp_policy)
        fully_shard(model.probe, mesh=mesh, mp_policy=mp_policy)
        return model

    if strategy in ("model_parallel_sharded", "model_parallel_full"):
        mesh = init_device_mesh(
            "cpu",
            (params.data_parallel_size, params.tensor_parallel_size),
            mesh_dim_names=("dp", "tp"),
        )
        dp_mesh = mesh["dp"]
        tp_mesh = mesh["tp"]

        for layer in model.payload.model.layers:
            if params.tensor_parallel_size > 1:
                parallelize_module(layer, tp_mesh, _llama_tp_plan())
            fully_shard(layer, mesh=dp_mesh, mp_policy=mp_policy)
        fully_shard(model.payload, mesh=dp_mesh, mp_policy=mp_policy)
        fully_shard(model.probe, mesh=dp_mesh, mp_policy=mp_policy)
        return model

    raise ValueError(f"Unknown strategy: {strategy}")


def setup_model_and_optimizer(params):
    """Loads, parallelizes the benchmark model and materializes AdamW optimizer states."""
    model = load_benchmark_model(params)
    model = parallelize_model(model, params)
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3)
    materialize_adamw_states(optimizer)
    return model, optimizer


def resolve_storage(prefix: str):
    """Resolves fsspec and PyArrow filesystems for checkpoint storage."""
    fs, base_path = fsspec.core.url_to_fs(prefix)
    if str(prefix).startswith("gs://"):
        assert_fsspec_gcsfs(prefix)
    arrow_fs = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(fs))
    return fs, arrow_fs, base_path


def _pyarrow_fs_copy_files(
    source,
    destination,
    source_filesystem=None,
    destination_filesystem=None,
    chunk_size=64 * 1024 * 1024,
):
    """Copies files using PyArrow with 64 MiB chunk size for high-throughput transfers."""
    return pyarrow.fs.copy_files(
        source,
        destination,
        source_filesystem=source_filesystem,
        destination_filesystem=destination_filesystem,
        chunk_size=chunk_size,
    )


def _get_staging_dir(prefix: str, min_free_gb: float = 50.0) -> str:
    """Creates a temporary staging directory, prioritizing /dev/shm or /mnt/ramdisk to avoid root disk exhaustion."""
    for candidate in ("/mnt/ramdisk", "/dev/shm"):
        if os.path.isdir(candidate) and os.access(candidate, os.W_OK):
            try:
                stat = os.statvfs(candidate)
                avail_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
                if avail_gb >= min_free_gb:
                    return tempfile.mkdtemp(prefix=prefix, dir=candidate)
            except Exception as e:
                logging.warning(
                    "Could not use staging directory candidate %s: %s",
                    candidate,
                    e,
                )
    return tempfile.mkdtemp(prefix=prefix)


def save_checkpoint_step(
    model,
    optimizer,
    params,
    rank: int,
    arrow_fs,
    fs,
    destination_ckpt: str,
    staging_prefix: str = "ray-ckpt",
):
    """Performs a single distributed or single-node checkpoint save to storage."""
    is_sharded = params.strategy in (
        "fsdp_sharded",
        "model_parallel_sharded",
    )
    options = StateDictOptions(
        full_state_dict=not is_sharded,
        cpu_offload=not is_sharded,
    )

    local_dir = None
    try:
        model_state, opt_state = get_state_dict(model, optimizer, options=options)
        app_state = {"model": model_state, "optimizer": opt_state}

        if is_sharded:
            local_dir = _get_staging_dir(f"{staging_prefix}-rank{rank}-")
            dcp.save(
                {"app": app_state},
                storage_writer=dcp.FileSystemWriter(local_dir),
            )
            arrow_fs.create_dir(destination_ckpt)
            _pyarrow_fs_copy_files(
                local_dir,
                destination_ckpt,
                destination_filesystem=arrow_fs,
            )
        else:
            if rank == 0:
                local_dir = _get_staging_dir(f"{staging_prefix}-rank0-")
                ckpt_file = os.path.join(local_dir, "checkpoint.pt")
                torch.save(app_state, ckpt_file)
                arrow_fs.create_dir(destination_ckpt)
                _pyarrow_fs_copy_files(
                    local_dir,
                    destination_ckpt,
                    destination_filesystem=arrow_fs,
                )

        del app_state, model_state, opt_state
        dist.barrier()
        if is_sharded and rank == 0:
            fs.touch(f"{destination_ckpt.rstrip('/')}/_SUCCESS")
    finally:
        if local_dir and os.path.exists(local_dir):
            shutil.rmtree(local_dir, ignore_errors=True)
