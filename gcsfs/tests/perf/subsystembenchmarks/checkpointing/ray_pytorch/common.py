"""Shared utilities, models, and execution harness for Ray checkpointing benchmarks."""

from __future__ import annotations

import dataclasses
import json
import logging
import os
import tempfile

import fsspec
import pyarrow.fs
import ray
import torch
import torch.distributed as dist
import torch.distributed.checkpoint as dcp
from torch.distributed.checkpoint.state_dict import StateDictOptions, get_state_dict

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator, launcher
from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
    find_free_port,
    reduce_durations,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing._llama_tp import apply_llama_tp
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.model_staging import model_dir
from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import assert_fsspec_gcsfs

_TPU_ENV_PREFIX = "TORCH_TPU_"
_RAY_NOSET_TPU_ENV = "RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS"
_WORKER_MODULE = (
    "gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_pytorch._worker"
)


def prepare_tpu_env(world_size: int) -> None:
    """Clear stale TORCH_TPU_* vars and set single-node TPU slice env for Ray actors."""
    count = accelerator.local_device_count()
    if world_size > count:
        raise ValueError(
            f"world_size {world_size} exceeds {count} "
            f"{accelerator.accelerator_type()} devices"
        )
    for key in [k for k in os.environ if k.startswith(_TPU_ENV_PREFIX)]:
        os.environ.pop(key, None)
    if world_size > 1:
        os.environ[_RAY_NOSET_TPU_ENV] = "1"
        from torch.tpu import distributed as tpu_distributed

        tpu_distributed.set_tpu_launch_env(nproc_per_node=world_size)


def ensure_ray_initialized(world_size: int | None = None) -> None:
    """Initialize a local Ray cluster, bootstrapping TPU launch env if needed."""
    if ray.is_initialized():
        return
    if accelerator.device_kind() == "tpu":
        prepare_tpu_env(world_size if world_size is not None else 1)
    ray.init(
        address="local",
        include_dashboard=False,
        ignore_reinit_error=True,
        logging_level=logging.WARNING,
    )


def kill_actors(workers) -> None:
    """Explicitly terminate Ray actors before cluster shutdown."""
    if not workers or not ray.is_initialized():
        return
    for worker in workers:
        try:
            ray.kill(worker, no_restart=True)
        except Exception:
            pass


def shutdown_ray() -> None:
    """Shut down Ray if initialized."""
    if ray.is_initialized():
        ray.shutdown()


def run_ray_workers(
    worker_cls,
    world_size: int,
    prefix: str,
    params,
    *,
    run_method: str,
    setup_method: str | None = None,
):
    """Spawn ``world_size`` Ray actors, optionally run setup, execute ``run_method``, and kill them."""
    ensure_ray_initialized(world_size)
    port = find_free_port()
    actor_options = dict(accelerator.ray_resources(world_size))
    ram_root = _ram_staging_root()
    if ram_root is not None:
        # Checkpoint.as_directory() downloads into tempfile.gettempdir(); keep it off
        # the boot disk. Every actor on the host shares it, so Ray's download lock
        # still dedupes across ranks.
        actor_options["runtime_env"] = {"env_vars": {"TMPDIR": ram_root}}
    workers = []
    try:
        workers = [
            worker_cls.options(**actor_options).remote(
                rank, world_size, port, prefix, params
            )
            for rank in range(world_size)
        ]
        if setup_method is not None:
            ray.get([getattr(w, setup_method).remote() for w in workers])
        return ray.get([getattr(w, run_method).remote() for w in workers])
    finally:
        kill_actors(workers)


def reduce_ray_durations(results, world_size: int, rounds: int) -> list[float]:
    """Reduce per-rank ``[(start, end), ...]`` round timestamps across ``world_size``."""
    return reduce_durations(results, world_size, rounds)


def run_subprocess_worker(
    op: str,
    prefix: str,
    params,
    *,
    expect_result: bool = False,
) -> dict | None:
    """Launch ``ray_pytorch._worker`` in an isolated subprocess."""
    with tempfile.TemporaryDirectory(prefix="ssb-ray-ckpt-") as tmp_dir:
        spec_file = os.path.join(tmp_dir, "spec.json")
        result_file = os.path.join(tmp_dir, "result.json")
        with open(spec_file, "w", encoding="utf-8") as f:
            json.dump({"prefix": prefix, "params": dataclasses.asdict(params)}, f)
        args = ["--op", op, "--spec-file", spec_file]
        if expect_result:
            args.extend(["--result-file", result_file])
        launcher.run_subprocess(_WORKER_MODULE, args)
        if not expect_result:
            return None
        with open(result_file, encoding="utf-8") as f:
            return json.load(f)


def is_distributed_strategy(strategy: str) -> bool:
    """Returns True if the strategy requires multi-process coordination."""
    return strategy != "single"


def is_sharded_strategy(strategy: str) -> bool:
    """Returns True if each rank owns a shard of the checkpoint."""
    return strategy in ("fsdp_sharded", "model_parallel_sharded")


class BenchmarkModel(torch.nn.Module):
    """Model combining a frozen large payload and a trainable probe."""

    def __init__(self, payload, probe):
        super().__init__()
        self.payload = payload
        self.probe = probe

    def forward(self, x):
        return self.probe(x)


def load_benchmark_model(params):
    """Loads the benchmark model, freezing the large payload."""
    import transformers

    model_id = params.model_id
    use_local_files_only = model_id.startswith("gs://")
    resolved_model = model_dir(model_id)

    payload = transformers.AutoModelForCausalLM.from_pretrained(
        resolved_model,
        dtype=torch.bfloat16,
        local_files_only=use_local_files_only,
        use_cache=False,
    )

    for p in payload.parameters():
        p.requires_grad = False

    probe = torch.nn.Linear(8, 8, dtype=torch.bfloat16)
    probe.weight.requires_grad = True

    return BenchmarkModel(payload=payload, probe=probe)


def materialize_adamw_states(optimizer):
    """Eagerly allocate AdamW moments so checkpoint size is realistic."""
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


def parallelize_model(model, params, device_type: str = "cpu"):
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

    mp_policy = MixedPrecisionPolicy(
        param_dtype=torch.bfloat16,
        reduce_dtype=torch.bfloat16,
    )

    if strategy in ("fsdp_sharded", "fsdp_full"):
        mesh = init_device_mesh(device_type, (params.world_size,))
        for layer in model.payload.model.layers:
            fully_shard(layer, mesh=mesh, mp_policy=mp_policy)
        fully_shard(model.payload, mesh=mesh, mp_policy=mp_policy)
        fully_shard(model.probe, mesh=mesh, mp_policy=mp_policy)
        return model

    if strategy in ("model_parallel_sharded", "model_parallel_full"):
        mesh = init_device_mesh(
            device_type,
            (params.data_parallel_size, params.tensor_parallel_size),
            mesh_dim_names=("dp", "tp"),
        )
        dp_mesh = mesh["dp"]
        tp_mesh = mesh["tp"]

        if params.tensor_parallel_size > 1:
            apply_llama_tp(model.payload, tp_mesh)
        for layer in model.payload.model.layers:
            fully_shard(layer, mesh=dp_mesh, mp_policy=mp_policy)
        fully_shard(model.payload, mesh=dp_mesh, mp_policy=mp_policy)
        fully_shard(model.probe, mesh=dp_mesh, mp_policy=mp_policy)
        return model

    raise ValueError(f"Unknown strategy: {strategy}")


def setup_model_and_optimizer(params, device: torch.device | None = None):
    """Loads, moves to device, parallelizes the benchmark model, and materializes AdamW states."""
    model = load_benchmark_model(params)
    if device is not None:
        model = model.to(device)
    device_type = device.type if device is not None else "cpu"
    model = parallelize_model(model, params, device_type)
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


def _ram_staging_root(min_free_gb: float = 50.0) -> str | None:
    """Return a writable RAM-backed directory with ``min_free_gb`` free, else None."""
    for candidate in ("/mnt/ramdisk", "/dev/shm"):
        if os.path.isdir(candidate) and os.access(candidate, os.W_OK):
            try:
                stat = os.statvfs(candidate)
                avail_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
                if avail_gb >= min_free_gb:
                    return candidate
            except Exception as e:
                logging.warning(
                    "Could not use staging directory candidate %s: %s",
                    candidate,
                    e,
                )
    return None


def _get_staging_dir(prefix: str, min_free_gb: float = 50.0) -> str:
    """Creates a temporary staging directory, prioritizing /dev/shm or /mnt/ramdisk to avoid root disk exhaustion."""
    return tempfile.mkdtemp(prefix=prefix, dir=_ram_staging_root(min_free_gb))


def stage_checkpoint_locally(
    model,
    optimizer,
    params,
    rank: int,
    staging_prefix: str = "ray-ckpt",
):
    """Gathers the state dict and writes it to a local staging directory."""
    is_sharded = is_sharded_strategy(params.strategy)
    options = StateDictOptions(
        full_state_dict=not is_sharded,
        cpu_offload=not is_sharded,
    )

    local_dir = None
    model_state, opt_state = get_state_dict(model, optimizer, options=options)
    app_state = {"model": model_state, "optimizer": opt_state}
    try:
        if is_sharded:
            local_dir = _get_staging_dir(f"{staging_prefix}-rank{rank}-")
            dcp.save(
                {"app": app_state},
                storage_writer=dcp.FileSystemWriter(local_dir),
            )
        elif rank == 0:
            local_dir = _get_staging_dir(f"{staging_prefix}-rank0-")
            ckpt_file = os.path.join(local_dir, "checkpoint.pt")
            torch.save(app_state, ckpt_file)
    finally:
        del app_state, model_state, opt_state
    return local_dir


def upload_checkpoint(
    local_dir,
    params,
    rank: int,
    arrow_fs,
    fs,
    destination_ckpt: str,
):
    """Copies the staged checkpoint to storage."""
    if local_dir is not None:
        _pyarrow_fs_copy_files(
            local_dir,
            destination_ckpt,
            destination_filesystem=arrow_fs,
        )
    dist.barrier()
    if is_sharded_strategy(params.strategy) and rank == 0:
        fs.touch(f"{destination_ckpt.rstrip('/')}/_SUCCESS")
