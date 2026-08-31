"""Ray Data + Ray Train Llama macrobenchmark.

GPU workers execute real Llama training. CPU workers preserve the same data,
parallelism, optimizer, and checkpoint paths while replacing the expensive
decoder forward with a small token-derived probe.
"""

from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import logging
import math
import os
import pickle
import posixpath
import random
import shutil
import sys
import tempfile
import time
from pathlib import Path

import fsspec
import metric_logging
import numpy
import pyarrow
import pyarrow.fs

import gcsfs

os.environ.setdefault("RAY_TRAIN_V2_ENABLED", "1")

import ray
import ray.train
import ray.train.torch
import torch
from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2._internal.execution.storage import (
    _exists_at_fs_path,
    _pyarrow_fs_copy_files,
)

SUPPORTED_STRATEGIES = frozenset(
    {
        "ddp",
        "fsdp_sharded",
        "fsdp_full",
        "model_parallel_sharded",
        "model_parallel_full",
    }
)

_METRIC_LOGGER = None


def _get_metric_logger():
    global _METRIC_LOGGER
    if _METRIC_LOGGER is None:
        logger = logging.getLogger("gcsfs_benchmark.metrics")
        logger.handlers.clear()
        logger.addHandler(metric_logging.AtomicContainerHandler())
        logger.setLevel(logging.INFO)
        logger.propagate = False
        _METRIC_LOGGER = logger
    return _METRIC_LOGGER


def _emit_metric(*, event, **fields):
    for name, value in fields.items():
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"non-finite metric field {name}: {value}")
    metric_logging.emit_metric(_get_metric_logger(), event=event, **fields)


def _checkpoint_path(checkpoint) -> str:
    path = getattr(checkpoint, "path", None)
    if not isinstance(path, str) or not path:
        raise RuntimeError("Ray checkpoint observation is missing checkpoint.path")
    return path


def _checkpoint_step_from_dir_name(dir_name) -> int:
    """Recover the optimizer step from a ``step-<n>.ckpt`` directory name."""
    stem = str(dir_name).rstrip("/").rsplit("/", 1)[-1].removesuffix(".ckpt")
    step = stem.removeprefix("step-") if stem.startswith("step-") else ""
    if not step.isdigit():
        raise RuntimeError(f"unparseable checkpoint directory name: {dir_name!r}")
    return int(step)


class BenchmarkMetricsCallback(ray.train.UserCallback):
    """Emit the checkpoint size once every worker has reported a checkpoint.

    Runs on the Ray Train controller. The commit duration is measured in the
    worker instead -- ``after_report`` is dispatched from the controller's
    polling loop, so its arrival time is quantized to the health-check
    interval and cannot time a transfer.
    """

    def __init__(self):
        self._emitted = set()

    def after_report(self, run_context, metrics, checkpoint):
        if checkpoint is None:
            return
        path = _checkpoint_path(checkpoint)
        if path in self._emitted:
            return
        if not metrics:
            raise RuntimeError(f"checkpoint {path} has no worker report metrics")
        steps = [worker.get("gcsfs_checkpoint_step") for worker in metrics]
        if (
            any(
                isinstance(step, bool) or not isinstance(step, int) or step < 0
                for step in steps
            )
            or len(set(steps)) != 1
        ):
            raise RuntimeError(
                f"all workers must report the same checkpoint step for {path}: {steps}"
            )
        sizes = [worker.get("gcsfs_checkpoint_size_bytes") for worker in metrics]
        if any(
            isinstance(size, bool) or not isinstance(size, int) or size < 0
            for size in sizes
        ):
            raise RuntimeError(
                f"all workers must report a nonnegative checkpoint size for {path}"
            )
        step = steps[0]
        _emit_metric(
            event="checkpoint_size",
            checkpoint_location=path,
            checkpoint_step=step,
            size_bytes=sum(sizes),
        )
        self._emitted.add(path)


class BenchmarkControllerCallback(ControllerCallback):
    """Install the checkpoint-delete probe inside the controller actor.

    Retention deletion runs on the controller, which is a separate Ray actor,
    so a probe installed on the driver would never see it. ``after_controller_start``
    runs in that actor before the control loop, and therefore before any
    checkpoint exists to delete.
    """

    def after_controller_start(self, train_run_context):
        install_checkpoint_delete_probe()


_DELETE_PROBE_ORIGINAL = "_gcsfs_benchmark_original_delete_fs_path"


def _checkpoint_manager_module():
    from ray.train.v2._internal.execution.checkpoint import checkpoint_manager

    return checkpoint_manager


def install_checkpoint_delete_probe():
    """Time Ray's own retention deletes by wrapping ``delete_fs_path``.

    Ray resolves ``delete_fs_path`` through its module global, so rebinding that
    name intercepts the call without touching Ray's source. Upstream swallows
    every deletion exception, so success is re-derived from whether the path
    survived rather than taken on faith.
    """
    module = _checkpoint_manager_module()
    original = getattr(module, _DELETE_PROBE_ORIGINAL, None)
    if original is None:
        original = module.delete_fs_path
        setattr(module, _DELETE_PROBE_ORIGINAL, original)

    def timed_delete(fs, fs_path):
        started = time.perf_counter()
        try:
            original(fs=fs, fs_path=fs_path)
        finally:
            duration_s = time.perf_counter() - started
        succeeded = not _exists_at_fs_path(fs, fs_path)
        step = _checkpoint_step_from_dir_name(fs_path)
        _emit_metric(
            event="checkpoint_deleted",
            checkpoint_location=fs_path,
            checkpoint_step=step,
            duration_s=duration_s,
            success=succeeded,
        )
        if not succeeded:
            raise RuntimeError(f"Ray failed to delete checkpoint {fs_path}")

    module.delete_fs_path = timed_delete


def make_checkpoint_upload_fn(*, destination_root, storage_filesystem, global_rank):
    """Build the ``ray.train.report`` upload hook that times the transfer.

    Mirrors ``StorageContext.persist_current_checkpoint`` -- create the
    destination directory, then copy with Ray's own ``_pyarrow_fs_copy_files``
    -- so the measured work is the upload Ray would otherwise have done. The
    checkpoint directory name arrives as an argument, so each event is
    attributed to its own step with no shared state between the training thread
    and the background upload thread.
    """

    def upload(checkpoint, checkpoint_dir_name):
        step = _checkpoint_step_from_dir_name(checkpoint_dir_name)
        destination = f"{destination_root}/{checkpoint_dir_name}"
        start_time_s = time.time()
        started = time.perf_counter()
        storage_filesystem.create_dir(destination)
        _pyarrow_fs_copy_files(
            source=checkpoint.path,
            destination=destination,
            source_filesystem=checkpoint.filesystem,
            destination_filesystem=storage_filesystem,
        )
        duration_s = time.perf_counter() - started
        _emit_metric(
            event="checkpoint_committed",
            global_rank=global_rank,
            checkpoint_location=destination,
            checkpoint_step=step,
            start_time_s=start_time_s,
            duration_s=duration_s,
        )
        return ray.train.Checkpoint(path=destination, filesystem=storage_filesystem)

    return upload


def emit_ray_data_iteration(dataset_shard, *, config, rank, total_s):
    """Emit the run's Ray Data snapshot without masking an in-flight error.

    Called from the training loop's ``finally``. A metrics failure must fail the
    run when training itself succeeded, but must never replace the exception
    that ended training -- that one is the error worth reading.
    """
    training_failed = sys.exc_info()[0] is not None
    split_index = (
        rank // config.tensor_parallel_size if config.is_model_parallel else rank
    )
    try:
        metrics = read_iteration_metrics(dataset_shard, total_s=total_s)
    except Exception:
        if not training_failed:
            raise
        logging.exception("failed to read Ray Data iteration metrics")
        return
    _emit_metric(
        event="ray_data_iteration",
        global_rank=rank,
        split_index=split_index,
        **metrics,
    )


def _require_ray_data_seconds(value, *, name) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise RuntimeError(f"invalid Ray Data metric {name}: {value}")
    return float(value)


def read_iteration_metrics(dataset_shard, *, total_s):
    """Read the iterator's cumulative Ray Data timers.

    Published Ray records these on ``DatasetStats`` but exposes them only
    through a formatted string, so the stats object is read directly. The
    timers accumulate across epochs, so this is read once per run rather than
    summed per epoch. ``total_s`` comes from the caller's wall clock because
    Ray's ``iter_total_s`` is never recorded when iteration ends in a ``break``,
    which is how the step budget terminates this workload.
    """
    stats = getattr(dataset_shard, "_iter_stats", None)
    if stats is None:
        raise RuntimeError(
            "Ray Data iterator does not expose _iter_stats; "
            "the installed Ray is not the supported baseline"
        )
    blocked_timer = stats.iter_total_blocked_s
    total_blocked_s = _require_ray_data_seconds(
        blocked_timer.get(), name="total_blocked_s"
    )
    time_to_first_batch_s = _require_ray_data_seconds(
        stats.iter_time_to_first_batch_s.get(), name="time_to_first_batch_s"
    )
    lifetime_s = _require_ray_data_seconds(total_s, name="total_s")
    blocked_calls = blocked_timer._total_count
    if (
        isinstance(blocked_calls, bool)
        or not isinstance(blocked_calls, (int, float))
        or not math.isfinite(blocked_calls)
        or blocked_calls < 0
        or int(blocked_calls) != blocked_calls
    ):
        raise RuntimeError(f"invalid Ray Data metric blocked_calls: {blocked_calls}")
    if total_blocked_s > lifetime_s + 1e-6:
        raise RuntimeError(
            f"Ray Data blocked time {total_blocked_s} exceeds the iteration "
            f"lifetime {lifetime_s}"
        )
    return {
        "total_blocked_s": total_blocked_s,
        "total_s": lifetime_s,
        "time_to_first_batch_s": time_to_first_batch_s,
        "blocked_calls": int(blocked_calls),
    }


def _directory_size_bytes(directory) -> int:
    if directory is None:
        return 0
    return sum(
        path.stat().st_size for path in Path(directory).rglob("*") if path.is_file()
    )


def pack_token_documents(
    documents: list[list[int]],
    *,
    pending: list[int],
    eos_token_id: int,
    sequence_length: int,
) -> tuple[numpy.ndarray, list[int]]:
    tokens = list(pending)
    for document in documents:
        tokens.extend(document)
        tokens.append(eos_token_id)
    complete_rows = len(tokens) // sequence_length
    used_tokens = complete_rows * sequence_length
    rows = numpy.asarray(tokens[:used_tokens], dtype=numpy.int64).reshape(
        complete_rows, sequence_length
    )
    return rows, tokens[used_tokens:]


class TokenizeAndPack:
    def __init__(self, model_path, sequence_length, tokenizer=None):
        if tokenizer is None:
            from transformers import AutoTokenizer

            tokenizer = AutoTokenizer.from_pretrained(
                model_path,
                local_files_only=True,
            )
        if tokenizer.eos_token_id is None:
            raise ValueError("the tokenizer must define eos_token_id")
        self.tokenizer = tokenizer
        self.sequence_length = sequence_length
        self.pending: list[int] = []

    def __call__(self, batch: pyarrow.Table) -> dict[str, numpy.ndarray]:
        documents = self.tokenizer(
            batch.column("text").to_pylist(),
            add_special_tokens=False,
            padding=False,
            truncation=False,
        )["input_ids"]
        rows, self.pending = pack_token_documents(
            documents,
            pending=self.pending,
            eos_token_id=self.tokenizer.eos_token_id,
            sequence_length=self.sequence_length,
        )
        return {
            "input_ids": rows,
            "attention_mask": numpy.ones_like(rows),
            "labels": rows.copy(),
        }


def resolve_parquet_source(
    uri: str,
) -> tuple[object, pyarrow.fs.PyFileSystem, list[str]]:
    fs, base_path = fsspec.core.url_to_fs(uri)
    if uri.startswith("gs://"):
        if not isinstance(fs, gcsfs.GCSFileSystem):
            raise RuntimeError(
                f"gs:// dataset did not resolve through gcsfs: {type(fs)!r}"
            )
    paths = sorted(fs.glob(f"{base_path.rstrip('/')}/*.parquet"))
    if not paths:
        raise FileNotFoundError(f"no Parquet files under {uri}")
    arrow_fs = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(fs))
    return fs, arrow_fs, paths


def build_train_dataset(config: "WorkloadConfig", tokenizer_factory=None):
    filesystem, arrow_fs, paths = resolve_parquet_source(config.dataset_path)
    logging.info(
        "Ray Data source filesystems: fsspec=%s pyarrow=%s",
        type(filesystem).__name__,
        type(arrow_fs).__name__,
    )
    file_shuffle = (
        ray.data.FileShuffleConfig(seed=42, reseed_after_execution=True)
        if config.shuffle_files
        else None
    )
    # Read concurrency is the GCS parallelism under test, so it is left to Ray's
    # resource-based sizing unless READ_CONCURRENCY names a deliberate cap. It
    # must never be derived from an unrelated knob: capping it silently makes the
    # benchmark measure its own task limit instead of gcsfs.
    dataset = ray.data.read_parquet(
        paths,
        filesystem=arrow_fs,
        columns=["text"],
        shuffle=file_shuffle,
        concurrency=(
            min(config.read_concurrency, len(paths))
            if config.read_concurrency
            else None
        ),
    )
    tokenizer = tokenizer_factory() if tokenizer_factory else None
    # DATALOADER_NUM_WORKERS sizes the tokenization actor pool, which holds one
    # CPU per actor. Size it so tokenization keeps ahead of the reads; too small
    # a pool shows up as Ray Data blocked time that is CPU, not storage.
    dataset = dataset.map_batches(
        TokenizeAndPack,
        batch_size=config.tokenize_batch_size,
        batch_format="pyarrow",
        compute=ray.data.ActorPoolStrategy(
            size=config.dataloader_workers,
            max_tasks_in_flight_per_actor=2,
        ),
        fn_constructor_kwargs={
            "model_path": config.local_model_path,
            "sequence_length": config.sequence_length,
            "tokenizer": tokenizer,
        },
        num_cpus=1,
    )
    return dataset


def dp_data_assignments(world_size: int, tp_size: int) -> list[int | None]:
    if tp_size <= 0:
        raise ValueError("TP size must be positive")
    if world_size % tp_size:
        raise ValueError("world size must be divisible by TP size")
    return [
        rank // tp_size if rank % tp_size == 0 else None for rank in range(world_size)
    ]


class DPAwareDataConfig(ray.train.DataConfig):
    """Split by data-parallel replica and leave TP followers non-consuming."""

    def __init__(self, tp_size: int):
        # Shard locality off, so Ray Data's OutputSplitter gets no node hints.
        # With hints it defers dispatching to a split until a block on that
        # split's node shows up in its buffer, and when several DP splits share
        # a node (FSDP, or any run with more than one rank per node) that wait
        # can stall the split for as long as the buffer stays node-mismatched.
        super().__init__(datasets_to_split="all", enable_shard_locality=False)
        self.tp_size = tp_size

    def configure(
        self,
        datasets,
        world_size,
        worker_handles,
        worker_node_ids,
        **kwargs,
    ):
        # Ray Train is asked to split for the data-parallel leaders only, and
        # its own DataConfig does the splitting. Reimplementing it here loses
        # the pieces that come with it: streaming_split(equal=True), the
        # execution options for the run, the exclude_resources reservation
        # that keeps Ray Data from booking the CPUs the Train workers hold,
        # and whatever Ray adds to configure() next.
        assignments = dp_data_assignments(world_size, self.tp_size)
        leader_ranks = [
            rank
            for rank, split_index in enumerate(assignments)
            if split_index is not None
        ]
        leader_outputs = super().configure(
            datasets,
            len(leader_ranks),
            (
                [worker_handles[rank] for rank in leader_ranks]
                if worker_handles is not None
                else None
            ),
            (
                [worker_node_ids[rank] for rank in leader_ranks]
                if worker_node_ids is not None
                else None
            ),
            **kwargs,
        )
        empty_output = {
            name: dataset.limit(0).iterator() for name, dataset in datasets.items()
        }
        return [
            (
                leader_outputs[split_index]
                if split_index is not None
                else dict(empty_output)
            )
            for split_index in assignments
        ]


def _validate_train_batch(batch, *, config, device):
    expected_shape = (config.per_device_batch_size, config.sequence_length)
    result = {}
    for name in ("input_ids", "attention_mask", "labels"):
        tensor = batch[name]
        if not isinstance(tensor, torch.Tensor):
            raise TypeError(f"{name} must be a Ray Data Torch tensor")
        if tensor.dtype != torch.int64:
            raise TypeError(f"{name} has dtype {tensor.dtype}, expected torch.int64")
        if tuple(tensor.shape) != expected_shape:
            raise ValueError(
                f"{name} has shape {tuple(tensor.shape)}, expected {expected_shape}"
            )
        if tensor.device != device:
            raise ValueError(f"{name} is on {tensor.device}, expected {device}")
        result[name] = tensor if tensor.is_contiguous() else tensor.contiguous()
    return result


def iter_train_batches(dataset_shard, *, config, device, epoch):
    shuffle_buffer_size = config.shuffle_buffer_size or None
    shuffle_seed = 42 + epoch if shuffle_buffer_size is not None else None
    return iter(
        dataset_shard.iter_torch_batches(
            batch_size=config.per_device_batch_size,
            prefetch_batches=config.dataloader_prefetch_factor,
            collate_fn=None,
            drop_last=True,
            local_shuffle_buffer_size=shuffle_buffer_size,
            local_shuffle_seed=shuffle_seed,
            pin_memory=device.type == "cuda",
            device=device,
        )
    )


def next_train_batch(iterator, *, config, device, tp_mesh):
    """Fetch one DP batch and replicate it within its tensor-parallel group.

    Nothing here waits on a data-parallel collective. Ray Data hands every
    data-parallel leader the same number of rows, so a fixed batch size and
    ``drop_last=True`` give every leader the same number of batches and they
    reach the end of the epoch together. Agreeing on end-of-data through a
    world-wide reduce instead would stop each rank from reading its own split
    until the slowest rank had read its own, which is exactly what
    ``streaming_split`` forbids: one late block then parks every other rank
    inside the collective until the torch timeout kills the run, and the
    traceback points at the collective instead of at the late split.
    """

    if tp_mesh is None:
        return _validate_train_batch(next(iterator), config=config, device=device)

    tp_group = tp_mesh.get_group()
    source_rank = torch.distributed.get_process_group_ranks(tp_group)[0]
    is_source = torch.distributed.get_rank() == source_rank
    # A tensor-parallel follower never reads the stream, so the source has to
    # tell it whether a batch exists at all. That exchange stays inside the
    # tensor-parallel group, whose ranks are already in lockstep for every
    # other tensor-parallel collective.
    status = torch.ones(1, dtype=torch.int8, device=device)
    source_error = None
    batch = None
    if is_source:
        try:
            batch = _validate_train_batch(next(iterator), config=config, device=device)
        except StopIteration:
            status.zero_()
        except Exception as error:  # The status broadcast must run before raising.
            source_error = error
            status.fill_(-1)

    torch.distributed.broadcast(status, src=source_rank, group=tp_group)

    batch_status = status.item()
    if batch_status == 0:
        raise StopIteration
    if batch_status == -1:
        message = f"TP batch source rank {source_rank} failed"
        if source_error is not None:
            raise RuntimeError(message) from source_error
        raise RuntimeError(message)

    if not is_source:
        shape = (config.per_device_batch_size, config.sequence_length)
        batch = {
            name: torch.empty(shape, dtype=torch.int64, device=device)
            for name in ("input_ids", "attention_mask", "labels")
        }
    for name in ("input_ids", "attention_mask", "labels"):
        torch.distributed.broadcast(
            batch[name],
            src=source_rank,
            group=tp_group,
        )
    return batch


class CausalProbe(torch.nn.Module):
    """Small trainable CPU path whose loss still depends on streamed tokens."""

    def __init__(self):
        super().__init__()
        self.up_proj = torch.nn.Linear(8, 32, bias=False)
        self.down_proj = torch.nn.Linear(32, 8, bias=False)

    def forward(self, batch):
        features = (batch["input_ids"][:, :8] % 1024).float() / 1024.0
        targets = (batch["input_ids"][:, 8] % 8).long()
        logits = self.down_proj(torch.nn.functional.silu(self.up_proj(features)))
        return torch.nn.functional.cross_entropy(logits, targets)


class BenchmarkModel(torch.nn.Module):
    def __init__(self, payload, probe, use_gpu):
        super().__init__()
        self.payload = payload
        self.probe = probe
        self.use_gpu = use_gpu

    def forward(self, batch):
        if self.use_gpu:
            return self.payload(**batch).loss
        return self.probe(batch)


def load_benchmark_model(config, *, use_gpu, mp_dtype):
    from transformers import AutoModelForCausalLM

    payload = AutoModelForCausalLM.from_pretrained(
        config.local_model_path,
        local_files_only=True,
        low_cpu_mem_usage=True,
        use_safetensors=True,
        attn_implementation="sdpa",
        dtype=mp_dtype,
    )
    payload.config.use_cache = False
    probe = CausalProbe()
    if use_gpu:
        payload.gradient_checkpointing_enable(
            gradient_checkpointing_kwargs={"use_reentrant": False}
        )
        payload.enable_input_require_grads()
        probe.requires_grad_(False)
    else:
        payload.requires_grad_(False)
    return BenchmarkModel(payload, probe, use_gpu)


def validate_tp_node_ids(node_ids, *, tp_size):
    if tp_size <= 0 or len(node_ids) % tp_size:
        raise ValueError("node ID count must be divisible by TP size")
    for first_rank in range(0, len(node_ids), tp_size):
        group_node_ids = node_ids[first_rank : first_rank + tp_size]
        if len(set(group_node_ids)) != 1:
            ranks = list(range(first_rank, first_rank + tp_size))
            raise ValueError(
                "TP group must stay on one node: "
                f"ranks={ranks}, node_ids={group_node_ids}"
            )


def gather_and_validate_placement(config):
    context = ray.train.get_context()
    node_ranks = [None] * context.get_world_size()
    torch.distributed.all_gather_object(node_ranks, context.get_node_rank())
    if config.is_model_parallel:
        validate_tp_node_ids(node_ranks, tp_size=config.tensor_parallel_size)
    if context.get_world_rank() == 0:
        placements = [
            {
                "rank": rank,
                "node_rank": node_rank,
                "dp_rank": (
                    rank // config.tensor_parallel_size
                    if config.is_model_parallel
                    else rank
                ),
                "tp_rank": (
                    rank % config.tensor_parallel_size
                    if config.is_model_parallel
                    else 0
                ),
            }
            for rank, node_rank in enumerate(node_ranks)
        ]
        logging.info("Ray Train rank placement: %s", placements)
    return node_ranks


def validate_llama_tp_dimensions(model_config, *, tp_size):
    dimensions = {
        "hidden_size": model_config.hidden_size,
        "intermediate_size": model_config.intermediate_size,
        "num_attention_heads": model_config.num_attention_heads,
        "num_key_value_heads": model_config.num_key_value_heads,
    }
    for name, value in dimensions.items():
        if value % tp_size:
            raise ValueError(f"{name}={value} must be divisible by TP size={tp_size}")


def llama_tp_plan():
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


def _fully_shard_bottom_up(model, *, dp_mesh, mp_policy):
    from torch.distributed.fsdp import fully_shard

    for layer in model.payload.model.layers:
        fully_shard(
            layer,
            mesh=dp_mesh,
            mp_policy=mp_policy,
            reshard_after_forward=True,
        )
    fully_shard(
        model.payload,
        mesh=dp_mesh,
        mp_policy=mp_policy,
        reshard_after_forward=True,
    )
    fully_shard(
        model.probe,
        mesh=dp_mesh,
        mp_policy=mp_policy,
        reshard_after_forward=True,
    )


def parallelize_benchmark_model(model, config, *, device, mp_dtype):
    """Move and wrap a model, returning it with its DP and TP meshes."""
    from torch.distributed.device_mesh import init_device_mesh
    from torch.distributed.fsdp import MixedPrecisionPolicy
    from torch.distributed.tensor.parallel import parallelize_module

    if config.strategy == "ddp":
        model = ray.train.torch.prepare_model(
            model,
            move_to_device=device,
            parallel_strategy="ddp",
        )
        return model, None, None

    model.to(device)
    mp_policy = MixedPrecisionPolicy(
        param_dtype=mp_dtype,
        reduce_dtype=mp_dtype,
    )
    if config.is_model_parallel:
        validate_llama_tp_dimensions(
            model.payload.config,
            tp_size=config.tensor_parallel_size,
        )
        mesh = init_device_mesh(
            device.type,
            (config.data_parallel_size, config.tensor_parallel_size),
            mesh_dim_names=("dp", "tp"),
        )
        dp_mesh = mesh["dp"]
        tp_mesh = mesh["tp"]
        for layer in model.payload.model.layers:
            parallelize_module(layer, tp_mesh, llama_tp_plan())
    else:
        dp_mesh = init_device_mesh(device.type, (config.world_size,))
        tp_mesh = None

    _fully_shard_bottom_up(model, dp_mesh=dp_mesh, mp_policy=mp_policy)
    return model, dp_mesh, tp_mesh


@dataclasses.dataclass(frozen=True)
class PrecisionPolicy:
    model_dtype: torch.dtype
    compute_dtype: torch.dtype
    scaler: object | None


def select_precision(*, use_gpu):
    if not use_gpu:
        return PrecisionPolicy(
            model_dtype=torch.bfloat16,
            compute_dtype=torch.float32,
            scaler=None,
        )

    torch.set_float32_matmul_precision("high")
    if torch.cuda.is_bf16_supported():
        return PrecisionPolicy(
            model_dtype=torch.bfloat16,
            compute_dtype=torch.bfloat16,
            scaler=None,
        )
    return PrecisionPolicy(
        model_dtype=torch.float16,
        compute_dtype=torch.float16,
        scaler=torch.amp.GradScaler("cuda"),
    )


def _materialize_frozen_adamw_state(optimizer):
    for group in optimizer.param_groups:
        for parameter in group["params"]:
            if parameter.requires_grad:
                continue
            state = optimizer.state[parameter]
            state["step"] = torch.tensor(1.0)
            epsilon = torch.finfo(parameter.dtype).eps
            state["exp_avg"] = torch.ones_like(parameter) * epsilon
            state["exp_avg_sq"] = torch.ones_like(parameter) * epsilon


def _initialize_missing_optimizer_state_for_restore(optimizer):
    missing = {
        parameter
        for group in optimizer.param_groups
        for parameter in group["params"]
        if parameter.requires_grad and not optimizer.state.get(parameter)
    }
    if not missing:
        return

    original_lrs = [group["lr"] for group in optimizer.param_groups]
    original_grads = {
        parameter: parameter.grad
        for group in optimizer.param_groups
        for parameter in group["params"]
    }
    try:
        for group in optimizer.param_groups:
            lr = group["lr"]
            group["lr"] = torch.zeros_like(lr) if torch.is_tensor(lr) else 0.0
            for parameter in group["params"]:
                parameter.grad = (
                    torch.zeros_like(parameter) if parameter in missing else None
                )
        optimizer.step()
    finally:
        for group, lr in zip(optimizer.param_groups, original_lrs):
            group["lr"] = lr
        for parameter, grad in original_grads.items():
            parameter.grad = grad


def build_optimizer(model, config, *, use_gpu):
    optimizer_options = {
        "lr": config.learning_rate,
        "weight_decay": config.weight_decay,
    }
    if config.is_model_parallel:
        optimizer_options.update(foreach=False, fused=False)
    elif use_gpu and torch.cuda.is_available():
        optimizer_options["fused"] = True
    optimizer = torch.optim.AdamW(model.parameters(), **optimizer_options)
    if not use_gpu:
        _materialize_frozen_adamw_state(optimizer)
    return optimizer


def build_scheduler(optimizer, *, total_steps, warmup_ratio):
    warmup_steps = int(total_steps * warmup_ratio)

    def lr_scale(step):
        if warmup_steps and step < warmup_steps:
            return float(step + 1) / warmup_steps
        decay_steps = max(1, total_steps - warmup_steps)
        progress = min(1.0, (step - warmup_steps) / decay_steps)
        return 0.5 * (1.0 + math.cos(math.pi * progress))

    return torch.optim.lr_scheduler.LambdaLR(optimizer, lr_scale)


def active_training_module(model, config, *, use_gpu):
    if config.strategy == "ddp":
        return model
    return model.payload if use_gpu else model.probe


def trainable_parameters(model):
    return tuple(
        parameter for parameter in model.parameters() if parameter.requires_grad
    )


@contextlib.contextmanager
def gradient_sync_context(model, *, synchronize):
    if synchronize:
        yield
        return
    if hasattr(model, "no_sync"):
        with model.no_sync():
            yield
        return
    if hasattr(model, "set_requires_gradient_sync"):
        model.set_requires_gradient_sync(False)
        try:
            yield
        finally:
            model.set_requires_gradient_sync(True)
        return
    yield


def _checkpoint_storage(uri):
    filesystem, path = fsspec.core.url_to_fs(uri)
    if uri.startswith(("gs://", "gcs://")) and not isinstance(
        filesystem, gcsfs.GCSFileSystem
    ):
        raise RuntimeError(
            f"GCS checkpoint did not resolve through gcsfs: {type(filesystem)!r}"
        )
    arrow_filesystem = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(filesystem))
    return filesystem, arrow_filesystem, path


def _checkpoint_run_path(checkpoint_root, run_id):
    if not run_id or run_id in {".", ".."} or "/" in run_id or "\\" in run_id:
        raise ValueError("RUN_ID must be one safe path component")
    root = checkpoint_root.rstrip("/")
    target = posixpath.join(root, run_id)
    if not root or target == root or not target.startswith(f"{root}/"):
        raise ValueError("RUN_ID does not produce a strict checkpoint child")
    return target


def prepare_checkpoint_run(config):
    if config.checkpoint_load_path and config.overwrite_existing_run:
        raise ValueError(
            "OVERWRITE_EXISTING_RUN cannot be enabled for a checkpoint load"
        )
    if not config.checkpoint_write_path:
        return

    filesystem, _, checkpoint_root = _checkpoint_storage(config.checkpoint_write_path)
    target = _checkpoint_run_path(checkpoint_root, config.run_id)
    if config.checkpoint_load_path or not filesystem.exists(target):
        return
    if not config.overwrite_existing_run:
        raise FileExistsError(
            f"checkpoint run already exists: {target}; set "
            "OVERWRITE_EXISTING_RUN=true to replace it"
        )
    filesystem.rm(target, recursive=True)
    if filesystem.exists(target):
        raise RuntimeError(f"failed to remove existing checkpoint run: {target}")


def _ray_checkpoint_from_uri(uri):
    _, arrow_filesystem, path = _checkpoint_storage(uri)
    return ray.train.Checkpoint(path=path, filesystem=arrow_filesystem)


CHECKPOINT_SCHEMA_VERSION = 1
MODEL_CONFIG_FILES = ("config.json",)
TOKENIZER_FILES = (
    "added_tokens.json",
    "merges.txt",
    "special_tokens_map.json",
    "tokenizer.json",
    "tokenizer.model",
    "tokenizer_config.json",
    "vocab.json",
)
TOKENIZER_VOCAB_FILES = frozenset({"tokenizer.json", "tokenizer.model", "vocab.json"})


def _hash_named_files(root: Path, names: tuple[str, ...]):
    digest = hashlib.sha256()
    found = set()
    for name in sorted(names):
        path = root / name
        if not path.is_file():
            continue
        name_bytes = name.encode("utf-8")
        contents = path.read_bytes()
        digest.update(len(name_bytes).to_bytes(4, "big"))
        digest.update(name_bytes)
        digest.update(len(contents).to_bytes(8, "big"))
        digest.update(contents)
        found.add(name)
    return digest.hexdigest(), found


def model_artifact_fingerprints(local_model_path):
    root = Path(local_model_path)
    if not (root / "config.json").is_file():
        raise FileNotFoundError(f"missing model contract file: {root / 'config.json'}")
    model_hash, _ = _hash_named_files(root, MODEL_CONFIG_FILES)
    tokenizer_hash, tokenizer_files = _hash_named_files(root, TOKENIZER_FILES)
    if not tokenizer_files.intersection(TOKENIZER_VOCAB_FILES):
        expected = ", ".join(sorted(TOKENIZER_VOCAB_FILES))
        raise FileNotFoundError(
            f"missing tokenizer vocabulary under {root}; expected one of {expected}"
        )
    return model_hash, tokenizer_hash


def checkpoint_metadata(config):
    model_hash, tokenizer_hash = model_artifact_fingerprints(config.local_model_path)
    return {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "checkpoint_format": config.checkpoint_format,
        "strategy": config.strategy,
        "world_size": config.world_size,
        "tp_size": config.tensor_parallel_size if config.is_model_parallel else 1,
        "dp_size": config.dp_size,
        "model_config_sha256": model_hash,
        "tokenizer_sha256": tokenizer_hash,
    }


def validate_checkpoint_metadata(saved_metadata, expected_metadata):
    if not isinstance(saved_metadata, dict):
        raise ValueError("checkpoint metadata must be a mapping")
    for name, expected_value in expected_metadata.items():
        saved_value = saved_metadata.get(name)
        if saved_value != expected_value:
            raise ValueError(
                f"checkpoint {name} mismatch: saved={saved_value!r}, "
                f"expected={expected_value!r}"
            )


def _capture_rng_state():
    return {
        "torch": torch.get_rng_state(),
        "cuda": torch.cuda.get_rng_state_all() if torch.cuda.is_available() else [],
        "numpy": pickle.dumps(numpy.random.get_state()),
        "python": pickle.dumps(random.getstate()),
    }


def _restore_rng_state(state):
    torch.set_rng_state(state["torch"].cpu())
    if state["cuda"] and torch.cuda.is_available():
        torch.cuda.set_rng_state_all(state["cuda"])
    numpy.random.set_state(pickle.loads(state["numpy"]))
    random.setstate(pickle.loads(state["python"]))


def _frozen_optimizer_state_from_canonical(model, optimizer_state):
    saved_states = optimizer_state.get("state", {})
    frozen_state = {}
    for name, parameter in model.named_parameters():
        optimizer_name = name
        if name not in saved_states and name.startswith("module."):
            optimizer_name = name.removeprefix("module.")
        if parameter.requires_grad or optimizer_name not in saved_states:
            continue
        frozen_state[optimizer_name] = dict(saved_states[optimizer_name])
    return frozen_state


def _load_frozen_optimizer_state(model, optimizer, frozen_state):
    from torch.distributed.tensor import DTensor, distribute_tensor

    for name, parameter in model.named_parameters():
        optimizer_name = name
        if name not in frozen_state and name.startswith("module."):
            optimizer_name = name.removeprefix("module.")
        if parameter.requires_grad or optimizer_name not in frozen_state:
            continue
        parameter_state = {}
        for state_name, value in frozen_state[optimizer_name].items():
            if (
                isinstance(parameter, DTensor)
                and isinstance(value, torch.Tensor)
                and not isinstance(value, DTensor)
                and value.shape == parameter.shape
            ):
                value = distribute_tensor(
                    value,
                    device_mesh=parameter.device_mesh,
                    placements=parameter.placements,
                )
            parameter_state[state_name] = value
        optimizer.state[parameter].clear()
        optimizer.state[parameter].update(parameter_state)


class CheckpointAppState:
    def __init__(
        self,
        *,
        model,
        optimizer,
        scheduler,
        scaler,
        metadata,
        state_dict_options,
    ):
        self.model = model
        self.optimizer = optimizer
        self.scheduler = scheduler
        self.scaler = scaler
        self.metadata = dict(metadata)
        self.state_dict_options = state_dict_options

    def state_dict(self):
        from torch.distributed.checkpoint.state_dict import get_state_dict

        model_state, optimizer_state = get_state_dict(
            self.model,
            self.optimizer,
            options=self.state_dict_options,
        )
        local_rng = _capture_rng_state()
        gathered_rng = [None] * torch.distributed.get_world_size()
        torch.distributed.all_gather_object(gathered_rng, local_rng)
        rng_by_rank = {
            str(rng_index): rng_state
            for rng_index, rng_state in enumerate(gathered_rng)
        }
        return {
            "model": model_state,
            "optimizer": optimizer_state,
            "scheduler": self.scheduler.state_dict(),
            "scaler": self.scaler.state_dict() if self.scaler is not None else None,
            "metadata": dict(self.metadata),
            "rng_by_rank": rng_by_rank,
        }

    def load_state_dict(self, state_dict):
        from torch.distributed.checkpoint.state_dict import (
            set_model_state_dict,
            set_optimizer_state_dict,
            set_state_dict,
        )

        validate_checkpoint_metadata(state_dict["metadata"], self.metadata)
        frozen_optimizer_state = _frozen_optimizer_state_from_canonical(
            self.model,
            state_dict["optimizer"],
        )
        if self.state_dict_options.full_state_dict:
            frozen_state_list = [
                frozen_optimizer_state if torch.distributed.get_rank() == 0 else None
            ]
            torch.distributed.broadcast_object_list(frozen_state_list, src=0)
            frozen_optimizer_state = frozen_state_list[0]
        if self.state_dict_options.full_state_dict:
            set_model_state_dict(
                self.model,
                state_dict["model"],
                options=self.state_dict_options,
            )
            set_optimizer_state_dict(
                self.model,
                self.optimizer,
                state_dict["optimizer"],
                options=self.state_dict_options,
            )
        else:
            set_state_dict(
                self.model,
                self.optimizer,
                model_state_dict=state_dict["model"],
                optim_state_dict=state_dict["optimizer"],
                options=self.state_dict_options,
            )
        _load_frozen_optimizer_state(
            self.model,
            self.optimizer,
            frozen_optimizer_state,
        )
        self.scheduler.load_state_dict(state_dict["scheduler"])
        if self.scaler is not None and state_dict["scaler"] is not None:
            self.scaler.load_state_dict(state_dict["scaler"])
        rank_rng = state_dict["rng_by_rank"][str(torch.distributed.get_rank())]
        _restore_rng_state(rank_rng)


def _register_checkpoint_app_state():
    """Declare the app state a DCP ``Stateful`` once, at import.

    ``dcp.save``/``dcp.load`` dispatch on the registration, which is a property
    of the class rather than of any one save, so it belongs here and not on
    every checkpoint.
    """
    from torch.distributed.checkpoint.stateful import Stateful

    Stateful.register(CheckpointAppState)


_register_checkpoint_app_state()


@dataclasses.dataclass(frozen=True)
class _PreparedCheckpoint:
    checkpoint: object | None
    checkpoint_dir_name: str
    step: int
    size_bytes: int
    local_path: str | None = None


@dataclasses.dataclass(frozen=True)
class _PendingCheckpoint:
    step: int
    local_path: str | None


def _remove_local_checkpoint(local_path, *, step):
    if local_path is None or not os.path.exists(local_path):
        return
    shutil.rmtree(local_path)
    if os.path.exists(local_path):
        raise RuntimeError(
            f"local checkpoint staging for step {step} still exists: {local_path}"
        )


def _report_checkpoint(prepared_checkpoint, *, upload_fn):
    try:
        ray.train.report(
            {
                "gcsfs_checkpoint_step": prepared_checkpoint.step,
                "gcsfs_checkpoint_size_bytes": prepared_checkpoint.size_bytes,
            },
            checkpoint=prepared_checkpoint.checkpoint,
            checkpoint_dir_name=prepared_checkpoint.checkpoint_dir_name,
            checkpoint_upload_mode=ray.train.CheckpointUploadMode.ASYNC,
            delete_local_checkpoint_after_upload=True,
            checkpoint_upload_fn=upload_fn,
        )
    except Exception as report_error:
        try:
            _remove_local_checkpoint(
                prepared_checkpoint.local_path,
                step=prepared_checkpoint.step,
            )
        except Exception as cleanup_error:
            raise RuntimeError(
                f"checkpoint step {prepared_checkpoint.step} report failed and "
                "local staging cleanup also failed"
            ) from ExceptionGroup(
                "checkpoint report and cleanup failures",
                [report_error, cleanup_error],
            )
        raise
    return _PendingCheckpoint(
        step=prepared_checkpoint.step,
        local_path=prepared_checkpoint.local_path,
    )


def _wait_for_pending_checkpoint(pending, *, timeout_s):
    reported = ray.train.get_all_reported_checkpoints(
        consistency_mode=ray.train.CheckpointConsistencyMode.COMMITTED,
        timeout_s=timeout_s,
    )
    matches = [
        item
        for item in reported
        if item.metrics.get("gcsfs_checkpoint_step") == pending.step
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"checkpoint step {pending.step} was not committed exactly once"
        )
    if matches[0].status is not ray.train.ReportedCheckpointStatus.COMMITTED:
        raise RuntimeError(
            f"checkpoint step {pending.step} has status {matches[0].status}"
        )
    _remove_local_checkpoint(pending.local_path, step=pending.step)


class CheckpointManager:
    def __init__(self, *, model, optimizer, scheduler, scaler, config):
        self.model = model
        self.optimizer = optimizer
        self.scheduler = scheduler
        self.scaler = scaler
        self.config = config
        self._metadata = None

    def _app_state(self, *, restoring=False):
        from torch.distributed.checkpoint.state_dict import StateDictOptions

        if self._metadata is None:
            self._metadata = checkpoint_metadata(self.config)
        full = self.config.checkpoint_format == "full"
        options = StateDictOptions(
            full_state_dict=full,
            cpu_offload=full and not restoring,
            broadcast_from_rank0=full and restoring,
        )
        return CheckpointAppState(
            model=self.model,
            optimizer=self.optimizer,
            scheduler=self.scheduler,
            scaler=self.scaler,
            metadata=self._metadata,
            state_dict_options=options,
        )

    def save_local(self, *, step):
        import torch.distributed.checkpoint as dcp

        rank = ray.train.get_context().get_world_rank()
        sharded = self.config.checkpoint_format == "sharded"
        app_state = self._app_state()
        local_directory = None
        try:
            if sharded:
                local_directory = tempfile.mkdtemp(
                    prefix=f"ray-train-step-{step}-rank-{rank}-"
                )
                dcp.save(
                    {"app": app_state},
                    storage_writer=dcp.FileSystemWriter(local_directory),
                )
                if rank == 0:
                    Path(local_directory, "_SUCCESS").touch()
            else:
                state = app_state.state_dict()
                if rank == 0:
                    local_directory = tempfile.mkdtemp(
                        prefix=f"ray-train-step-{step}-rank-0-"
                    )
                    torch.save(state, Path(local_directory, "checkpoint.pt"))
        except Exception:
            if local_directory is not None:
                shutil.rmtree(local_directory, ignore_errors=True)
            raise
        checkpoint = (
            ray.train.Checkpoint.from_directory(local_directory)
            if local_directory is not None
            else None
        )
        return _PreparedCheckpoint(
            checkpoint=checkpoint,
            checkpoint_dir_name=f"step-{step}.ckpt",
            step=step,
            size_bytes=_directory_size_bytes(local_directory),
            local_path=local_directory,
        )

    def restore(self, checkpoint):
        import torch.distributed.checkpoint as dcp

        rank = ray.train.get_context().get_world_rank()
        sharded = self.config.checkpoint_format == "sharded"
        app_state = self._app_state(restoring=True)
        directory_context = (
            checkpoint.as_directory()
            if sharded or rank == 0
            else contextlib.nullcontext(None)
        )
        with directory_context as local_directory:
            if sharded:
                if not Path(local_directory, "_SUCCESS").is_file():
                    raise FileNotFoundError("incomplete sharded checkpoint")
                if self.optimizer is not None:
                    _initialize_missing_optimizer_state_for_restore(self.optimizer)
                dcp.load(
                    {"app": app_state},
                    storage_reader=dcp.FileSystemReader(local_directory),
                )
            else:
                if rank == 0:
                    state = torch.load(
                        Path(local_directory, "checkpoint.pt"),
                        map_location="cpu",
                        weights_only=False,
                    )
                    metadata = {
                        name: value
                        for name, value in state.items()
                        if name not in ("model", "optimizer")
                    }
                else:
                    state = {"model": {}, "optimizer": {}}
                    metadata = None
                metadata_list = [metadata]
                torch.distributed.broadcast_object_list(metadata_list, src=0)
                state.update(metadata_list[0])
                app_state.load_state_dict(state)
            torch.distributed.barrier()
        return None


def require_declared_gpu_mode(observed_gpu, *, config, where) -> None:
    """Fail unless the accelerator present is the one the chart declared.

    ``USE_GPU`` is the chart's ``workload.gpu``. Detecting the mode instead of
    declaring it lets a GPU run whose driver or device plugin is missing fall
    through to the CPU probe and report a "successful" benchmark that measured
    something else entirely, with nothing in the summary row to show it.
    """
    if observed_gpu != config.use_gpu:
        raise RuntimeError(
            f"USE_GPU={config.use_gpu} but {where} sees "
            f"use_gpu={observed_gpu}; refusing to run a benchmark in a mode "
            "the configuration did not ask for"
        )


def train_loop_per_worker(train_loop_config):
    initial_checkpoint = None
    if isinstance(train_loop_config, dict) and "config" in train_loop_config:
        initial_checkpoint = train_loop_config.get("initial_checkpoint")
        train_loop_config = train_loop_config["config"]
    config = (
        train_loop_config
        if isinstance(train_loop_config, WorkloadConfig)
        else WorkloadConfig(**train_loop_config)
    )
    config.validate()
    context = ray.train.get_context()
    rank = context.get_world_rank()
    device = ray.train.torch.get_device()
    use_gpu = device.type == "cuda"
    require_declared_gpu_mode(use_gpu, config=config, where=f"rank {rank}")
    dataset_shard = ray.train.get_dataset_shard("train")
    gather_and_validate_placement(config)

    precision = select_precision(use_gpu=use_gpu)
    model = load_benchmark_model(
        config,
        use_gpu=use_gpu,
        mp_dtype=precision.model_dtype,
    )
    model, _, tp_mesh = parallelize_benchmark_model(
        model,
        config,
        device=device,
        mp_dtype=precision.compute_dtype,
    )
    sync_module = active_training_module(model, config, use_gpu=use_gpu)
    parameters_to_clip = trainable_parameters(model)
    optimizer = build_optimizer(model, config, use_gpu=use_gpu)
    total_steps = (
        config.max_steps if config.max_steps > 0 else max(config.epochs, 1) * 1000
    )
    scheduler = build_scheduler(
        optimizer,
        total_steps=total_steps,
        warmup_ratio=config.warmup_ratio,
    )
    scaler = precision.scaler
    checkpoint_manager = CheckpointManager(
        model=model,
        optimizer=optimizer,
        scheduler=scheduler,
        scaler=scaler,
        config=config,
    )
    if initial_checkpoint is not None:
        checkpoint_path = _checkpoint_path(initial_checkpoint)
        restore_start_time_s = time.time()
        restore_started = time.perf_counter()
        _emit_metric(
            event="checkpoint_restore_started",
            global_rank=rank,
            checkpoint_location=checkpoint_path,
            time_s=restore_start_time_s,
        )
        checkpoint_manager.restore(initial_checkpoint)
        restore_end_time_s = time.time()
        _emit_metric(
            event="checkpoint_restore_completed",
            global_rank=rank,
            checkpoint_location=checkpoint_path,
            duration_s=time.perf_counter() - restore_started,
            time_s=restore_end_time_s,
        )

    is_data_leader = not config.is_model_parallel or (
        rank % config.tensor_parallel_size == 0
    )
    run_step = 0
    epoch = 0
    pending_checkpoint = None
    checkpoint_upload_fn = None
    if config.checkpoint_write_path:
        _, checkpoint_filesystem, checkpoint_root = _checkpoint_storage(
            config.checkpoint_write_path
        )
        checkpoint_upload_fn = make_checkpoint_upload_fn(
            destination_root=f"{checkpoint_root}/{config.run_id}",
            storage_filesystem=checkpoint_filesystem,
            global_rank=rank,
        )
    iterated_dataset = False
    snapshot_emitted = False
    data_started = time.perf_counter()
    optimizer.zero_grad(set_to_none=True)

    def emit_data_snapshot():
        """Emit the run's Ray Data snapshot at most once."""
        nonlocal snapshot_emitted
        if snapshot_emitted or not (is_data_leader and iterated_dataset):
            return
        snapshot_emitted = True
        emit_ray_data_iteration(
            dataset_shard,
            config=config,
            rank=rank,
            total_s=time.perf_counter() - data_started,
        )

    try:
        while config.max_steps > 0 or epoch < config.epochs:
            if config.max_steps > 0 and run_step >= config.max_steps:
                break
            iterator = None
            try:
                iterator = (
                    iter_train_batches(
                        dataset_shard,
                        config=config,
                        device=device,
                        epoch=epoch,
                    )
                    if is_data_leader
                    else None
                )
                steps_before_epoch = run_step
                logging.info("Rank %d: Epoch %d started", rank, epoch)

                while config.max_steps < 0 or run_step < config.max_steps:
                    step_started = time.perf_counter()
                    complete_step = True
                    for micro_step in range(config.gradient_accumulation_steps):
                        try:
                            batch = next_train_batch(
                                iterator,
                                config=config,
                                device=device,
                                tp_mesh=tp_mesh,
                            )
                        except StopIteration:
                            complete_step = False
                            break
                        synchronize = (
                            micro_step + 1 == config.gradient_accumulation_steps
                        )
                        with gradient_sync_context(
                            sync_module,
                            synchronize=synchronize,
                        ):
                            autocast = (
                                torch.autocast(
                                    device_type="cuda",
                                    dtype=precision.compute_dtype,
                                )
                                if use_gpu
                                else contextlib.nullcontext()
                            )
                            with autocast:
                                loss = model(batch)
                                scaled_loss = loss / config.gradient_accumulation_steps
                            if scaler is None:
                                scaled_loss.backward()
                            else:
                                scaler.scale(scaled_loss).backward()

                    if not complete_step:
                        optimizer.zero_grad(set_to_none=True)
                        break

                    if not use_gpu and config.simulated_step_compute_seconds:
                        time.sleep(config.simulated_step_compute_seconds)
                    if scaler is not None:
                        scaler.unscale_(optimizer)
                    torch.nn.utils.clip_grad_norm_(
                        parameters_to_clip,
                        config.max_grad_norm,
                    )
                    if scaler is None:
                        optimizer.step()
                    else:
                        scaler.step(optimizer)
                        scaler.update()
                    scheduler.step()
                    optimizer.zero_grad(set_to_none=True)
                    torch.distributed.barrier()

                    run_step += 1
                    step_duration = time.perf_counter() - step_started
                    if rank == 0:
                        samples = (
                            config.per_device_batch_size
                            * config.gradient_accumulation_steps
                            * config.dp_size
                        )
                        _emit_metric(
                            event="step",
                            global_rank=rank,
                            step=run_step,
                            loss=float(loss.detach().float().item()),
                            duration_s=step_duration,
                            samples_per_second=samples / step_duration,
                        )
                    if (
                        config.checkpoint_write_path
                        and run_step % config.checkpoint_interval == 0
                    ):
                        if pending_checkpoint is not None:
                            _wait_for_pending_checkpoint(
                                pending_checkpoint,
                                timeout_s=config.checkpoint_upload_timeout_seconds,
                            )
                            pending_checkpoint = None
                        prepared_checkpoint = checkpoint_manager.save_local(
                            step=run_step
                        )
                        pending_checkpoint = _report_checkpoint(
                            prepared_checkpoint,
                            upload_fn=checkpoint_upload_fn,
                        )
                if run_step == steps_before_epoch:
                    raise RuntimeError(
                        f"epoch {epoch} produced no complete optimizer step; "
                        "check dataset size and batch configuration"
                    )
                logging.info(
                    "Rank %d: Epoch %d ended at run step %d", rank, epoch, run_step
                )
                epoch += 1
            finally:
                if iterator is not None and hasattr(iterator, "close"):
                    iterator.close()
                if is_data_leader and iterator is not None:
                    iterated_dataset = True
        # Emitted before the final wait: worker records reach the pod's
        # stdout only through Ray's asynchronous log forwarding, so the last
        # record written before the process exits is the one most likely to be
        # lost. Waiting for the upload gives the forwarder time to catch up.
        emit_data_snapshot()
        if pending_checkpoint is not None:
            _wait_for_pending_checkpoint(
                pending_checkpoint,
                timeout_s=config.checkpoint_upload_timeout_seconds,
            )
            pending_checkpoint = None
    finally:
        emit_data_snapshot()
    return None


def _optional_path(name: str) -> str | None:
    return os.getenv(name, "").rstrip("/") or None


def _optional_int_env(name: str) -> int | None:
    value = os.getenv(name, "").strip()
    return int(value) if value else None


def _bool_env(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes"}


@dataclasses.dataclass(frozen=True)
class WorkloadConfig:
    run_id: str
    dataset_path: str
    model_id: str
    local_model_path: str
    checkpoint_write_path: str | None
    checkpoint_load_path: str | None
    strategy: str
    nodes: int
    ranks_per_node: int
    max_steps: int
    epochs: int
    checkpoint_interval: int
    checkpoints_to_keep: int
    per_device_batch_size: int
    gradient_accumulation_steps: int
    dataloader_workers: int
    dataloader_prefetch_factor: int
    shuffle_buffer_size: int
    tensor_parallel_size: int
    data_parallel_size: int
    simulated_step_compute_seconds: float
    sequence_length: int = 512
    learning_rate: float = 2e-5
    weight_decay: float = 1e-6
    warmup_ratio: float = 0.03
    max_grad_norm: float = 1.0
    tokenize_batch_size: int = 64
    use_gpu: bool = False
    read_concurrency: int | None = None
    shuffle_files: bool = True
    checkpoint_upload_timeout_seconds: float = 1800.0
    overwrite_existing_run: bool = False
    torch_distributed_timeout_seconds: int = 14400

    @classmethod
    def from_env(cls) -> "WorkloadConfig":
        model_id = os.getenv("MODEL_ID", "meta-llama/Llama-3.1-8B")
        return cls(
            run_id=os.getenv("RUN_ID", ""),
            dataset_path=os.getenv("DATASET_PATH", "").rstrip("/"),
            model_id=model_id,
            local_model_path=os.getenv("LOCAL_MODEL_PATH", model_id),
            checkpoint_write_path=_optional_path("CKPT_WRITE_PATH"),
            checkpoint_load_path=_optional_path("CKPT_LOAD_PATH"),
            strategy=os.getenv("TRAINING_STRATEGY", "ddp").lower(),
            nodes=int(os.getenv("NNODES", "1")),
            ranks_per_node=int(os.getenv("RANKS_PER_NODE", "1")),
            max_steps=int(os.getenv("MAX_STEPS", "-1")),
            epochs=int(os.getenv("NUM_TRAIN_EPOCHS", "3")),
            checkpoint_interval=int(os.getenv("CHECKPOINT_WRITE_INTERVAL", "25")),
            checkpoints_to_keep=int(os.getenv("CKPT_TO_KEEP", "1")),
            per_device_batch_size=int(os.getenv("PER_DEVICE_TRAIN_BATCH_SIZE", "8")),
            gradient_accumulation_steps=int(
                os.getenv("GRADIENT_ACCUMULATION_STEPS", "1")
            ),
            dataloader_workers=int(os.getenv("DATALOADER_NUM_WORKERS", "16")),
            dataloader_prefetch_factor=int(
                os.getenv("DATALOADER_PREFETCH_FACTOR", "2")
            ),
            shuffle_buffer_size=int(os.getenv("SHUFFLE_BUFFER_SIZE", "10000")),
            shuffle_files=_bool_env("SHUFFLE_FILES", "true"),
            tensor_parallel_size=int(os.getenv("TENSOR_PARALLEL_SIZE", "4")),
            data_parallel_size=int(os.getenv("DATA_PARALLEL_SIZE", "2")),
            simulated_step_compute_seconds=float(
                os.getenv("SIMULATED_STEP_COMPUTE_SECONDS", "1.0")
            ),
            learning_rate=float(os.getenv("LEARNING_RATE", "2e-5")),
            weight_decay=float(os.getenv("WEIGHT_DECAY", "1e-6")),
            warmup_ratio=float(os.getenv("WARMUP_RATIO", "0.03")),
            max_grad_norm=float(os.getenv("MAX_GRAD_NORM", "1.0")),
            tokenize_batch_size=int(os.getenv("TOKENIZE_BATCH_SIZE", "64")),
            use_gpu=_bool_env("USE_GPU"),
            read_concurrency=_optional_int_env("READ_CONCURRENCY"),
            checkpoint_upload_timeout_seconds=float(
                os.getenv("CHECKPOINT_UPLOAD_TIMEOUT_SECONDS", "1800")
            ),
            overwrite_existing_run=_bool_env("OVERWRITE_EXISTING_RUN"),
            torch_distributed_timeout_seconds=int(
                os.getenv("TORCH_DISTRIBUTED_TIMEOUT_SECONDS", "900")
            ),
        )

    @property
    def world_size(self) -> int:
        return self.nodes * self.ranks_per_node

    @property
    def checkpoint_format(self) -> str:
        return "sharded" if self.strategy.endswith("_sharded") else "full"

    @property
    def is_model_parallel(self) -> bool:
        return self.strategy.startswith("model_parallel_")

    @property
    def dp_size(self) -> int:
        return self.data_parallel_size if self.is_model_parallel else self.world_size

    def validate(self) -> None:
        required = (
            self.run_id,
            self.dataset_path,
            self.model_id,
            self.local_model_path,
        )
        if not all(required):
            raise ValueError(
                "RUN_ID, DATASET_PATH, MODEL_ID, and LOCAL_MODEL_PATH are required"
            )
        if self.strategy not in SUPPORTED_STRATEGIES:
            raise ValueError(f"unsupported strategy: {self.strategy}")

        positive = {
            "nodes": self.nodes,
            "ranks_per_node": self.ranks_per_node,
            "epochs": self.epochs,
            "checkpoint_interval": self.checkpoint_interval,
            "checkpoints_to_keep": self.checkpoints_to_keep,
            "per_device_batch_size": self.per_device_batch_size,
            "gradient_accumulation_steps": self.gradient_accumulation_steps,
            "dataloader_workers": self.dataloader_workers,
            "dataloader_prefetch_factor": self.dataloader_prefetch_factor,
            "tensor_parallel_size": self.tensor_parallel_size,
            "data_parallel_size": self.data_parallel_size,
            "sequence_length": self.sequence_length,
            "tokenize_batch_size": self.tokenize_batch_size,
        }
        bad = {name: value for name, value in positive.items() if value <= 0}
        if bad:
            raise ValueError(f"positive configuration values required: {bad}")
        if self.max_steps == 0 or self.max_steps < -1:
            raise ValueError("MAX_STEPS must be -1 or a positive integer")
        if self.shuffle_buffer_size < 0:
            raise ValueError("SHUFFLE_BUFFER_SIZE must be non-negative")
        if 0 < self.shuffle_buffer_size < self.per_device_batch_size:
            raise ValueError(
                "SHUFFLE_BUFFER_SIZE must be zero or at least "
                "PER_DEVICE_TRAIN_BATCH_SIZE"
            )
        if self.simulated_step_compute_seconds < 0:
            raise ValueError("simulated compute must be non-negative")
        if self.learning_rate <= 0 or self.weight_decay < 0:
            raise ValueError("optimizer learning rate/weight decay are invalid")
        if not 0 <= self.warmup_ratio < 1:
            raise ValueError("WARMUP_RATIO must be in [0, 1)")
        if self.max_grad_norm <= 0:
            raise ValueError("MAX_GRAD_NORM must be positive")
        if self.read_concurrency is not None and self.read_concurrency <= 0:
            raise ValueError("READ_CONCURRENCY must be unset or a positive integer")
        if self.checkpoint_upload_timeout_seconds <= 0:
            raise ValueError("CHECKPOINT_UPLOAD_TIMEOUT_SECONDS must be positive")
        if self.checkpoint_load_path and self.overwrite_existing_run:
            raise ValueError(
                "OVERWRITE_EXISTING_RUN cannot be enabled for a checkpoint load"
            )
        if self.torch_distributed_timeout_seconds <= 0:
            raise ValueError("TORCH_DISTRIBUTED_TIMEOUT_SECONDS must be positive")

        if self.is_model_parallel:
            if self.tensor_parallel_size * self.data_parallel_size != self.world_size:
                raise ValueError("TP * DP must equal world size")
            if self.tensor_parallel_size > self.ranks_per_node:
                raise ValueError("TP must not exceed ranks per node")
            if self.ranks_per_node % self.tensor_parallel_size:
                raise ValueError("TP must divide ranks per node")


def _build_trainer(config, dataset, *, use_gpu):
    if config.checkpoint_write_path:
        _, storage_filesystem, storage_path = _checkpoint_storage(
            config.checkpoint_write_path
        )
    else:
        storage_filesystem = None
        storage_path = "/tmp/ray-results"
    initial_checkpoint = (
        _ray_checkpoint_from_uri(config.checkpoint_load_path)
        if config.checkpoint_load_path
        else None
    )
    return ray.train.torch.TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config={
            "config": dataclasses.asdict(config),
            "initial_checkpoint": initial_checkpoint,
        },
        datasets={"train": dataset},
        dataset_config=DPAwareDataConfig(
            config.tensor_parallel_size if config.is_model_parallel else 1
        ),
        scaling_config=ray.train.ScalingConfig(
            num_workers=config.world_size,
            use_gpu=use_gpu,
            resources_per_worker={"CPU": 1, "train_slot": 1},
            placement_strategy="PACK",
        ),
        torch_config=ray.train.torch.TorchConfig(
            timeout_s=config.torch_distributed_timeout_seconds
        ),
        run_config=ray.train.RunConfig(
            name=config.run_id,
            storage_path=storage_path,
            storage_filesystem=storage_filesystem,
            callbacks=[
                BenchmarkMetricsCallback(),
                BenchmarkControllerCallback(),
            ],
            failure_config=ray.train.FailureConfig(max_failures=0),
            checkpoint_config=ray.train.CheckpointConfig(
                num_to_keep=config.checkpoints_to_keep
            ),
        ),
    )


def main():
    config = WorkloadConfig.from_env()
    config.validate()
    require_declared_gpu_mode(torch.cuda.is_available(), config=config, where="driver")
    use_gpu = config.use_gpu
    prepare_checkpoint_run(config)
    ray.init(address="auto")
    ray.data.DataContext.get_current().enable_progress_bars = False
    filesystem, _ = fsspec.core.url_to_fs(config.dataset_path)
    logging.info(
        "Ray Data source filesystem=%s path=%s",
        type(filesystem).__name__,
        config.dataset_path,
    )
    logging.info(
        "Ray Train configuration: use_gpu=%s strategy=%s world_size=%d "
        "ranks_per_node=%d tp_size=%d dp_size=%d per_device_batch=%d "
        "gradient_accumulation=%d",
        use_gpu,
        config.strategy,
        config.world_size,
        config.ranks_per_node,
        config.tensor_parallel_size if config.is_model_parallel else 1,
        config.dp_size,
        config.per_device_batch_size,
        config.gradient_accumulation_steps,
    )
    dataset_started = time.perf_counter()
    dataset = build_train_dataset(config)
    _emit_metric(
        event="dataset_build",
        global_rank=0,
        duration_s=time.perf_counter() - dataset_started,
        dataset_path=config.dataset_path,
    )
    trainer = _build_trainer(config, dataset, use_gpu=use_gpu)
    result = trainer.fit()
    if result.error:
        raise result.error


if __name__ == "__main__":
    main()
