"""TorchTitan (``main``) Llama 3.1 70B macrobenchmark recipe for gcsfs.

Runs upstream TorchTitan's own ``Trainer`` (launched with
``python -m torchtitan.train --module torchtitan_train --config llama3_70b_gcsfs``)
and changes only:

* the dataset: a Grain ``HuggingFaceStreamingSource`` over ``gs://`` Parquet
  shards, read through ``datasets``/``fsspec``/``gcsfs`` and packed into
  ``SEQ_LEN`` rows with upstream's concatenate-then-split packing;
* checkpointing: TorchTitan's native DCP ``CheckpointManager`` writing full
  training state to a ``gs://`` folder;
* instrumentation: a thin ``Trainer``/``TrainingEngine``/``GrainDataLoader``
  subclass layer that prints the log lines parsed by
  ``cloudbuild/macrobenchmarks/metrics/parsers/hf.py``.

Every run setting comes from environment variables set by the Helm chart (see
``templates/workload-job.yaml``).
"""

from __future__ import annotations

import dataclasses
import logging
import os
import re
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

import fsspec
import torch
import torch.distributed as dist
from torch.distributed.checkpoint.fsspec_filesystem import FsspecReader
from torchtitan.components.checkpointer import DATALOADER, CheckpointManager
from torchtitan.components.data import (
    ConcatThenSplitPackingConfig,
    GrainDataLoader,
    HuggingFaceStreamingSource,
    SingleDatasetConfig,
    TrainingMicrobatch,
)
from torchtitan.components.data.text_datasets import TextProcessor
from torchtitan.components.loss import ChunkedLossWrapper, CrossEntropyLoss
from torchtitan.config.parallelism import ParallelismConfig
from torchtitan.models.common.config_utils import decoder_vocab_size
from torchtitan.models.llama3 import build_model_config
from torchtitan.observability.metrics import MetricsProcessor
from torchtitan.observability.profiler import Profiler
from torchtitan.tools import filesystem
from torchtitan.trainer import Trainer
from torchtitan.training_engine import TrainingEngine
from torchtitan_recipes.data import c4_document_capacity
from torchtitan_recipes.models.llama3 import llama3_70b

VALID_TRAINING_STRATEGIES = ("fsdp_sharded", "model_parallel_sharded")
# "debugmodel" (vocab 2048) is for smoke/unit tests and needs a matching small
# tokenizer via LOCAL_TOKENIZER_PATH, e.g. TorchTitan's tests/assets/tokenizer.
SUPPORTED_FLAVORS = ("70B", "debugmodel")

# Upstream's purge thread logs "Checkpointer deleted %s in %.2f seconds."
_CHECKPOINTER_LOGGER = "torchtitan.components.checkpointer.base"
_DELETED_MSG = "Checkpointer deleted %s in %.2f seconds."
_STEP_DIR_RE = re.compile(r"step-(\d+)/?$")


def _env_str(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name, "").strip()
    return int(value) if value else default


def _global_rank() -> int:
    if dist.is_available() and dist.is_initialized():
        return dist.get_rank()
    return int(os.environ.get("RANK", "0"))


def _barrier() -> None:
    if dist.is_available() and dist.is_initialized():
        dist.barrier()


# =====================================================================
# 1. Recipe: env -> Trainer.Config
# =====================================================================


def resolve_parallelism(
    *,
    world_size: int,
    training_strategy: str,
    tp_size: int = 1,
    dp_size: int | None = None,
    dp_replicate_size: int = 1,
    cp_size: int = 1,
    pp_size: int = 1,
) -> ParallelismConfig:
    """Map the benchmark's strategy/degree env vars onto ``ParallelismConfig``."""
    if training_strategy not in VALID_TRAINING_STRATEGIES:
        raise ValueError(
            f"TRAINING_STRATEGY must be one of {VALID_TRAINING_STRATEGIES} "
            f"(got {training_strategy!r})"
        )
    for name, val in (
        ("TENSOR_PARALLEL_SIZE", tp_size),
        ("DP_REPLICATE_SIZE", dp_replicate_size),
        ("CONTEXT_PARALLEL_SIZE", cp_size),
        ("PIPELINE_PARALLEL_SIZE", pp_size),
    ):
        if val < 1:
            raise ValueError(f"{name} must be >= 1 (got {val})")
    if dp_size is not None and dp_size < 1:
        raise ValueError(f"DATA_PARALLEL_SIZE must be >= 1 (got {dp_size})")

    use_mp = training_strategy == "model_parallel_sharded" and world_size > 1
    tp = tp_size if use_mp else 1
    cp = cp_size if use_mp else 1
    pp = pp_size if use_mp else 1
    dp_replicate = dp_replicate_size if use_mp else 1

    non_dp_shard = pp * dp_replicate * cp * tp
    if world_size % non_dp_shard != 0:
        raise ValueError(
            f"WORLD_SIZE ({world_size}) must be divisible by "
            f"PIPELINE_PARALLEL_SIZE ({pp}) * DP_REPLICATE_SIZE ({dp_replicate}) * "
            f"CONTEXT_PARALLEL_SIZE ({cp}) * TENSOR_PARALLEL_SIZE ({tp}) = {non_dp_shard}."
        )
    dp_shard = world_size // non_dp_shard
    if use_mp and dp_size is not None and dp_size != dp_shard:
        raise ValueError(
            f"PIPELINE_PARALLEL_SIZE ({pp}) * DP_REPLICATE_SIZE ({dp_replicate}) * "
            f"DATA_PARALLEL_SIZE ({dp_size}) * CONTEXT_PARALLEL_SIZE ({cp}) * "
            f"TENSOR_PARALLEL_SIZE ({tp}) must equal WORLD_SIZE ({world_size})."
        )
    return ParallelismConfig(
        data_parallel_replicate_degree=dp_replicate,
        data_parallel_shard_degree=dp_shard,
        tensor_parallel_degree=tp,
        context_parallel_degree=cp,
        pipeline_parallel_degree=pp,
        num_pp_microbatches=pp,
    )


def _keep_sample(sample: Any) -> bool:
    return sample is not None


def build_parquet_dataset(dataset_path: str) -> ConcatThenSplitPackingConfig:
    """Grain dataset graph: streaming Parquet over gcsfs -> tokenize -> pack."""
    return ConcatThenSplitPackingConfig(
        dataset=SingleDatasetConfig(
            source=HuggingFaceStreamingSource.Config(
                path="parquet",
                split="train",
                load_dataset_kwargs={
                    "data_files": f"{dataset_path.rstrip('/')}/*.parquet",
                    "columns": ["text"],
                },
            ),
            processor=TextProcessor.Config(),
            post_filters=(_keep_sample,),
        )
    )


def build_checkpointer_config(
    *,
    ckpt_write_path: str,
    run_id: str,
    interval: int,
    keep_latest_k: int,
    ckpt_load_path: str,
) -> CheckpointManager.Config | None:
    """Full-state DCP checkpoints under ``<ckpt_write_path>/<run_id>``."""
    if not ckpt_write_path and not ckpt_load_path:
        return None
    return CheckpointManager.Config(
        folder=(
            f"{ckpt_write_path.rstrip('/')}/{run_id}"
            if ckpt_write_path
            else "checkpoint"
        ),
        interval=max(1, interval),
        keep_latest_k=0 if keep_latest_k <= 0 else max(2, keep_latest_k),
        # Keep the final save a full, resumable checkpoint (upstream defaults to a
        # model-only export) so every save in a run has the same shape.
        last_save_model_only=False,
        initial_load_path=ckpt_load_path or None,
        initial_load_model_only=False,
        load_only=not ckpt_write_path,
    )


def llama3_70b_gcsfs() -> Trainer.Config:
    """Upstream ``llama3_70b()`` with gcsfs data/checkpoint paths from env vars."""
    dataset_path = _env_str("DATASET_PATH").rstrip("/")
    if not dataset_path:
        raise SystemExit("DATASET_PATH env var is required.")
    run_id = _env_str("RUN_ID")
    if not run_id:
        raise SystemExit("RUN_ID env var is required.")

    flavor = _env_str("TORCHTITAN_FLAVOR", "70B")
    if flavor not in SUPPORTED_FLAVORS:
        raise ValueError(
            f"Unsupported TORCHTITAN_FLAVOR={flavor!r}. Supported flavors: {SUPPORTED_FLAVORS}"
        )
    seq_len = _env_int("SEQ_LEN", 2048)
    max_steps = _env_int("MAX_STEPS", 50)
    if max_steps < 1:
        raise ValueError(f"MAX_STEPS must be >= 1 (got {max_steps})")
    per_device_batch = max(1, _env_int("PER_DEVICE_TRAIN_BATCH_SIZE", 1))
    grad_accum = max(1, _env_int("GRADIENT_ACCUMULATION_STEPS", 1))
    world_size = _env_int("WORLD_SIZE", 1)
    dp_size_env = _env_str("DATA_PARALLEL_SIZE")

    parallelism = resolve_parallelism(
        world_size=world_size,
        training_strategy=_env_str("TRAINING_STRATEGY", "model_parallel_sharded"),
        tp_size=_env_int("TENSOR_PARALLEL_SIZE", 4),
        dp_size=int(dp_size_env) if dp_size_env else None,
        dp_replicate_size=_env_int("DP_REPLICATE_SIZE", 1),
        cp_size=_env_int("CONTEXT_PARALLEL_SIZE", 1),
        pp_size=_env_int("PIPELINE_PARALLEL_SIZE", 1),
    )
    model_config = build_model_config(
        flavor,
        seq_len=seq_len,
        parallelism=parallelism,
        attn_backend=_env_str("ATTN_BACKEND", "flex"),
    )

    base = llama3_70b(seq_len=seq_len)
    tokens_per_microbatch = per_device_batch * model_config.max_context_length
    dp_degree = (
        parallelism.data_parallel_replicate_degree
        * parallelism.data_parallel_shard_degree
    )
    training = dataclasses.replace(
        base.training,
        steps=max_steps,
        max_context_length=model_config.max_context_length,
        num_tokens_per_microbatch_per_dp_rank=tokens_per_microbatch,
        num_tokens_per_train_step=(
            grad_accum
            * tokens_per_microbatch
            * parallelism.num_pp_microbatches
            * dp_degree
        ),
    )
    optim = base.optim
    optim.lr_scheduler.warmup_steps = max(1, max_steps // 5)
    # Upstream bakes the 70B vocab into its loss config; rebuild it for `flavor`.
    loss = ChunkedLossWrapper.Config(
        loss_fn=CrossEntropyLoss.Config(
            global_vocab_size=decoder_vocab_size(model_config),
        ),
    )

    dataloader = GcsfsGrainDataLoader.Config(
        dataset=build_parquet_dataset(dataset_path),
        max_num_documents=c4_document_capacity(tokens_per_microbatch),
        streaming_shuffle_buffer_size=max(1, _env_int("SHUFFLE_BUFFER_SIZE", 1000)),
        dataset_path=dataset_path,
    )
    comm = dataclasses.replace(
        base.comm,
        init_timeout_seconds=_env_int("TORCH_DISTRIBUTED_TIMEOUT_SECONDS", 3600),
        train_timeout_seconds=_env_int("TORCH_DISTRIBUTED_TIMEOUT_SECONDS", 3600),
    )

    overrides: dict[str, Any] = {
        "model": model_config,
        "parallelism": parallelism,
        "training": training,
        "optim": optim,
        "loss": loss,
        "dataloader": dataloader,
        "comm": comm,
        "checkpointer": build_checkpointer_config(
            ckpt_write_path=_env_str("CKPT_WRITE_PATH").rstrip("/"),
            run_id=run_id,
            interval=min(_env_int("CHECKPOINT_WRITE_INTERVAL", 25), max_steps),
            keep_latest_k=_env_int("CKPT_TO_KEEP", 1),
            ckpt_load_path=_env_str("CKPT_LOAD_PATH"),
        ),
        "hf_assets_path": _env_str("LOCAL_TOKENIZER_PATH") or base.hf_assets_path,
        # log_freq=1 so the per-step "Loss" in the metric line is always fresh.
        "metrics": MetricsProcessor.Config(log_freq=1, enable_tensorboard=False),
        "profiler": Profiler.Config(enable_profiling=False),
        "validator": None,
        "dump_folder": _env_str("TORCHTITAN_DUMP_FOLDER", "/tmp/torchtitan_outputs"),
    }
    kwargs = {f.name: getattr(base, f.name) for f in dataclasses.fields(base) if f.init}
    kwargs.update(overrides)
    return GcsfsBenchmarkTrainer.Config(**kwargs)


# =====================================================================
# 2. Dataloader: time dataset-graph construction
# =====================================================================


class GcsfsGrainDataLoader(GrainDataLoader):
    """``GrainDataLoader`` that reports how long building the dataset graph took."""

    @dataclass(kw_only=True, slots=True)
    class Config(GrainDataLoader.Config):
        dataset_path: str = ""
        """Dataset path echoed in the ``Dataset Build`` metric line."""

    def __init__(self, config: Config, **kwargs: Any) -> None:
        start = time.perf_counter()
        super().__init__(config, **kwargs)
        print(
            f"Dataset Build : Rank : {_global_rank()} : "
            f"Duration : {time.perf_counter() - start:.4f} seconds : "
            f"Path: {config.dataset_path}",
            flush=True,
        )


# =====================================================================
# 3. Engine: time checkpoint save / restore / delete
# =====================================================================


class _CheckpointDeleteLogBridge(logging.Filter):
    """Re-emit upstream purge-thread timings as the benchmark's delete metric."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.msg == _DELETED_MSG and isinstance(record.args, tuple):
            path, seconds = record.args[0], float(record.args[1])
            match = _STEP_DIR_RE.search(str(path))
            step = int(match.group(1)) if match else -1
            print(
                f"Finished deleting checkpoint {path} in {seconds:.4f} seconds "
                f"for global_step {step} from rank {_global_rank()}",
                flush=True,
            )
        return True


def _install_delete_bridge() -> None:
    target = logging.getLogger(_CHECKPOINTER_LOGGER)
    if not any(isinstance(f, _CheckpointDeleteLogBridge) for f in target.filters):
        target.addFilter(_CheckpointDeleteLogBridge())


def saved_dataloader_dp_ranks(checkpoint_id: str) -> set[int]:
    """DP ranks whose dataloader state is present in a DCP checkpoint's metadata."""
    metadata = FsspecReader(checkpoint_id).read_metadata()
    pattern = re.compile(rf"^{re.escape(DATALOADER)}\.dp_rank_(\d+)(?:\.|$)")
    ranks = set()
    for key in metadata.state_dict_metadata:
        match = pattern.match(key)
        if match:
            ranks.add(int(match.group(1)))
    return ranks


class GcsfsTrainingEngine(TrainingEngine):
    """``TrainingEngine`` that prints checkpoint save/restore metric lines."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _install_delete_bridge()
        super().__init__(*args, **kwargs)

    def _checkpoint_dir(self, step: int) -> str:
        folder = getattr(self.checkpointer, "folder", self.config.checkpointer.folder)
        return filesystem.join(folder, f"step-{step}")

    def _current_dp_degree(self) -> int:
        ctx = self.parallelism_context
        return ctx.get_mesh("dp").size() if ctx.dp_enabled else 1

    def _skip_incompatible_dataloader_state(self, checkpoint_id: str) -> None:
        """Upstream ``GrainDataLoader`` refuses to resume after a DP-degree change;
        restore model/optimizer/train state and restart the data stream instead."""
        saved = saved_dataloader_dp_ranks(checkpoint_id)
        current = self._current_dp_degree()
        if saved and len(saved) == current:
            return
        excluded = self.checkpointer.exclude_from_loading
        if DATALOADER not in excluded:
            excluded.append(DATALOADER)
        if _global_rank() == 0:
            print(
                f"WARNING: Skipping dataloader state restore: checkpoint has state for "
                f"{len(saved)} DP ranks, current DP degree is {current}.",
                flush=True,
            )

    def load_checkpoint(self) -> bool:
        ckpt_config = self.config.checkpointer
        if ckpt_config is None:
            return False
        load_path = ckpt_config.initial_load_path
        fresh_folder = not filesystem.isdir(self.checkpointer.folder)
        if load_path and fresh_folder:
            self._skip_incompatible_dataloader_state(load_path)
        else:
            load_path = self.checkpointer.folder

        start_wall = time.time()
        start = time.perf_counter()
        loaded = super().load_checkpoint()
        if loaded:
            _barrier()
            rank = _global_rank()
            print(
                f"Checkpoint Restore Start : Rank : {rank} : "
                f"Start time: {start_wall:.4f} seconds : Path: {load_path}",
                flush=True,
            )
            print(
                f"Finished restoring checkpoint : Rank : {rank} : "
                f"Duration: {time.perf_counter() - start:.4f} seconds : "
                f"End Time: {time.time():.4f} seconds : Path: {load_path}",
                flush=True,
            )
        return loaded

    def save_checkpoint(self, *, last_step: bool = False) -> bool:
        start_wall = time.time()
        start = time.perf_counter()
        saved = super().save_checkpoint(last_step=last_step)
        if not saved:
            return False
        self.checkpointer.maybe_wait_for_saving()
        _barrier()
        duration = time.perf_counter() - start
        step = self.num_completed_steps
        ckpt_dir = self._checkpoint_dir(step)
        rank = _global_rank()
        print(
            f"Checkpoint Save : Rank: {rank} : Step: {step} : "
            f"Start time: {start_wall:.4f} seconds: Path: {ckpt_dir}",
            flush=True,
        )
        print(
            f"Finished saving checkpoint to {ckpt_dir} in {duration:.4f} seconds "
            f"for global_step {step} from rank {rank}",
            flush=True,
        )
        if rank == 0:
            fs, fs_path = fsspec.core.url_to_fs(ckpt_dir)
            total_bytes = int(fs.du(fs_path)) if fs.exists(fs_path) else 0
            print(
                f"Checkpoint Size : Rank : 0 : Step : {step} : "
                f"Bytes : {total_bytes} : Path: {ckpt_dir}",
                flush=True,
            )
        _barrier()
        return True


# =====================================================================
# 4. Trainer: time data waits and steps
# =====================================================================


class GcsfsBenchmarkTrainer(Trainer):
    """Upstream ``Trainer`` plus the benchmark's step / data-wait metric lines."""

    engine_cls = GcsfsTrainingEngine

    @dataclass(kw_only=True, slots=True)
    class Config(Trainer.Config):
        pass

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self._last_loss = 0.0
        self._fetch_count = 0
        self._fetch_wait_s = 0.0
        self._data_wait_total_s = 0.0
        self._step_wall_s = 0.0

        ctx = self.engine.parallelism_context
        dp_degree = ctx.get_mesh("dp").size() if ctx.dp_enabled else 1
        rows_per_microbatch = max(
            1,
            config.training.num_tokens_per_microbatch_per_dp_rank
            // config.training.max_context_length,
        )
        self._samples_per_step = (
            rows_per_microbatch
            * self.gradient_accumulation_steps
            * self.num_pp_microbatches
            * dp_degree
        )

        upstream_log = self.metrics_processor.log

        def _capture_loss(
            step, global_avg_loss, global_max_loss, grad_norm, extra_metrics=None
        ):
            self._last_loss = float(global_avg_loss)
            return upstream_log(
                step,
                global_avg_loss,
                global_max_loss,
                grad_norm,
                extra_metrics=extra_metrics,
            )

        self.metrics_processor.log = _capture_loss

    def _timed_batches(
        self, data_iterable: Iterable[TrainingMicrobatch]
    ) -> Iterator[TrainingMicrobatch]:
        rank = _global_rank()
        start = time.perf_counter()
        iterator = iter(data_iterable)
        setup = time.perf_counter() - start
        self._data_wait_total_s += setup
        print(
            f"Data Wait : Rank : {rank} : Fetch : 0 : Action : setup_train_dataloader : "
            f"Duration : {setup:.6f} seconds : Total : {self._data_wait_total_s:.6f} seconds",
            flush=True,
        )
        while True:
            start = time.perf_counter()
            try:
                microbatch = next(iterator)
            except StopIteration:
                return
            wait = time.perf_counter() - start
            self._fetch_count += 1
            self._fetch_wait_s += wait
            self._data_wait_total_s += wait
            print(
                f"Data Wait : Rank : {rank} : Fetch : {self._fetch_count} : "
                f"Action : [_TrainingEpochLoop].train_dataloader_next : "
                f"Duration : {wait:.6f} seconds : Total : {self._data_wait_total_s:.6f} seconds",
                flush=True,
            )
            yield microbatch

    def microbatch_generator(
        self, data_iterable: Iterable[TrainingMicrobatch]
    ) -> Iterator[TrainingMicrobatch]:
        return super().microbatch_generator(self._timed_batches(data_iterable))

    def train_step(self, data_iterator: Iterator[TrainingMicrobatch]) -> None:
        start = time.perf_counter()
        super().train_step(data_iterator)
        if torch.cuda.is_available():
            torch.cuda.synchronize()
        duration = time.perf_counter() - start
        self._step_wall_s += duration
        if _global_rank() == 0:
            throughput = self._samples_per_step / duration if duration > 0 else 0.0
            print(
                f"Global Rank: 0 | Step: {self.engine.num_completed_steps} | "
                f"Loss: {abs(self._last_loss):.4f} | Step Time: {duration:.4f}s | "
                f"Throughput: {throughput:.2f} samples/s",
                flush=True,
            )

    def train(self) -> None:
        super().train()
        self._print_summary()

    def _print_summary(self) -> None:
        mean_wait = self._fetch_wait_s / self._fetch_count if self._fetch_count else 0.0
        blocked_pct = min(
            100.0, self._fetch_wait_s / max(1e-6, self._step_wall_s) * 100.0
        )
        sep = "-" * 80
        header = (
            "|  Action                                         "
            "|  Mean duration (s) |  Num calls         "
            "|  Total time (s)    |  Percentage %      |"
        )
        row = (
            "|  [_TrainingEpochLoop].train_dataloader_next     "
            f"|  {mean_wait:<17.6f} |  {self._fetch_count:<17d} "
            f"|  {self._fetch_wait_s:<17.6f} |  {blocked_pct:<17.2f} |"
        )
        print(f"{sep}\n{header}\n{sep}\n{row}\n{sep}", flush=True)
        if torch.cuda.is_available() and _global_rank() == 0:
            print(
                f"GPU Memory : Rank : 0 : "
                f"Max Allocated : {torch.cuda.max_memory_allocated() / 1024**3:.2f} GiB : "
                f"Max Reserved : {torch.cuda.max_memory_reserved() / 1024**3:.2f} GiB",
                flush=True,
            )
