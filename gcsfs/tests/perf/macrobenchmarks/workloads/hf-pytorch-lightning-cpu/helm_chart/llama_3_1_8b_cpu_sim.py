"""CPU-only IO simulator for the Llama 3.1 8B Lightning training benchmark.

Reproduces the GCS IO pattern (N_NODES x 4 ranks x 16 dataloader workers,
periodic checkpoint writes of the full bf16 8B state dict) without GPUs. The
real Llama model is loaded and held frozen so checkpoint file sizes match
production; GPU compute is replaced by ``time.sleep(SIMULATED_STEP_COMPUTE_SECONDS)``
in ``training_step``.

Single-node launch (smoke test):
    torchrun --nproc_per_node=4 --nnodes=1 llama_3_1_8b_cpu_sim.py

Multi-node launch (the production emulator: 2 c4-standard-192 VMs, each
running 4 processes that stand in for GPU chips -- capped at 4/node, down from
8, so a checkpoint-restoring run fits the 720GB host RAM). The Helm chart in
``emulated/templates/`` wires up the K8s JobSet and the per-pod launcher;
on each pod it ultimately runs:
    torchrun --nproc_per_node=4 --nnodes=$NNODES --node_rank=$NODE_RANK \\
             --master_addr=$MASTER_ADDR --master_port=$MASTER_PORT \\
             llama_3_1_8b_cpu_sim.py

Required env vars: ``DATASET_PATH``, ``RUN_ID`` (always); ``HF_TOKEN`` only
when ``MODEL_ID`` points at the HuggingFace gated repo (i.e. not gs://).
Optional env vars: ``MODEL_ID`` (default ``meta-llama/Llama-3.1-8B``; may be
``gs://bucket/path`` for a launcher pre-downloaded copy), ``CKPT_WRITE_PATH``,
``MAX_STEPS``, ``CHECKPOINT_WRITE_INTERVAL``, etc.
"""

import logging
import os
import sys
import time
from datetime import timedelta

import torch.multiprocessing

# Match the original training script: forkserver before any other torch import
# that could spawn workers.
try:
    torch.multiprocessing.set_start_method("forkserver", force=True)
except RuntimeError:
    pass  # context already set

import datasets
import datasets.distributed
import fsspec
import lightning.pytorch as pl
import torch
import transformers
from lightning.pytorch.callbacks import Callback, DeviceStatsMonitor, ModelCheckpoint
from lightning.pytorch.loops.fetchers import _PrefetchDataFetcher
from lightning.pytorch.loops.fit_loop import _FitLoop as FitLoop
from lightning.pytorch.strategies import DDPStrategy
from torch.utils.data import DataLoader

# ---- Logging --------------------------------------------------------------
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
storage_log_level = os.getenv("GCSFS_LOG_LEVEL", "INFO").upper()
if storage_log_level == "TRACE":
    storage_log_level = "DEBUG"
logging.getLogger("gcsfs").setLevel(storage_log_level)
logging.getLogger("fsspec").setLevel(storage_log_level)

run_id = os.environ.get("RUN_ID")
if not run_id:
    raise SystemExit("RUN_ID env var is required.")

log_format = (
    "%(asctime)s - %(levelname)s - %(name)s - [Thread: %(thread)d] - %(message)s"
)
logging.basicConfig(
    format=log_format,
    level=log_level,
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ---- Simulated compute ----------------------------------------------------
# Per-step stand-in for GPU forward+backward in training_step. Configurable via
# SIMULATED_STEP_COMPUTE_SECONDS (default 1.0) and recorded in the run summary.
SIMULATED_STEP_COMPUTE_SECONDS = float(
    os.getenv("SIMULATED_STEP_COMPUTE_SECONDS", "1.0")
)
# Single grep-able config marker per knob (parity with the model_id: line).
logging.info("simulated_step_compute_seconds: %s", SIMULATED_STEP_COMPUTE_SECONDS)

# ---- Config (env-overridable, mirrors original script's pattern) ----------
preset_max_steps = int(os.getenv("MAX_STEPS", "1000"))
# Default 1 (not the launcher-overridden 4 this once carried): the launcher and
# the Helm template both set GRADIENT_ACCUMULATION_STEPS=1 to match the GPU
# a4_v1 run, so 1 is the effective value -- the standalone smoke test should
# agree rather than silently use a different accumulation.
gradient_accumulation_steps = int(os.getenv("GRADIENT_ACCUMULATION_STEPS", "1"))
per_device_train_batch_size = int(os.getenv("PER_DEVICE_TRAIN_BATCH_SIZE", "8"))
dataloader_num_workers = int(os.getenv("DATALOADER_NUM_WORKERS", "16"))
checkpoint_load_path = os.getenv("CKPT_LOAD_PATH", None)
checkpoint_write_interval = int(os.getenv("CHECKPOINT_WRITE_INTERVAL", "25"))
checkpoint_write_interval = min(checkpoint_write_interval, preset_max_steps)
checkpoints_to_keep = int(os.getenv("CKPT_TO_KEEP", "1"))
model_id = os.getenv("MODEL_ID", "meta-llama/Llama-3.1-8B")

# Parallel training strategy. ``ddp`` (default) replicates the frozen model on
# every rank and rank 0 writes the full checkpoint. Validate eagerly -- before
# the 16 GB model load -- so a typo fails fast instead of after a long download.
# (The ``fsdp`` strategy is added by the fsdp-cpu-macrobench branch.)
training_strategy = os.getenv("TRAINING_STRATEGY", "ddp").lower()
if training_strategy not in ("ddp",):
    raise SystemExit(f"TRAINING_STRATEGY must be 'ddp' (got {training_strategy!r}).")
# Parity with the model_id: line -- a single grep-able config marker per knob.
logging.info("training_strategy: %s", training_strategy)

# Map model_id to the canonical id and log it as ``model_id: <id>``.
# NOTE: this macrobenchmarks pipeline does NOT consume this line -- it derives
# the summary's model_id from calculate.py's ``--model-id`` flag. The line is
# emitted purely for parity with the GPU benchmark (a4_v1/llama_3_1_8b.py) and
# the tessellations HF metadata generator that scrapes it (regex
# ``model_id: ([a-zA-Z0-9-]+)``), so GPU and CPU run logs stay identical.
# The benchmark class sets model_id=None on purpose. Computed from the original
# model_id (before the gs:// -> /tmp remap below, which would otherwise log a
# path the regex can't parse); kept in sync with a4_v1/llama_3_1_8b.py so the
# GPU and CPU-emulated benchmarks report the same model_id.
if "Llama-3.1-8B" in model_id:
    metadata_model_id = "llama3-1-8b"  # Default
else:
    metadata_model_id = "unknown"
logging.info("model_id: %s", metadata_model_id)

# If ``MODEL_ID`` is a GCS path, the launcher pre-downloads the weights to
# ``/tmp/<basename>`` (gcloud storage cp -r). Remap ``model_id`` to the local
# directory and force ``local_files_only`` so transformers does not phone home.
# Matches a4_v1/llama_3_1_8b.py exactly so behavior is consistent across the
# GPU and CPU-emulated benchmarks. Without this, 8 ranks (2 nodes x 4 procs)
# would concurrently pull the 16 GB Llama-3.1-8B weights from HuggingFace.
use_local_files_only = False
if model_id.startswith("gs://"):
    use_local_files_only = True
    dir_name = os.path.basename(model_id.rstrip("/"))
    model_id = os.path.join("/tmp", dir_name)

# Required: dataset path. Fail fast if unset. Strip trailing slash so the
# downstream glob doesn't produce ``gs://bucket/dir//*.parquet``, which
# behaves inconsistently across fsspec versions.
dataset_path = os.environ.get("DATASET_PATH")
if not dataset_path:
    raise SystemExit(
        "DATASET_PATH env var is required (e.g. gs://your-bucket/parquet-dir)."
    )
dataset_path = dataset_path.rstrip("/")

# HF token is only needed when weights are pulled from the HuggingFace gated
# repo. If ``MODEL_ID`` is a GCS path, the launcher has already downloaded the
# weights locally and ``local_files_only=True`` is set below, so no token is
# required.
if not use_local_files_only and not os.environ.get("HF_TOKEN"):
    raise SystemExit(
        "HF_TOKEN env var is required when MODEL_ID is a HuggingFace repo "
        "(Llama-3.1-8B is gated). Set MODEL_ID=gs://... to use a "
        "pre-downloaded copy instead."
    )

# Optional: checkpoint write path. If unset, the checkpoint callback is
# omitted entirely (matches the original behavior). Strip trailing slash for
# the same reason as ``dataset_path``.
checkpoint_write_path = os.getenv("CKPT_WRITE_PATH")
if checkpoint_write_path:
    checkpoint_write_path = checkpoint_write_path.rstrip("/")

# torchrun-provided env (defaults so the module is importable outside torchrun).
# Note: torchrun sets RANK, LOCAL_RANK, WORLD_SIZE, LOCAL_WORLD_SIZE,
# MASTER_ADDR, MASTER_PORT -- but NOT NNODES (that's a torchrun CLI flag and
# doesn't propagate to env). Derive num_nodes from WORLD_SIZE / LOCAL_WORLD_SIZE
# so multi-node launches are reported correctly.
world_size = int(os.environ.get("WORLD_SIZE", "1"))
local_world_size = int(os.environ.get("LOCAL_WORLD_SIZE", "1"))
num_nodes = max(1, world_size // local_world_size)
global_batch_size = (
    per_device_train_batch_size * gradient_accumulation_steps * world_size
)
logging.info("global_batch_size: %d", global_batch_size)

# ---- Tokenizer ------------------------------------------------------------
# Real Llama tokenizer. Requires HF_TOKEN env var when downloading from the
# HuggingFace gated repo; when ``MODEL_ID=gs://...`` the launcher has already
# placed the tokenizer files alongside the weights, so ``local_files_only``
# avoids any network access from this process.
tokenizer = transformers.AutoTokenizer.from_pretrained(
    model_id, local_files_only=use_local_files_only
)
if tokenizer.pad_token is None:
    tokenizer.pad_token = tokenizer.eos_token


def collate_fn(examples):
    """Identical to the original training script's collate_fn.

    Runs in DataLoader worker processes, so tokenization CPU work overlaps
    with the next batch's GCS reads -- this is the IO-overlap behavior we
    care about preserving.
    """
    tokens = tokenizer(
        [ex["text"] for ex in examples],
        return_tensors="pt",
        padding="longest",
        truncation=True,
        max_length=512,
    )
    tokens["labels"] = tokens["input_ids"].clone()
    return tokens


# ---- LightningModule ------------------------------------------------------
class LlamaLitModel(pl.LightningModule):
    """Holds the real Llama 8B model (frozen) for realistic checkpoint size;
    runs a fake forward via a tiny trainable Linear so DDP all-reduce has
    something to sync without paying 8B-param collective costs.

    ``training_step`` sleeps for ``SIMULATED_STEP_COMPUTE_SECONDS`` to mimic the time
    a GPU step would take. ``self.model``'s parameters end up in the
    Lightning state_dict, and AdamW is configured over ``self.model`` with
    materialized optimizer state. When ``ModelCheckpoint`` writes via fsspec
    to ``gs://...`` the uploaded blob is approximately the size of a real
    bf16 Llama 8B checkpoint with optimizer state.
    """

    def __init__(self, model):
        super().__init__()
        self.model = model
        for p in self.model.parameters():
            p.requires_grad = False
        # Tiny DDP-trainable params; the only thing all-reduce touches.
        self.trainable = torch.nn.Linear(8, 8)

    def training_step(self, batch, batch_idx):
        # Pull the batch out of the dataloader -- this is what drives the
        # GCS read traffic we are benchmarking. The batch contents are then
        # ignored; we sleep to simulate GPU compute.
        del batch
        time.sleep(SIMULATED_STEP_COMPUTE_SECONDS)
        # Real loss with a real grad path so backward + DDP all-reduce run.
        # Squared so the loss is always non-negative: the metrics pipeline's
        # step-metrics regex matches "Loss: [0-9.]+" (no leading '-'), so a
        # negative loss would silently drop every step_time/throughput sample.
        # self.trainable is never optimized (configure_optimizers builds AdamW
        # over the frozen self.model), so without the square the loss is a
        # constant whose sign is random per run -- ~50% of runs would emit a
        # negative loss and capture zero step metrics.
        return (self.trainable(torch.zeros(1, 8)) ** 2).sum()

    @staticmethod
    def _materialize_adamw_state(optimizer):
        """Eagerly allocate AdamW moments so checkpoint size is realistic."""
        for group in optimizer.param_groups:
            for p in group["params"]:
                state = optimizer.state[p]
                if state:
                    continue
                state["step"] = torch.zeros((), dtype=torch.float32)
                state["exp_avg"] = torch.zeros_like(
                    p, memory_format=torch.preserve_format
                )
                state["exp_avg_sq"] = torch.zeros_like(
                    p, memory_format=torch.preserve_format
                )
                if group["amsgrad"]:
                    state["max_exp_avg_sq"] = torch.zeros_like(
                        p, memory_format=torch.preserve_format
                    )

    def configure_optimizers(self):
        optimizer = torch.optim.AdamW(
            self.model.parameters(),
            lr=float(os.getenv("LEARNING_RATE", "2e-5")),
            weight_decay=float(os.getenv("WEIGHT_DECAY", "1e-6")),
        )
        self._materialize_adamw_state(optimizer)
        return optimizer


# ---- Callbacks ------------------------------------------------------------
# Kept in sync with a4_v1/llama_3_1_8b.py so log lines and metric names stay
# identical to the production benchmark.
class StepTimeCallback(Callback):
    """Logs ``step_time`` and ``throughput`` every optimizer step."""

    def __init__(self):
        super().__init__()
        self.ckpt_time = 0.0

    def on_train_start(self, trainer, pl_module):
        # Start timer at the beginning of the training to capture the first batch's data loading time
        self.start_time = time.perf_counter()
        self.ckpt_time = 0.0

    def on_train_batch_end(self, trainer, pl_module, outputs, batch, batch_idx):
        # Only emit metrics on micro-batches that complete an optimizer step;
        # otherwise step_time would cover a single micro-batch while
        # global_batch_size counts the whole accumulation window, inflating
        # throughput by gradient_accumulation_steps.
        if (batch_idx + 1) % trainer.accumulate_grad_batches != 0:
            return

        # Calculate step time excluding the checkpointing time
        step_time = time.perf_counter() - self.start_time - self.ckpt_time

        per_rank_batch_size = (
            per_device_train_batch_size * trainer.accumulate_grad_batches
        )
        local_throughput = per_rank_batch_size / step_time
        global_throughput = global_batch_size / step_time

        pl_module.log("step_time", step_time)
        pl_module.log("local_throughput", local_throughput)
        pl_module.log("global_throughput", global_throughput)

        loss = outputs["loss"] if isinstance(outputs, dict) else outputs
        loss_val = loss.item() if isinstance(loss, torch.Tensor) else loss
        logging.info(
            "Global Rank: %d | Step: %d | Loss: %.4f | Step Time: %.4fs | "
            "Throughput: %.2f samples/s | Local Throughput: %.2f samples/s",
            trainer.global_rank,
            trainer.global_step,
            loss_val,
            step_time,
            global_throughput,
            local_throughput,
        )

        # Reset the timer for the next step, capturing its data loading time.
        self.start_time = time.perf_counter()
        self.ckpt_time = 0.0


class LoggedModelCheckpoint(ModelCheckpoint):
    """ModelCheckpoint wrapper that logs save/delete duration.

    Under DDP, only rank 0 actually writes the checkpoint; that single
    upload of the bf16 Llama 8B state_dict (~16 GB) to gs:// is the
    headline IO event we want to time.
    """

    def _save_checkpoint(self, trainer, filepath):
        # Log wall-clock time.time() (not perf_counter) for the "Start time"
        # absolute timestamp the metrics parser pairs across log lines:
        # perf_counter's origin is per-process and meaningless outside it. Writes
        # are rank-0 only so this is less load-bearing than the restore path
        # below, but keeps every checkpoint timestamp on one comparable clock.
        logging.info(
            "Checkpoint Save : Rank: %d : Step: %d : Start time: %f seconds: Path: %s",
            trainer.global_rank,
            trainer.global_step,
            time.time(),
            filepath,
        )
        start_time = time.perf_counter()
        super()._save_checkpoint(trainer, filepath)
        duration = time.perf_counter() - start_time

        # Accumulate checkpointing time to be excluded from step time
        for callback in trainer.callbacks:
            if isinstance(callback, StepTimeCallback):
                callback.ckpt_time += duration

        logging.info(
            "Finished saving checkpoint to %s in %.2f seconds for global_step %d from rank %d",
            filepath,
            duration,
            trainer.global_step,
            trainer.global_rank,
        )

    def _remove_checkpoint(self, trainer, filepath):
        logging.info(
            "Checkpoint Delete Start : Rank: %d : Step: %d : Path: %s",
            trainer.global_rank,
            trainer.global_step,
            filepath,
        )
        start_time = time.perf_counter()
        super()._remove_checkpoint(trainer, filepath)
        duration = time.perf_counter() - start_time

        # Accumulate checkpointing time to be excluded from step time
        for callback in trainer.callbacks:
            if isinstance(callback, StepTimeCallback):
                callback.ckpt_time += duration

        logging.info(
            "Finished deleting checkpoint %s in %.2f seconds for global_step %d from rank %d",
            filepath,
            duration,
            trainer.global_step,
            trainer.global_rank,
        )


class LoggedDDPStrategy(DDPStrategy):

    def load_checkpoint(self, checkpoint_path, weights_only: bool = False, **kwargs):
        # Under DDP every rank restores, and calc_restore_metrics aggregates the
        # distributed restore as max(end) - min(start) ACROSS ranks. perf_counter
        # is monotonic-from-boot and per-machine, so mixing ranks on different
        # nodes (the default 2-node topology) produces a meaningless span. Log
        # wall-clock time.time() for the absolute Start/End timestamps so the
        # cross-node span is valid (NTP-synced); duration stays on perf_counter,
        # a within-process elapsed measurement.
        logging.info(
            "Checkpoint Restore Start : Rank : %d : Start time: %f seconds : Path: %s",
            self.global_rank,
            time.time(),
            checkpoint_path,
        )
        start_time = time.perf_counter()
        checkpoint = super().load_checkpoint(checkpoint_path, weights_only, **kwargs)
        duration = time.perf_counter() - start_time
        logging.info(
            "Finished restoring checkpoint : Rank : %d : Duration: %.2f seconds : End Time: %.2f seconds : Path: %s",
            self.global_rank,
            duration,
            time.time(),
            checkpoint_path,
        )
        return checkpoint


def build_strategy(name):
    """Construct the parallel-training strategy for ``name`` (currently ddp).

    Uses the gloo CPU backend and a 600s collective timeout so a stuck rank
    surfaces a Gloo timeout (which names the missing rank) within ~10 min
    instead of hanging. (The ``fsdp`` branch is added by fsdp-cpu-macrobench.)
    """
    timeout = timedelta(seconds=600)
    if name == "ddp":
        # find_unused_parameters=False: the frozen Llama params have
        # requires_grad=False, so only self.trainable participates in DDP
        # autograd, and it is fully used -- no unused parameters.
        return LoggedDDPStrategy(
            timeout=timeout,
            process_group_backend="gloo",
            find_unused_parameters=False,
        )
    raise SystemExit(f"Unsupported TRAINING_STRATEGY: {name!r} (use ddp).")


if __name__ == "__main__":
    # ---- Verify gcsfs is the active fsspec backend for "gs" ----------------
    # Matches the original benchmark's startup self-check; keeps the same log
    # lines so downstream log parsing isn't disturbed.
    try:
        fs = fsspec.filesystem("gs")
        logging.info("[SYSTEM CHECK] fsspec 'gs' backend class: %s", type(fs))
        logging.info(
            "[SYSTEM CHECK] If this says 'gcsfs.core.GCSFileSystem', you are using gcsfs."
        )
    except Exception as e:
        logging.info("[SYSTEM CHECK] Failed to load GS filesystem: %s", e)

    # ---- Dataset: HuggingFace streaming parquet -----------------------------
    # Identical to the original's HF reader branch -- this is the GCS read
    # pattern under test.
    logging.info("[INFO] Loading %s dataset", dataset_path)
    logging.info("[INFO] Using HF dataloader")
    load_start = time.perf_counter()
    ds = datasets.load_dataset(
        "parquet",
        data_files=f"{dataset_path}/*.parquet",
        split="train",
        streaming=True,
    )
    logging.info(
        f"[INFO] HF dataloader prepared in {time.perf_counter() - load_start:.4f}s"
    )
    # Shard the streaming dataset across DDP ranks. torchrun sets RANK and
    # WORLD_SIZE before Python starts, so reading from env works at this point
    # (torch.distributed isn't initialized until trainer.fit). Without this,
    # every rank iterates the same parquet files -- 8x the GCS read traffic
    # and duplicate training samples.
    if world_size > 1:
        ds = datasets.distributed.split_dataset_by_node(
            ds,
            rank=int(os.environ["RANK"]),
            world_size=world_size,
        )
    train_loader = DataLoader(
        ds,
        batch_size=per_device_train_batch_size,
        collate_fn=collate_fn,
        num_workers=dataloader_num_workers,
        persistent_workers=dataloader_num_workers > 0,
    )

    # ---- Model: real Llama 8B in bf16, frozen -------------------------------
    # Each rank holds its own copy (DDP replicates). Real weights so the
    # state_dict serialized at checkpoint time matches production size.
    model = transformers.AutoModelForCausalLM.from_pretrained(
        model_id,
        torch_dtype=torch.bfloat16,
        local_files_only=use_local_files_only,
    )

    # ---- Callbacks ----------------------------------------------------------
    callbacks = [DeviceStatsMonitor(cpu_stats=True)]
    if checkpoint_write_path:
        callbacks.append(
            LoggedModelCheckpoint(
                dirpath=f"{checkpoint_write_path}/{run_id}/",
                filename="llama-{epoch:02d}-{step:02d}",
                every_n_train_steps=checkpoint_write_interval,
                save_top_k=checkpoints_to_keep,
                save_last=False,
                monitor="step",
                mode="max",
            )
        )
    callbacks.append(StepTimeCallback())

    # ---- Strategy: DDP on CPU via gloo --------------------------------------
    # Selected by TRAINING_STRATEGY (default ddp). The 600s collective timeout
    # surfaces a stuck rank (which Gloo names) within ~10 min instead of
    # hanging; it is comfortably above the worst-case first-batch latency
    # (DataLoader worker cold start + initial GCS streaming reads).
    strategy = build_strategy(training_strategy)

    # ---- Trainer ------------------------------------------------------------
    # accelerator="cpu" + devices=local_world_size dynamically matches the
    # local rank count (e.g., 4 devices with torchrun --nproc_per_node=4).
    # ``precision="bf16-mixed"`` is the closest CPU equivalent of the original
    # "bf16" setting; since training_step doesn't actually forward through
    # the Llama model, CPU bf16 op limitations don't affect correctness.
    trainer = pl.Trainer(
        max_epochs=1,
        num_nodes=num_nodes,
        max_steps=preset_max_steps,
        accumulate_grad_batches=gradient_accumulation_steps,
        precision="bf16-mixed",
        enable_checkpointing=bool(checkpoint_write_path),
        callbacks=callbacks,
        accelerator="cpu",
        devices=local_world_size,
        limit_test_batches=50,
        limit_val_batches=32,
        log_every_n_steps=1,
        strategy=strategy,
        profiler="simple",
        enable_progress_bar=False,
    )

    if checkpoint_load_path:
        logging.info("[INFO] Resuming from checkpoint: %s", checkpoint_load_path)
    else:
        checkpoint_load_path = None

    # TODO: These are underscore private APIs, which may break during a Lightning upgrade.
    # We should consider contributing what we need into Lightning.
    # Tracked in: https://github.com/Lightning-AI/pytorch-lightning/pull/21776
    # ==============================================================================
    # PROFILER HOOK 1: Profile the entire setup_data() phase
    # ==============================================================================
    original_setup_data = FitLoop.setup_data

    def profiled_setup_data(self, *args, **kwargs):
        rank = self.trainer.global_rank
        logging.info(f"[RANK {rank}] [PROFILER] FitLoop.setup_data started")
        # We use the PL Profiler so this appears directly in the FIT Profiler Report
        with self.trainer.profiler.profile("FitLoop.setup_data (Data loading)"):
            return original_setup_data(self, *args, **kwargs)

    FitLoop.setup_data = profiled_setup_data

    # ==============================================================================
    # PROFILER HOOK 2: Isolate the iter() call that spawns the workers
    # ==============================================================================
    original_fetcher_iter = _PrefetchDataFetcher.__iter__

    def profiled_fetcher_iter(self):
        start_time = time.perf_counter()

        # This triggers the actual worker forks, gRPC init, and initial file opens
        result = original_fetcher_iter(self)

        duration = time.perf_counter() - start_time
        # We log this to the console immediately for real-time visibility
        rank = os.environ.get("RANK", "0")
        logging.info(
            f"[RANK {rank}] [PROFILER] _PrefetchDataFetcher.__iter__ "
            f"(Worker Spawn and Data Loading) took {duration:.4f} seconds."
        )

        return result

    _PrefetchDataFetcher.__iter__ = profiled_fetcher_iter
    # ==============================================================================

    logging.info("[INFO] Training Started.")

    trainer.fit(LlamaLitModel(model), train_loader, ckpt_path=checkpoint_load_path)
    logging.info("[INFO] Training Completed.")
