"""Llama 3.1 8B Lightning training benchmark: CPU IO simulator, or real GPU
training when GPUs are visible.

On CPU, reproduces the GCS IO pattern (N_NODES x 4 ranks x 16 dataloader
workers, periodic checkpoint writes of the full bf16 8B state dict) without
GPUs. The real Llama model is loaded and held frozen so checkpoint file
sizes match production; GPU compute is replaced by
``time.sleep(SIMULATED_STEP_COMPUTE_SECONDS)`` in ``training_step``.

On GPU (detected via ``torch.cuda.is_available()``), the script runs real
end-to-end training instead: the model is trainable, ``training_step`` does
a real forward/backward pass, and AdamW state builds up from real
``.step()`` calls. Everything else is shared between both modes.

Single-node launch (smoke test):
    torchrun --nproc_per_node=4 --nnodes=1 llama_3_1_8b_cpu_sim.py

Multi-node launch (the production emulator: 2 c4-standard-192 VMs, each
running 4 processes that stand in for GPU chips -- capped at 4/node, down from
8, so a checkpoint-restoring run fits the 720GB host RAM). The Helm chart in
``helm_chart/templates/`` wires up the K8s JobSet and the per-pod launcher;
on each pod it ultimately runs:
    torchrun --nproc_per_node=4 --nnodes=$NNODES --node_rank=$NODE_RANK \\
             --master_addr=$MASTER_ADDR --master_port=$MASTER_PORT \\
             llama_3_1_8b_cpu_sim.py

Required env vars: ``DATASET_PATH``, ``RUN_ID`` (always); ``HF_TOKEN`` only
when ``MODEL_ID`` points at the HuggingFace gated repo (i.e. not gs://).
Optional env vars: ``MODEL_ID`` (default ``meta-llama/Llama-3.1-8B``; may be
``gs://bucket/path`` for a launcher pre-downloaded copy), ``CKPT_WRITE_PATH``,
``MAX_STEPS``, ``CHECKPOINT_WRITE_INTERVAL``, ``TRAINING_STRATEGY``, etc.

``NUM_TRAIN_EPOCHS`` (default 3) bounds the run if ``MAX_STEPS=-1``.
``SHUFFLE_BUFFER_SIZE`` (default 10000) and ``DATALOADER_PREFETCH_FACTOR``
(default 2) are Helm-injected; defaults are for standalone runs.

``TRAINING_STRATEGY`` (default ``ddp``; ``fsdp_sharded`` shards the model and
writes a per-rank sharded/distributed checkpoint; ``fsdp_full`` shards the
model but consolidates to a single rank-0-written checkpoint at save time,
like ``ddp``) selects the parallel-training strategy. A resume
(``CKPT_LOAD_PATH``) must point at a checkpoint produced by the same strategy
-- cross-strategy restore is unsupported.
"""

import datetime
import logging
import os
import sys
import time

import torch.multiprocessing

# forkserver before any other torch import that could spawn workers.
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
from lightning.pytorch.profilers import SimpleProfiler
from lightning.pytorch.strategies import (
    DDPStrategy,
    FSDPStrategy,
    ModelParallelStrategy,
)
from torch.distributed.fsdp import FullyShardedDataParallel, fully_shard
from torch.distributed.fsdp.wrap import wrap
from torch.distributed.tensor.parallel import (
    ColwiseParallel,
    RowwiseParallel,
    parallelize_module,
)
from torchdata.stateful_dataloader import StatefulDataLoader

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

# ---- Config (env-overridable) ---------------------------------------------
preset_max_steps = int(os.getenv("MAX_STEPS", "1000"))
full_pass = preset_max_steps < 0
# Binds if MAX_STEPS=-1.
num_train_epochs = int(os.getenv("NUM_TRAIN_EPOCHS", "3"))
logging.info("num_train_epochs: %d", num_train_epochs)
# Default 1 (not the launcher-overridden 4 this once carried): the launcher and
# the Helm template both set GRADIENT_ACCUMULATION_STEPS=1, so 1 is the
# effective value -- the standalone smoke test should agree rather than
# silently use a different accumulation.
gradient_accumulation_steps = int(os.getenv("GRADIENT_ACCUMULATION_STEPS", "1"))
per_device_train_batch_size = int(os.getenv("PER_DEVICE_TRAIN_BATCH_SIZE", "8"))
dataloader_num_workers = int(os.getenv("DATALOADER_NUM_WORKERS", "16"))
checkpoint_load_path = os.getenv("CKPT_LOAD_PATH", None)
checkpoint_write_interval = int(os.getenv("CHECKPOINT_WRITE_INTERVAL", "25"))
if not full_pass:
    checkpoint_write_interval = min(checkpoint_write_interval, preset_max_steps)
checkpoints_to_keep = int(os.getenv("CKPT_TO_KEEP", "1"))
model_id = os.getenv("MODEL_ID", "meta-llama/Llama-3.1-8B")

shuffle_buffer_size = int(os.getenv("SHUFFLE_BUFFER_SIZE", "10000"))
logging.info("shuffle_buffer_size: %d", shuffle_buffer_size)

shuffle_max_buffer_input_shards = int(
    os.getenv("SHUFFLE_MAX_BUFFER_INPUT_SHARDS", "10")
)
logging.info("shuffle_max_buffer_input_shards: %d", shuffle_max_buffer_input_shards)

SHUFFLE_SEED = 42

dataloader_prefetch_factor = int(os.getenv("DATALOADER_PREFETCH_FACTOR", "2"))
logging.info("dataloader_prefetch_factor: %d", dataloader_prefetch_factor)

# Parallel training strategy. ``ddp`` (default) replicates the frozen model on
# every rank and rank 0 writes the full checkpoint; ``fsdp_sharded`` shards the
# model and writes a sharded (distributed) checkpoint where every rank writes
# its own shard concurrently; ``fsdp_full`` shards the model but consolidates
# to a single rank-0-written checkpoint at save time, like ``ddp``.
# ``model_parallel_*`` selects ModelParallelStrategy (FSDP2 + optional 2D TP);
# *_sharded/*_full encode only the checkpoint format, same as fsdp_*. The mesh
# (TP x DP) comes from the TENSOR_PARALLEL_SIZE/DATA_PARALLEL_SIZE knobs, not
# the name; TP=1 is pure FSDP2. init_variables.sh already enforces
# TP*DP == world_size before provisioning; the assert here is a backstop.
# Validate eagerly -- before the 16 GB model load -- so a typo fails fast.
MODEL_PARALLEL_STRATEGIES = (
    "model_parallel_sharded",
    "model_parallel_full",
)
tensor_parallel_size = int(os.getenv("TENSOR_PARALLEL_SIZE", "4"))
data_parallel_size = int(os.getenv("DATA_PARALLEL_SIZE", "2"))
training_strategy = os.getenv("TRAINING_STRATEGY", "ddp").lower()
if training_strategy not in (
    "ddp",
    "fsdp_sharded",
    "fsdp_full",
    *MODEL_PARALLEL_STRATEGIES,
):
    raise SystemExit(
        "TRAINING_STRATEGY must be 'ddp', 'fsdp_sharded', 'fsdp_full', or one of "
        f"{MODEL_PARALLEL_STRATEGIES} (got {training_strategy!r})."
    )
# Parity with the model_id: line -- a single grep-able config marker per knob.
logging.info("training_strategy: %s", training_strategy)

# Map model_id to the canonical id and log it as ``model_id: <id>``.
# NOTE: this macrobenchmarks pipeline does NOT consume this line -- it derives
# the summary's model_id from calculate.py's ``--model-id`` flag. The line is
# emitted so an HF metadata generator can scrape it via regex
# (``model_id: ([a-zA-Z0-9-]+)``). Computed from the original model_id
# (before the gs:// -> /tmp remap below, which would otherwise log a path
# the regex can't parse).
if "Llama-3.1-8B" in model_id:
    metadata_model_id = "llama3-1-8b"  # Default
else:
    metadata_model_id = "unknown"
logging.info("model_id: %s", metadata_model_id)

# If ``MODEL_ID`` is a GCS path, the launcher pre-downloads the weights to
# ``/tmp/<basename>`` (gcloud storage cp -r). Remap ``model_id`` to the local
# directory and force ``local_files_only`` so transformers does not phone home.
# Without this, 8 ranks (2 nodes x 4 procs) would concurrently pull the
# 16 GB Llama-3.1-8B weights from HuggingFace.
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
# omitted entirely. Strip trailing slash for the same reason as
# ``dataset_path``.
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

# ---- Device: real GPU training vs CPU IO simulation ------------------------
# See module docstring. Read as a bare module-level global everywhere below,
# same as model_id, checkpoints_to_keep, etc.
use_gpu = torch.cuda.is_available()
logging.info("use_gpu: %s", use_gpu)
process_group_backend = "nccl" if use_gpu else "gloo"

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
    """Runs in DataLoader worker processes, so tokenization CPU work overlaps
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
    # Mask padding out of the loss: pad_token == eos_token, so unmasked pad
    # positions would train the GPU path to emit EOS runs and skew the logged
    # loss by each batch's padding ratio. The CPU sim ignores labels entirely.
    tokens["labels"][tokens["attention_mask"] == 0] = -100
    return tokens


def build_train_dataset(
    path,
    *,
    shuffle_buffer_size,
    shuffle_seed,
    max_buffer_input_shards,
    rank,
    world_size,
):
    """Build the streaming dataset, sharded by node.

    - Projection is pushed to GCS via columns=["text"].
    - Shuffling permutes shard list before split_dataset_by_node.
    - max_buffer_input_shards improves shuffle quality but divides num_shards.
      Unless the result is a multiple of world_size, split_dataset_by_node
      falls back to per-sample round-robin and every rank reads the whole
      dataset (world_size x read amplification); warned about below.
    """
    ds = datasets.load_dataset(
        "parquet",
        data_files=f"{path}/*.parquet",
        split="train",
        streaming=True,
        columns=["text"],
    )
    if shuffle_buffer_size > 0:
        ds = ds.shuffle(
            seed=shuffle_seed,
            buffer_size=shuffle_buffer_size,
            max_buffer_input_shards=max_buffer_input_shards,
        )
    logging.info("dataset_num_shards: %d", ds.num_shards)
    if world_size > 1:
        if ds.num_shards % world_size != 0:
            # Not a multiple of world_size: split_dataset_by_node falls back
            # to per-sample round-robin, so every rank reads the full dataset.
            logging.warning(
                "dataset_num_shards=%d is not a multiple of world_size=%d; "
                "every rank will read the entire dataset (%dx read "
                "amplification). Adjust max_buffer_input_shards or the "
                "dataset's shard count.",
                ds.num_shards,
                world_size,
                world_size,
            )
        ds = datasets.distributed.split_dataset_by_node(
            ds, rank=rank, world_size=world_size
        )
    return ds


def build_train_dataloader(ds, *, batch_size, num_workers, prefetch_factor):
    """Stateful loader to resume stream from checkpoint instead of rewinding.

    persistent_workers keeps GCS clients alive across epochs; pin_memory speeds
    host->device copies on GPU.

    HF checkpoints the shuffle buffer's position but not its contents, so a
    resumed run skips shuffle_buffer_size * num_workers samples per rank. That
    is invisible in fixed-step mode; in full-pass mode the epoch ends early, so
    don't compare a resumed full pass against a clean one.
    """
    return StatefulDataLoader(
        ds,
        batch_size=batch_size,
        collate_fn=collate_fn,
        num_workers=num_workers,
        persistent_workers=num_workers > 0,
        prefetch_factor=prefetch_factor if num_workers > 0 else None,
        pin_memory=use_gpu,
    )


# ---- LightningModule ------------------------------------------------------
def _validate_tp_divisibility(config, tp):
    """Fail fast if tensor_parallel_size does not evenly divide the dims TP
    shards. DTensor requires exact divisibility to construct the sharded
    parameters; an indivisible dim otherwise crashes deep inside
    ``parallelize_module``. KV heads are the tightest constraint under GQA.
    """
    checks = {
        "num_attention_heads": config.num_attention_heads,
        "num_key_value_heads": config.num_key_value_heads,
        "intermediate_size": config.intermediate_size,
        "hidden_size": config.hidden_size,
    }
    bad = {name: val for name, val in checks.items() if val % tp != 0}
    if bad:
        raise SystemExit(
            f"TENSOR_PARALLEL_SIZE={tp} does not evenly divide {bad} for "
            f"model_id={model_id!r}; choose a TP that divides all of them."
        )


def _llama_tp_plan():
    """Tensor-parallel plan for one LlamaDecoderLayer.

    Attention q/k/v and MLP gate/up are column-parallel; the output o_proj and
    MLP down_proj are row-parallel. tensor_parallel_size must divide the model's
    head/dim counts; _validate_tp_divisibility enforces that before this plan is
    applied (Llama 3.1 8B has 32 heads / 8 KV heads, divisible by the default 4).
    """
    return {
        "self_attn.q_proj": ColwiseParallel(),
        "self_attn.k_proj": ColwiseParallel(),
        "self_attn.v_proj": ColwiseParallel(),
        "self_attn.o_proj": RowwiseParallel(),
        "mlp.gate_proj": ColwiseParallel(),
        "mlp.up_proj": ColwiseParallel(),
        "mlp.down_proj": RowwiseParallel(),
    }


class LlamaLitModel(pl.LightningModule):
    """On CPU (``use_gpu=False``), holds the real Llama 8B model (frozen)
    for realistic checkpoint size and runs a fake forward via a tiny
    trainable Linear so DDP all-reduce (or FSDP's per-shard gradient sync)
    has something to sync without paying 8B-param collective costs.
    ``training_step`` sleeps for ``SIMULATED_STEP_COMPUTE_SECONDS`` to mimic
    the time a GPU step would take, and AdamW optimizer state is
    artificially materialized so checkpoint sizes stay realistic despite no
    real training happening.

    On GPU (``use_gpu=True``), ``self.model`` is trainable and
    ``training_step`` runs a real forward/backward pass -- nothing is
    mocked, and AdamW state builds up from real ``.step()`` calls.

    Either way, ``self.model``'s parameters end up in the Lightning
    state_dict, and AdamW is configured over ``self.model`` (ddp) or all
    parameters post-shard-wrap (fsdp_sharded/fsdp_full/model_parallel_*).
    When ``ModelCheckpoint`` writes via fsspec to ``gs://...`` the uploaded
    blob is approximately the size of a real bf16 Llama 8B checkpoint with
    optimizer state.
    """

    def __init__(self, model, training_strategy="ddp"):
        super().__init__()
        self.model = model
        self._fsdp = training_strategy in ("fsdp_sharded", "fsdp_full")
        self._model_parallel = training_strategy in MODEL_PARALLEL_STRATEGIES
        self._tensor_parallel = self._model_parallel and tensor_parallel_size > 1
        # GPU trains real Llama params; CPU keeps them frozen and trains a
        # stand-in Linear instead (below).
        for p in self.model.parameters():
            p.requires_grad = use_gpu
        if not use_gpu:
            # Sharded strategies (FSDP1/FSDP2) keep bf16 to match a real
            # checkpoint; DDP replicates in fp32.
            sharded = self._fsdp or self._model_parallel
            trainable_dtype = model.dtype if sharded else torch.float32
            self.trainable = torch.nn.Linear(8, 8).to(trainable_dtype)

    def configure_model(self):
        if self._fsdp:
            # Explicit FSDP1 units keep the CPU fake step from gathering unused
            # frozen model parameters through a LightningModule root wrapper.
            if not isinstance(self.model, FullyShardedDataParallel):
                for index, layer in enumerate(self.model.model.layers):
                    self.model.model.layers[index] = wrap(layer)
                self.model = wrap(self.model)
            if not use_gpu and not isinstance(self.trainable, FullyShardedDataParallel):
                self.trainable = wrap(self.trainable)
            return

        # ModelParallelStrategy applies FSDP2 to the model below.
        if not self._model_parallel:
            return
        mesh = self.device_mesh
        if self._tensor_parallel and mesh["tensor_parallel"].size() > 1:
            _validate_tp_divisibility(self.model.config, tensor_parallel_size)
            tp_mesh = mesh["tensor_parallel"]
            for layer in self.model.model.layers:
                parallelize_module(layer, tp_mesh, _llama_tp_plan())
        dp_mesh = mesh["data_parallel"]
        for layer in self.model.model.layers:
            fully_shard(layer, mesh=dp_mesh)
        fully_shard(self.model, mesh=dp_mesh)

    def training_step(self, batch, batch_idx):
        if use_gpu:
            # Real forward/backward through Llama, driving both the GCS
            # read traffic and the training compute being benchmarked.
            return self.model(**batch).loss
        # Pull the batch out of the dataloader -- this is what drives the
        # GCS read traffic we are benchmarking. The batch contents are then
        # ignored; we sleep to simulate GPU compute.
        del batch
        time.sleep(SIMULATED_STEP_COMPUTE_SECONDS)
        zeros = torch.zeros(1, 8, dtype=self.trainable.weight.dtype)
        # Real loss with a real grad path so backward + DDP all-reduce run.
        # Squared so the loss is always non-negative: the metrics pipeline's
        # step-metrics regex matches "Loss: [0-9.]+" (no leading '-'), so a
        # negative loss would silently drop every step_time/throughput sample.
        # self.trainable is never optimized (configure_optimizers builds AdamW
        # over the frozen self.model), so without the square the loss is a
        # constant whose sign is random per run -- ~50% of runs would emit a
        # negative loss and capture zero step metrics.
        return (self.trainable(zeros) ** 2).sum()

    @staticmethod
    def _materialize_adamw_state(optimizer):
        """Eagerly allocate AdamW moments so checkpoint size is realistic."""
        for group in optimizer.param_groups:
            for p in group["params"]:
                state = optimizer.state[p]
                if state:
                    continue
                # Random, not zero: an all-zero buffer is trivially compressible/
                # dedupable (page merging, a future transport compression layer,
                # etc.), which would let this ~2/3 of the checkpoint transfer
                # faster than the real, non-degenerate floats a trained
                # optimizer actually produces -- skewing the IO benchmark.
                state["step"] = torch.zeros((), dtype=torch.float32)
                state["exp_avg"] = torch.randn_like(
                    p, memory_format=torch.preserve_format
                )
                state["exp_avg_sq"] = torch.rand_like(
                    p, memory_format=torch.preserve_format
                )
                if group["amsgrad"]:
                    state["max_exp_avg_sq"] = torch.rand_like(
                        p, memory_format=torch.preserve_format
                    )

    def configure_optimizers(self):
        params = (
            self.parameters()
            if (self._fsdp or self._model_parallel)
            else self.model.parameters()
        )
        optimizer = torch.optim.AdamW(
            params,
            lr=float(os.getenv("LEARNING_RATE", "2e-5")),
            weight_decay=float(os.getenv("WEIGHT_DECAY", "1e-6")),
        )
        if not use_gpu:
            # GPU builds real AdamW state from real .step() calls; this is
            # CPU-sim-only realism.
            self._materialize_adamw_state(optimizer)
        return optimizer


# ---- Callbacks ------------------------------------------------------------
class DatasetEpochCallback(Callback):
    """Advances HF dataset shuffle seed per epoch (workaround for Lightning + IterableDataset)."""

    def __init__(self, dataset):
        super().__init__()
        self._dataset = dataset

    def on_train_epoch_start(self, trainer, pl_module):
        logging.info(
            "Rank %d: Epoch %d started", trainer.global_rank, trainer.current_epoch
        )
        self._dataset.set_epoch(trainer.current_epoch)

    def on_train_epoch_end(self, trainer, pl_module):
        logging.info(
            "Rank %d: Epoch %d completed", trainer.global_rank, trainer.current_epoch
        )


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

        # Materialize the loss BEFORE reading the clock: on GPU, loss.item()
        # is the device->host sync that blocks until the enqueued
        # forward/backward/optimizer actually finish, so measuring after it
        # makes step_time include real GPU compute (measuring first would
        # collapse step_time to data-wait + kernel-launch overhead). Cheap
        # and harmless on CPU.
        loss = outputs["loss"] if isinstance(outputs, dict) else outputs
        loss_val = loss.item() if isinstance(loss, torch.Tensor) else loss

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

        logging.info(
            "Global Rank: %d | Step: %d | Loss: %.4f | Step Time: %.4fs | "
            "Throughput: %.2f samples/s | Local Throughput: %.2f samples/s | Epoch: %d",
            trainer.global_rank,
            trainer.global_step,
            loss_val,
            step_time,
            global_throughput,
            local_throughput,
            trainer.current_epoch,
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
        # perf_counter's origin is per-process and meaningless outside it.
        # Writes are rank-0-only under ddp/fsdp_full/model_parallel_full but
        # multi-rank under *_sharded (every rank writes its own shard);
        # either way this keeps timestamps on one comparable clock.
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

        if trainer.global_rank == 0:
            try:
                size_bytes = self._measure_checkpoint_bytes(filepath)
                logging.info(
                    "Checkpoint Size : Rank : %d : Step : %d : Bytes : %d : Path: %s",
                    trainer.global_rank,
                    trainer.global_step,
                    size_bytes,
                    filepath,
                )
            except Exception as e:
                logging.warning("Could not measure checkpoint size: %s", e)

    @staticmethod
    def _measure_checkpoint_bytes(filepath):
        fs, path = fsspec.core.url_to_fs(filepath)
        return int(fs.du(path))

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


class LoggedFSDPStrategy(FSDPStrategy):
    """FSDPStrategy with checkpoint restore logging."""

    def load_checkpoint(self, checkpoint_path, *args, **kwargs):
        logging.info(
            "Checkpoint Restore Start : Rank : %d : Start time: %f seconds : Path: %s",
            self.global_rank,
            time.time(),
            checkpoint_path,
        )
        start_time = time.perf_counter()
        checkpoint = super().load_checkpoint(checkpoint_path, *args, **kwargs)
        duration = time.perf_counter() - start_time
        logging.info(
            "Finished restoring checkpoint : Rank : %d : Duration: %.2f seconds : End Time: %.2f seconds : Path: %s",
            self.global_rank,
            duration,
            time.time(),
            checkpoint_path,
        )
        return checkpoint


class LoggedModelParallelStrategy(ModelParallelStrategy):
    """ModelParallelStrategy (FSDP2 / 2D) with checkpoint restore logging."""

    def load_checkpoint(self, checkpoint_path, *args, **kwargs):
        logging.info(
            "Checkpoint Restore Start : Rank : %d : Start time: %f seconds : Path: %s",
            self.global_rank,
            time.time(),
            checkpoint_path,
        )
        start_time = time.perf_counter()
        checkpoint = super().load_checkpoint(checkpoint_path, *args, **kwargs)
        duration = time.perf_counter() - start_time
        logging.info(
            "Finished restoring checkpoint : Rank : %d : Duration: %.2f seconds : End Time: %.2f seconds : Path: %s",
            self.global_rank,
            duration,
            time.time(),
            checkpoint_path,
        )
        if not use_gpu:
            # Restore drops AdamW state for the CPU sim's frozen params;
            # refill so later checkpoints include moments, not just weights.
            # GPU saved real state, which restores normally.
            for optimizer in self.optimizers:
                LlamaLitModel._materialize_adamw_state(optimizer)
        return checkpoint


def build_strategy(name):
    """Construct the parallel-training strategy for ``name``
    (ddp|fsdp_sharded|fsdp_full|model_parallel_*).

    Backend is nccl on GPU, gloo on CPU. The 7200s process-group timeout (vs the
    30-min default) covers long checkpoint save/restore collective barriers.
    """
    pg_kwargs = {
        "process_group_backend": process_group_backend,
        "timeout": datetime.timedelta(seconds=7200),
    }
    if name == "ddp":
        # find_unused_parameters=False: on GPU every real Llama param
        # participates in the forward/backward; on CPU only self.trainable
        # does (Llama params are frozen). Either way nothing is unused.
        return LoggedDDPStrategy(
            **pg_kwargs,
            find_unused_parameters=False,
        )
    if name in ("fsdp_sharded", "fsdp_full"):
        # fsdp_sharded writes sharded checkpoints; fsdp_full writes consolidated.
        # configure_model creates the FSDP1 units explicitly.
        state_dict_type = "sharded" if name == "fsdp_sharded" else "full"
        return LoggedFSDPStrategy(
            **pg_kwargs,
            state_dict_type=state_dict_type,
            use_orig_params=True,
        )
    if name in MODEL_PARALLEL_STRATEGIES:
        # Mesh comes from the TENSOR_PARALLEL_SIZE / DATA_PARALLEL_SIZE knobs;
        # the name encodes only the checkpoint format. TP=1 is pure FSDP2.
        assert tensor_parallel_size * data_parallel_size == world_size, (
            f"TP({tensor_parallel_size}) * DP({data_parallel_size}) must equal "
            f"world_size({world_size}); check _TENSOR_PARALLEL_SIZE/"
            "_DATA_PARALLEL_SIZE vs _NODES x _RANKS_PER_NODE."
        )
        return LoggedModelParallelStrategy(
            data_parallel_size=data_parallel_size,
            tensor_parallel_size=tensor_parallel_size,
            save_distributed_checkpoint=name.endswith("_sharded"),
            **pg_kwargs,
        )
    raise SystemExit(
        f"Unsupported TRAINING_STRATEGY: {name!r} "
        "(use ddp|fsdp_sharded|fsdp_full|model_parallel_*)."
    )


# ---- Profiler ---------------------------------------------------------------
# Lightning brackets every span where the fit loop blocks on the train
# dataloader with exactly two profiler actions, disjoint by construction:
#   - setup_train_dataloader: each iterator (re-)creation -- worker spawn,
#     StatefulDataLoader resume fast-forward, and the first-batch prefetch --
#     at fit start, on resume, and (with the fork fix, see below) at every
#     epoch boundary.
#   - [_TrainingEpochLoop].train_dataloader_next: every other blocking
#     next(iterator) fetch, including the epoch-end StopIteration probe.
# Their sum is the total time the dataloader blocked this rank. Requires the
# zhixiangli/pytorch-lightning fork past the epoch-boundary iter() fix; on
# stock lightning <= 2.6.2 the sum misses only the epoch >= 1 iterator
# re-creation handshake (the epoch >= 1 prefetch then lands in
# train_dataloader_next instead, so nothing is double counted either way).
DATA_WAIT_ACTIONS = (
    "setup_train_dataloader",
    "[_TrainingEpochLoop].train_dataloader_next",
)


class DataWaitProfiler(SimpleProfiler):
    """SimpleProfiler that also logs each dataloader-blocking span in real time.

    The end-of-fit SimpleProfiler report only prints from local-rank-0 pods and
    is lost entirely when a pod is preempted mid-run; these per-fetch lines make
    every rank's running total recoverable from its log tail at any point. The
    ``Fetch`` counter lets the metrics pipeline align fetch k across ranks and
    take the max -- the per-step stall that actually gates the job under
    the strategy's every-step gradient sync. (Off by one per-step since the
    N+1 fetch happens inside step N's window; totals are exact.)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.data_wait_total = 0.0
        self.data_wait_count = 0

    def stop(self, action_name):
        super().stop(action_name)
        if action_name not in DATA_WAIT_ACTIONS:
            return
        duration = self.recorded_durations[action_name][-1]
        self.data_wait_total += duration
        self.data_wait_count += 1
        logging.info(
            "Data Wait : Rank : %s : Fetch : %d : Action : %s : "
            "Duration : %.6f seconds : Total : %.6f seconds",
            os.environ.get("RANK", "0"),
            self.data_wait_count,
            action_name,
            duration,
            self.data_wait_total,
        )


if __name__ == "__main__":
    # ---- Verify gcsfs is the active fsspec backend for "gs" ----------------
    try:
        fs = fsspec.filesystem("gs")
        logging.info("[SYSTEM CHECK] fsspec 'gs' backend class: %s", type(fs))
        logging.info(
            "[SYSTEM CHECK] If this says 'gcsfs.core.GCSFileSystem', you are using gcsfs."
        )
    except Exception as e:
        logging.info("[SYSTEM CHECK] Failed to load GS filesystem: %s", e)

    # ---- Dataset: HuggingFace streaming parquet -----------------------------
    # This is the GCS read pattern under test.
    logging.info("[INFO] Loading %s dataset", dataset_path)
    logging.info("[INFO] Using HF dataloader")
    load_start = time.perf_counter()
    ds = build_train_dataset(
        dataset_path,
        shuffle_buffer_size=shuffle_buffer_size,
        shuffle_seed=SHUFFLE_SEED,
        max_buffer_input_shards=shuffle_max_buffer_input_shards,
        rank=int(os.environ.get("RANK", "0")),
        world_size=world_size,
    )
    logging.info(
        "Dataset Build : Rank : %s : Duration : %.6f seconds : Path: %s",
        os.environ.get("RANK", "0"),
        time.perf_counter() - load_start,
        dataset_path,
    )
    train_loader = build_train_dataloader(
        ds,
        batch_size=per_device_train_batch_size,
        num_workers=dataloader_num_workers,
        prefetch_factor=dataloader_prefetch_factor,
    )

    # ---- Model: real Llama 8B in bf16 ---------------------------------------
    # Every rank loads its own full copy here; sharded strategies shard it
    # later in configure_model/build_strategy. Real weights so the
    # state_dict serialized at checkpoint time is a realistic size. Frozen on
    # CPU (IO sim), trainable on GPU (real training) -- see LlamaLitModel.
    # use_cache=False: a KV cache is pointless for training and just wastes
    # GPU memory; harmless on CPU, where the model never forwards.
    model = transformers.AutoModelForCausalLM.from_pretrained(
        model_id,
        torch_dtype=torch.bfloat16,
        local_files_only=use_local_files_only,
        use_cache=False,
    )

    # ---- Callbacks ----------------------------------------------------------
    # cpu_stats=True: on the CPU accelerator this matches the default; on GPU
    # it logs host CPU stats ALONGSIDE the GPU stats, which the default
    # (cpu_stats=None) would drop. Dataloader tokenization is host-side work,
    # so host CPU saturation stays a first-class diagnostic for this IO
    # benchmark in both modes.
    callbacks = [
        DeviceStatsMonitor(cpu_stats=True),
        DatasetEpochCallback(ds),
    ]
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

    # ---- Strategy: ddp / fsdp_* / model_parallel_* --------------------------
    strategy = build_strategy(training_strategy)

    # ---- Trainer ------------------------------------------------------------
    # devices=local_world_size dynamically matches the local rank count
    # (e.g., 4 devices with torchrun --nproc_per_node=4).
    # ``precision="bf16-mixed"`` drives real mixed-precision training on
    # GPU; on CPU it's inert since training_step never forwards the model.
    trainer = pl.Trainer(
        max_epochs=num_train_epochs,
        num_nodes=num_nodes,
        max_steps=-1 if full_pass else preset_max_steps,
        accumulate_grad_batches=gradient_accumulation_steps,
        precision="bf16-mixed",
        enable_checkpointing=bool(checkpoint_write_path),
        callbacks=callbacks,
        accelerator="gpu" if use_gpu else "cpu",
        devices=local_world_size,
        limit_test_batches=50,
        limit_val_batches=32,
        log_every_n_steps=1,
        strategy=strategy,
        profiler=DataWaitProfiler(),
        enable_progress_bar=False,
    )

    if checkpoint_load_path:
        logging.info("[INFO] Resuming from checkpoint: %s", checkpoint_load_path)
    else:
        checkpoint_load_path = None

    # Pass the strategy so the module can adapt under FSDP.
    lit_model = LlamaLitModel(model, training_strategy=training_strategy)

    logging.info("[INFO] Training Started.")

    trainer.fit(lit_model, train_loader, ckpt_path=checkpoint_load_path)
    logging.info("[INFO] Training Completed.")
