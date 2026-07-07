# GCSFS Macrobenchmarks

## Introduction

The GCSFS macrobenchmark is an end-to-end, training-shaped performance test. It
runs a **Llama 3.1 8B PyTorch-Lightning CPU simulation** that reproduces the
Google Cloud Storage IO pattern of a real distributed fine-tuning job -- a
streaming parquet dataset read plus periodic full-state-dict checkpoint writes
and restores -- **without requiring GPUs**. Every gs:// access flows through
`gcsfs`/`fsspec`, so the benchmark measures how a *specific `gcsfs` build*
behaves under a realistic ML workload.

The simulation loads the **real** Llama 3.1 8B weights (held frozen) so the
serialized checkpoints are production-sized, and replaces GPU compute with a
configurable `time.sleep(...)` per training step. This isolates storage IO --
the thing under test -- from accelerator compute.

> **This README describes the workload itself (what it is and what it
> measures).** To actually run the benchmark and collect metrics, use the Cloud
> Build automation documented in
> [`cloudbuild/macrobenchmarks/README.md`](../../../../cloudbuild/macrobenchmarks/README.md).

## Workload architecture

A run is a Kubernetes JobSet on a GKE cluster, deployed by a Helm chart. The
execution chain, from the outside in:

1. **Helm chart** (`workloads/hf-pytorch-lightning-cpu/helm_chart/`) renders the
   Kubernetes objects and injects the run's configuration (steps, checkpoint
   interval, batch size, training strategy, gs:// paths, ...) from
   `values_base.yaml` plus `--set` overrides.
2. **JobSet** schedules one pod per node onto the dedicated node pool (pinned via
   `nodeSelector`), wiring up the multi-node rendezvous (`MASTER_ADDR`,
   `MASTER_PORT`, `NODE_RANK`, ...).
3. **`launcher.sh`** runs inside each pod: it installs the requested Python
   packages (including the `gcsfs` build under test), pre-downloads the model if
   `MODEL_ID` is a `gs://` path, and finally invokes `torchrun`.
4. **`torchrun`** starts the per-node ranks (processes that stand in for GPU
   chips) and launches the simulation.
5. **`llama_3_1_8b_cpu_sim.py`** is the actual workload: it streams the parquet
   dataset (sharded per rank), holds the frozen Llama model, sleeps for the
   simulated compute time each step, and writes/restores full-state-dict
   checkpoints through `gcsfs`.

## Training strategies

`TRAINING_STRATEGY` selects the parallel-training strategy, which changes the
checkpoint IO shape. A resume must point at a checkpoint produced by the *same*
strategy -- cross-strategy restore is unsupported.

| Strategy       | Model sharding | Checkpoint IO exercised |
| :------------- | :------------- | :---------------------- |
| `ddp`          | Replicated on every rank | Single consolidated checkpoint written by rank 0. |
| `fsdp_sharded` | Sharded across ranks | Per-rank sharded/distributed checkpoint (every rank writes its shard). |
| `fsdp_full`    | Sharded across ranks | Consolidated to a single rank-0-written checkpoint at save time, like `ddp`. |

## What this benchmark measures

The run emits one flat summary row per execution. Metrics are grouped into the
families below. (MFU/TFLOPs are intentionally **excluded** -- this benchmark is
about storage IO, not compute efficiency.) The concrete BigQuery column names
for each family live in the automation README's schema section.

| Metric family        | What it captures | What it isolates about GCS |
| :------------------- | :--------------- | :------------------------- |
| **Step time**        | Mean per-step duration, plus total/average over a "training window" (all steps) and a "stable window" (after warm-up). | End-to-end training throughput, which folds in dataloader stalls. |
| **Checkpoint write** | Wall-time to persist the full state dict, aggregated across the run (min/avg/percentiles/p100). | Write throughput of large sequential objects to GCS. |
| **Checkpoint restore** | Wall-time to `torch.load` a checkpoint back on resume, including the initial restore. | Read throughput / latency of the restore path. |
| **Checkpoint delete** | Wall-time to prune old checkpoints when `checkpoints_to_keep` is exceeded. | Delete / object-lifecycle latency. |
| **Data loading**     | Accelerator-blocked time and percentage -- how long the trainer stalled waiting on the dataloader. | Whether GCS dataset reads keep up with the training loop. |
| **System / resource** | Per-pod peak/mean CPU cores, memory bytes, and network send/receive rates. | Host-side pressure the IO path generates. |
| **Read amplification** | Bytes actually read from GCS vs. logical checkpoint/dataset size (amplification ratio). | Read-efficiency of `gcsfs` -- redundant or over-fetched bytes. |

## Repository layout

```
workloads/hf-pytorch-lightning-cpu/helm_chart/
├── Chart.yaml
├── values_base.yaml            # Default knobs; overridden per run via `--set`.
├── llama_3_1_8b_cpu_sim.py     # The workload: dataset stream + checkpoint IO.
├── launcher.sh                 # Per-pod entrypoint: installs deps, runs torchrun.
├── requirements.txt            # Base Python deps (the run's `_REQUIREMENTS` install last).
└── templates/
    ├── workload-job.yaml                    # The JobSet definition.
    ├── workload-svc.yaml                    # Headless service for rendezvous.
    ├── workload-config-configmap.yaml       # Mounts the sim script.
    └── workload-launcher-configmap.yaml     # Mounts launcher.sh.
```

## Dataset and model requirements

The pipeline copies your inputs into per-run buckets, but you must stage them
first:

* **Dataset** (`_DATASET_PATH`): a GCS directory of `*.parquet` shards, each
  containing a `text` column. The workload loads them with
  `datasets.load_dataset("parquet", data_files="gs://.../*.parquet",
  streaming=True)` and tokenizes the `text` field with the Llama tokenizer.
* **Model** (`_MODEL_ID`): the gated Llama 3.1 8B weights, supplied either as a
  HuggingFace repo id (requires `_HF_TOKEN`) or -- to avoid the gated download on
  every rank -- a `gs://` directory holding a pre-staged copy of the weights and
  tokenizer files.

## Running it

Standing up the GKE cluster, running the workload, scraping the metrics, and
ingesting them into BigQuery is all driven by Cloud Build. See
[`cloudbuild/macrobenchmarks/README.md`](../../../../cloudbuild/macrobenchmarks/README.md)
for prerequisites, the full substitutions reference, trigger setup, and where
the metrics land.
