# GCSFS Macrobenchmarks

## Introduction

The GCSFS macrobenchmark is an end-to-end, training-shaped performance test.
It measures a specific `gcsfs` build under streaming Parquet reads and
periodic model-state checkpoint writes and restores. Every `gs://` data or
checkpoint transfer uses `gcsfs`/`fsspec`, not a PyArrow native-GCS client.
Ray Train checkpoints pass through Ray-managed local staging before upload or
after download; the staged payload is the complete native checkpoint, not a
smaller substitute for the measured GCS transfer.

Two selectable workloads share the existing Cloud Build, metrics, and BigQuery
contracts:

| Workload | Execution model | Default use |
| :-- | :-- | :-- |
| `hf-pytorch-lightning-cpu` | PyTorch-Lightning CPU simulation with frozen Llama weights and sleep-based compute. | Existing CPU storage-I/O benchmark. |
| `ray-data-ray-train-pytorch` | Ray Data packed input and Ray Train PyTorch DDP, FSDP2, or DP+TP. | GPU reference training or a CPU translation of that same data, distributed, checkpoint, and metric path. |

The Ray workload's GPU mode trains the real Llama model. Its CPU mode retains
the complete Llama model and AdamW state, Ray Data graph, Ray Train
orchestration, distributed wrappers, optimizer/scheduler lifecycle, gcsfs
checkpoint transfer, and metric/failure flow. A small token-derived probe and
`SIMULATED_STEP_COMPUTE_SECONDS` replace accelerator-only decoder compute. CPU
results therefore measure realistic I/O behavior, not Llama compute throughput.

> **This README describes the workload itself (what it is and what it
> measures).** To actually run the benchmark and collect metrics, use the Cloud
> Build automation documented in
> [`cloudbuild/macrobenchmarks/README.md`](../../../../cloudbuild/macrobenchmarks/README.md).
> The Ray-specific event and strict-failure contract is documented
> in [`workloads/ray-data-ray-train-pytorch/README.md`](workloads/ray-data-ray-train-pytorch/README.md).

## Workload architecture

A run is a Kubernetes JobSet on a GKE cluster, deployed by the selected Helm
chart. The existing Lightning workload follows its `torchrun` chain. The Ray
workload follows this chain from the outside in:

1. **Helm chart** (`workloads/ray-data-ray-train-pytorch/helm_chart/`) renders the
   Kubernetes objects and injects the run's configuration (steps, checkpoint
   interval, batch size, training strategy, gs:// paths, ...) from
   `values_base.yaml` plus `--set` overrides.
2. **JobSet** schedules one pod per node onto the dedicated node pool (pinned
   via `nodeSelector`). Pod 0 has stable headless-Service DNS and starts the Ray
   head; the remaining indexed pods start Ray workers.
3. **`launcher.sh`** runs inside each pod: it installs the requested Python
   packages (including the `gcsfs` build under test), pre-downloads the model if
   `MODEL_ID` is a `gs://` path, and starts Ray.
4. **Ray Train** uses fixed `train_slot` resources to place one training worker
   per intended rank, then supplies the train loop's worker environment and
   process groups.
5. **Ray Data** projects the Parquet `text` column through a gcsfs-backed
   `fsspec` filesystem, tokenizes and packs fixed-length examples, does bounded
   shuffling, and splits data by data-parallel replica. Tensor-parallel
   followers receive their leader's batch by collective broadcast.
6. **`llama_3_1_8b_ray_train.py`** applies DDP, FSDP2, or FSDP2 plus tensor
   parallelism; serializes or restores full and distributed checkpoints with
   native PyTorch APIs; and publishes or downloads them through Ray Train's
   gcsfs-backed storage filesystem.

## Training strategies

`TRAINING_STRATEGY` selects the parallel-training strategy, which changes the
checkpoint I/O shape. The Ray workload uses native PyTorch DDP and FSDP2; a
configured initial checkpoint must come from the same strategy --
cross-strategy restore is unsupported.

| Strategy       | Model sharding | Checkpoint IO exercised |
| :------------- | :------------- | :---------------------- |
| `ddp`          | Replicated on every rank | Single consolidated checkpoint written by rank 0. |
| `fsdp_sharded` | Sharded across ranks | Per-rank sharded/distributed checkpoint (every rank writes its shard). |
| `fsdp_full`    | Sharded across ranks | Consolidated to a single rank-0-written checkpoint at save time, like `ddp`. |
| `model_parallel_sharded` | Sharded across ranks (2D mesh) | Per-rank sharded/distributed checkpoint (every rank writes its shard). |
| `model_parallel_full` | Sharded across ranks (2D mesh) | Consolidated to a single rank-0-written checkpoint at save time. |

## Checkpoint I/O and warm starts

Every save contains model, optimizer, scheduler, precision-scaler, per-rank
random-number-generator, and compatibility state. It is serialized in
native PyTorch full or distributed-checkpoint format in Ray-managed local staging.
Ray Train then uploads that complete file or shard directory through the
PyArrow FSSpec adapter around `gcsfs.GCSFileSystem`. Uploads remain asynchronous
during training, and every worker waits for the final report to be committed
before the JobSet can succeed.

When `CKPT_TO_KEEP` is exceeded, Ray retention deletes the actual superseded
GCS object or sharded prefix after the replacement checkpoint is committed.
Deleting only a local staging directory does not satisfy the benchmark's
retention path.

An explicitly configured initial checkpoint is a warm start for a new
benchmark run. The complete training state is restored before the new data
iterator is created, but it does not restore the producer's data cursor,
epoch, or step counter. The consumer begins at epoch zero and run-local step
zero, and `MAX_STEPS` counts only new optimizer steps in the consumer run.
Automatic Ray trial recovery is disabled.

Model download is separate setup work and may continue to use the launcher's
`gcloud storage cp` staging step. Dataset reads, checkpoint uploads, checkpoint
downloads, and retention deletion remain the GCS I/O paths under test.

## What this benchmark measures

The run emits one flat summary row per execution. Metrics are grouped into the
families below. (MFU/TFLOPs are intentionally **excluded** -- this benchmark is
about storage IO, not compute efficiency.) The concrete BigQuery column names
for each family live in the automation README's schema section.

| Metric family        | What it captures | What it isolates about GCS |
| :------------------- | :--------------- | :------------------------- |
| **Step time**        | Mean per-step duration, plus total/average over a "training window" (all steps) and a "stable window" (after warm-up). | End-to-end training throughput, which folds in dataloader stalls. |
| **Checkpoint write** | Wall-time to persist the full state dict, aggregated across the run (min/avg/percentiles/p100). | Write throughput of large sequential objects to GCS. |
| **Checkpoint restore** | Wall-time to restore checkpoint state during the initial load. | Read throughput / latency of the restore path. |
| **Checkpoint delete** | Wall-time to prune old checkpoints when `checkpoints_to_keep` is exceeded. | Delete / object-lifecycle latency. |
| **Data loading**     | Accelerator-blocked time and percentage -- how long the trainer stalled waiting on the dataloader. | Whether GCS dataset reads keep up with the training loop. |
| **System / resource** | Per-pod peak/mean CPU cores, memory bytes, and network send/receive rates. | Host-side pressure the IO path generates. |
| **Read amplification** | Bytes actually read from GCS vs. logical checkpoint/dataset size (amplification ratio). | Read-efficiency of `gcsfs` -- redundant or over-fetched bytes. |

## Ray workload layout

```
workloads/ray-data-ray-train-pytorch/helm_chart/
├── Chart.yaml
├── values_base.yaml            # Default knobs; overridden per run via `--set`.
├── llama_3_1_8b_ray_train.py   # Ray Data, Ray Train, parallelism, and checkpoints.
├── launcher.sh                 # Per-pod entrypoint: installs deps and starts Ray.
├── requirements.txt            # Ray and userspace deps; deliberately torch-free.
├── requirements-cpu.txt        # CPU-only PyTorch source and exact pin.
└── templates/
    ├── workload-job.yaml                    # Indexed JobSet and optional GPUs.
    ├── workload-svc.yaml                    # Headless service for the Ray head.
    ├── workload-config-configmap.yaml       # Mounts Python and both requirements files.
    └── workload-launcher-configmap.yaml     # Mounts launcher.sh.
```

## Dataset and model requirements

The pipeline copies your inputs into per-run buckets, but you must stage them
first:

* **Dataset** (`_DATASET_PATH`): a GCS directory of `*.parquet` shards, each
  containing a `text` column. The Ray workload resolves the URI through
  `fsspec.core.url_to_fs`, verifies `gcsfs.GCSFileSystem`, and passes its
  PyArrow fsspec adapter to `ray.data.read_parquet` before tokenizing and
  packing the `text` field.
* **Model** (`_MODEL_ID`): the gated Llama 3.1 8B weights, supplied either as a
  HuggingFace repo id (requires `_HF_TOKEN`) or -- to avoid the gated download on
  every rank -- a `gs://` directory holding a pre-staged copy of the weights and
  tokenizer files.

## Running it

Standing up the GKE cluster, running the workload, scraping the metrics, and
ingesting them into BigQuery is all driven by Cloud Build. See
[`cloudbuild/macrobenchmarks/README.md`](../../../../cloudbuild/macrobenchmarks/README.md)
for prerequisites, the full substitutions reference, trigger setup, and where
the metrics land. Select the Ray workload through the retained trigger setting:

```text
_WORKLOAD=ray-data-ray-train-pytorch
```

The Cloud Build run pipeline remains CPU-default. GPU runs are direct Helm
deployments to an already provisioned GPU-capable node pool; they do not add a
Cloud Build accelerator substitution. For example:

```bash
helm install ray-train-gpu \
  gcsfs/tests/perf/macrobenchmarks/workloads/ray-data-ray-train-pytorch/helm_chart \
  -f gcsfs/tests/perf/macrobenchmarks/workloads/ray-data-ray-train-pytorch/helm_chart/values_base.yaml \
  --set workload.gpu=true \
  --set 'nodeSelector.cloud\.google\.com/gke-nodepool=<gpu-node-pool>'
```

`workload.gpu=true` requests one `nvidia.com/gpu` per `ranksPerNode` and adds
the NVIDIA `NoSchedule` toleration. Leave it unset for the CPU translation.
