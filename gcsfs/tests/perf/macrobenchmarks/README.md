# GCSFS Macrobenchmarks

A macrobenchmark is an end-to-end, training-shaped run on GKE that measures
one `gcsfs` build under realistic storage I/O:

- streaming Parquet dataset reads,
- periodic checkpoint writes,
- checkpoint restore, and
- deletion of old checkpoints.

Dataset reads and checkpoint transfers to and from `gs://` go through
`gcsfs`/`fsspec`.

To run it, use Cloud Build; see
[`cloudbuild/macrobenchmarks/README.md`](../../../../cloudbuild/macrobenchmarks/README.md).

## Workloads

Choose a workload with the Cloud Build substitution `_WORKLOAD`.

| Workload | Stack | Compute |
| :-- | :-- | :-- |
| `hf-datasets-pytorch-lightning` (Cloud Build `_WORKLOAD` default) | Hugging Face Datasets streaming → PyTorch Lightning, launched with `torchrun` | CPU only. See below. |
| `ray-data-ray-train-pytorch` (Cloud Build `_WORKLOAD`) | Ray Data → Ray Train `TorchTrainer` | CPU by default. GPU with `workload.gpu=true`. |
| `torchtitan` (Helm deployment on GPU cluster) | Upstream TorchTitan `main` `Trainer` (up to 5D parallelism: `PP` + `HSDP` + `CP` + `TP`) with a Grain dataloader streaming Parquet over `gcsfs` and TorchTitan's DCP checkpointer writing to `gs://` | Multi-node GPU training & checkpointing benchmark for **Llama 3.1 70B**. |

**Lightning workload**

- The chart requests no GPUs, so training runs on CPU. The script uses a GPU
  only when `torch.cuda.is_available()` is true.
- Llama-3.1-8B is loaded and frozen. A small `Linear(8, 8)` layer provides
  the loss.
- With `ddp` (the default), the optimizer holds only the frozen model, so no
  weights change.
- Each step sleeps for `SIMULATED_STEP_COMPUTE_SECONDS` in place of real
  compute.
- The legacy name `hf-pytorch-lightning-cpu` is still accepted for
  `_WORKLOAD`, and it is still the `workload_name` recorded in BigQuery.

**Ray workload**

- CPU mode (the default): Llama-3.1-8B is loaded and frozen. A small probe
  model provides the loss. Each step sleeps for
  `SIMULATED_STEP_COMPUTE_SECONDS`.
- GPU mode (`workload.gpu=true`): real Llama training.

**TorchTitan workload**

- Deployed directly via Helm onto a GPU cluster (e.g., `a3-ultragpu-8g` / H200);
  rejected by the CPU-only Cloud Build `init_variables.sh` pipeline.
- Always runs the **latest TorchTitan `main`** (not pinned). `launcher.sh`
  replaces the image's `torch`/`torchao` with PyTorch nightlies (`main` tracks
  nightly) and logs the exact versions and TorchTitan commit in a
  `Workload Versions : ...` line.
- Runs upstream `python -m torchtitan.train` with the recipe
  `torchtitan_train.llama3_70b_gcsfs`, which starts from upstream
  `llama3_70b()` and only swaps in the `gs://` dataset, the `gs://` checkpoint
  folder, the parallelism layout, and the run length. Thin `Trainer` /
  `TrainingEngine` / `GrainDataLoader` subclasses print the metric lines
  parsed by `metrics/parsers/hf.py`.
- Data: Grain `HuggingFaceStreamingSource` (`datasets` streaming via `gcsfs`) →
  tokenize → upstream concatenate-then-split packing into `SEQ_LEN` rows.
  Each rank reads through one Grain iterator thread (no `DataLoader` worker
  processes).
- Uses the `flex` attention backend; upstream's default `varlen` needs
  FlashAttention 3 in the image.
- Trains **Llama 3.1 70B** with FP32 master weights and FP32 AdamW states and
  BF16 compute (FSDP2 mixed precision).

CPU results measure I/O behavior, not model compute speed.

## How a run executes

Cloud Build runs these steps:

1. Validate the substitutions.
2. In parallel:
   - clean up resources leaked by earlier builds,
   - create the per-run buckets and copy the dataset into one of them, and
   - create a GKE cluster.
3. Optionally make a seed checkpoint (see below).
4. Install the workload's Helm chart.
5. Scrape the metrics.
6. Clean up. Fail the build if an earlier step recorded a failure.

Inside the cluster:

1. **Helm chart**: renders an indexed JobSet from `values_base.yaml` plus
   `--set` overrides.
2. **`launcher.sh`** (in every pod):
   - installs the requirements, including the `gcsfs` build under test;
   - stages the model (the order and the supported model sources differ per
     chart);
   - then starts training:
     - Lightning: `torchrun` starts `ranksPerNode` processes per pod.
     - Ray: pod 0 starts the Ray head and the driver, and the other pods join
       as Ray workers. Ray Train starts `nodes × ranksPerNode` training
       workers.

The defaults are `nodes: 2` and `ranksPerNode: 4`, which gives 8 ranks.

## Training strategies

`_TRAINING_STRATEGY` selects the parallelism, which sets the checkpoint shape.

| Strategy | Lightning | Ray | TorchTitan | Checkpoint |
| :-- | :-- | :-- | :-- | :-- |
| `ddp` | DDP | DDP | Unsupported (70B requires sharding) | One file, written by rank 0 |
| `fsdp_full` | FSDP (`FSDPStrategy`) | FSDP2 | Unsupported (70B uses sharded DCP) | One file, written by rank 0 |
| `fsdp_sharded` | FSDP (`FSDPStrategy`) | FSDP2 | 1D FSDP2 | One shard per rank |
| `model_parallel_full` | `ModelParallelStrategy` | FSDP2 + tensor parallel | Unsupported (70B uses sharded DCP) | One file, written by rank 0 |
| `model_parallel_sharded` | `ModelParallelStrategy` | FSDP2 + tensor parallel | Up to 5D Parallelism (`PP`, `dp_replicate`, `dp_shard`, `CP`, `TP`) | One shard per rank |

## Checkpoints

The following Cloud Build settings apply across the workloads:

- `_CHECKPOINT_INTERVAL` sets how often a checkpoint is saved.
- `_CKPT_TO_KEEP` sets how many checkpoints are kept. Older ones are deleted
  from GCS.
- `_SEED_CHECKPOINT=true` (the default) first runs a 1-step seed job that
  writes a checkpoint, and the measured run then restores from it.
- `_CHECKPOINT_LOAD_PATH`, if set, is restored instead of the seed checkpoint.

The workloads save and restore as follows:

| | Lightning | Ray | TorchTitan |
| :-- | :-- | :-- | :-- |
| Save | Synchronous, through Lightning `ModelCheckpoint`. | Written to a local temp directory, then uploaded asynchronously by Ray Train. At most one upload is in flight at a time. | TorchTitan's `CheckpointManager` (synchronous sharded DCP) to `gs://<ckptWritePath>/<release>/step-N` via `fsspec`/`gcsfs`; the final save is a full checkpoint (`last_save_model_only=False`). Retention needs `ckptToKeep` ≥ 2 (or 0 to keep all); old checkpoints are deleted by TorchTitan's background purge thread. |
| Restore | Full resume through `trainer.fit(ckpt_path=...)`. | Warm start: model, optimizer, and scheduler state are restored, but step and epoch restart at 0. | Full resume from `ckptLoadPath` (`initial_load_path`, `initial_load_model_only=False`): model, optimizer, LR scheduler, Grain dataloader, and train state; training continues at the restored step + 1. |
| Compatibility | No explicit check. | These must match the new run: schema version, format, strategy, world size, TP/DP sizes, and SHA-256 hashes of the model `config.json` and tokenizer files. | DCP reshards model and optimizer state across topologies. Upstream `GrainDataLoader` refuses to resume after a DP-degree change, so when the checkpoint's dataloader state was saved at a different DP degree the workload excludes it from loading (with a warning) and the data stream restarts. |

## Metrics

Each run produces one summary row. For column names, see
[`macrobenchmarks_schema.json`](../../../../cloudbuild/macrobenchmarks/macrobenchmarks_schema.json).

| Family | Covers |
| :-- | :-- |
| Step time | Per-step duration, over all steps and over a stable window that skips the first 10 steps. |
| Checkpoint write / restore / delete | Duration statistics for each, checkpoint size, and write and restore throughput. |
| Data loading | Time the trainer waited for input batches. |
| System | The highest per-pod peak and mean CPU, memory, and network, plus peak CPU and memory as a fraction of node allocatable capacity. |
| Read amplification | GCS bytes served ÷ ideal bytes (see below). |

- **Ray checkpoint write time** covers only the upload to GCS, taking the
  slowest rank. It does not include serializing to local staging.
- **Dataset read amplification** = GCS bytes read from the dataset bucket ÷
  (steps × global batch size × dataset bytes per sample).
- **Checkpoint read amplification** = GCS bytes read from the per-run
  checkpoint bucket ÷ size of the restored checkpoint. Reads from an external
  `_CHECKPOINT_LOAD_PATH` bucket are not counted.
- These columns are best effort and can be empty: system,
  read-amplification, and restore-throughput.
- MFU/TFLOPs are not reported.

## Inputs

- **`_DATASET_PATH`**: a `gs://` directory with `*.parquet` files, each with a
  `text` column.
  - Only top-level `*.parquet` files are read.
  - The whole directory is copied into a per-run bucket.
  - Dataset read amplification estimates bytes per sample from the largest
    object in that bucket, so keep only the Parquet files there.
  - For `torchtitan`, TorchTitan's streaming source calls
    `split_dataset_by_node`, which shards by Parquet file when the file count
    is divisible by the DP degree and otherwise falls back to example-level
    interleaving (every rank reads every file). Use a file count divisible by
    the DP degree.
- **`_MODEL_ID`**: defaults to `gs://huggingface-model-weights/Llama-3.1-8B`.
  - A `gs://` model is copied to the pod with `gcloud storage cp`, not
    `gcsfs` (or staged once per node for tokenizer metadata in `torchtitan`).
  - A Hugging Face repo ID also works, together with `_HF_TOKEN`.
- **`_REQUIREMENTS`**: the `gcsfs` build under test. This is required. It is
  installed with `--no-cache-dir`.

## Node-local cache

All three charts mount the node directory `workload.hostCachePath` (default
`/var/lib/gcsfs-macrobench`) at `/workload/cache` in each pod. The measured
run on a node can then reuse what the seed run on that node downloaded. It
caches:

- pip wheels,
- the model / tokenizer metadata, when the launcher stages it, and
- the gcloud SDK, when the image lacks `gcloud`.

Cloud Build deletes the cluster at the end of each build (unless
`_SKIP_CLEANUP=true`), and the cache is deleted with it.

To turn the cache off, set the Helm value `workload.hostCachePath=""`. Cloud
Build does not expose this setting.

## GPU

Cloud Build's default `create_cluster.sh` provisions CPU nodes (`c4-standard-192`)
and never sets `workload.gpu`, so default Cloud Build runs are CPU only.

- **Ray (`ray-data-ray-train-pytorch`)**: supports `workload.gpu=true` when deployed
  with Helm onto a GPU node pool. It then requests `ranksPerNode` GPUs per pod and
  adds a `nvidia.com/gpu` `NoSchedule` toleration.
- **TorchTitan (`torchtitan`)**: is a GPU-native workload for **Llama 3.1 70B**
  that requests `ranksPerNode` GPUs per pod (default 8 GPUs/node across 2 nodes = 16 GPUs)
  and configures GPUDirect-RDMA (`nccl-plugin-gib`) when deployed on A3 Ultra / H200 clusters.
  The image (`nvcr.io/nvidia/pytorch:26.08-py3` by default) only provides CUDA and the
  system libraries; `torch`/`torchao` are replaced with nightlies from
  `workload.torchNightlyIndex` because the image's build lacks APIs TorchTitan `main` imports.

## Layout

```text
workloads/
├── hf-datasets-pytorch-lightning/helm_chart/
│   ├── Chart.yaml
│   ├── values_base.yaml
│   ├── train_pytorch_lightning.py
│   ├── launcher.sh
│   ├── requirements.txt
│   └── templates/          # JobSet, headless Service, ConfigMaps
├── ray-data-ray-train-pytorch/
│   ├── helm_chart/
│   │   ├── Chart.yaml
│   │   ├── values_base.yaml
│   │   ├── llama_3_1_8b_ray_train.py
│   │   ├── metric_logging.py
│   │   ├── launcher.sh
│   │   ├── requirements.txt
│   │   ├── requirements-cpu.txt
│   │   └── templates/      # JobSet, headless Service, ConfigMaps
│   └── tests/
└── torchtitan/
    ├── helm_chart/
    │   ├── Chart.yaml
    │   ├── values_base.yaml
    │   ├── torchtitan_train.py   # recipe + instrumented Trainer subclasses
    │   ├── launcher.sh
    │   ├── requirements.txt
    │   └── templates/      # JobSet, headless Service, ConfigMaps
    └── tests/
```

### Deploying the TorchTitan GPU workload (`torchtitan`)

To deploy the TorchTitan Llama 3.1 70B GPU workload (e.g., on `a3-ultragpu-8g` / H200 GKE nodes, supporting up to 5D parallelism where `WORLD_SIZE = pipelineParallelSize * dpReplicateSize * dataParallelSize * contextParallelSize * tensorParallelSize`):

```bash
helm install torchtitan-llama70b \
  gcsfs/tests/perf/macrobenchmarks/workloads/torchtitan/helm_chart \
  -f gcsfs/tests/perf/macrobenchmarks/workloads/torchtitan/helm_chart/values_base.yaml \
  --set workload.modelFlavor=70B \
  --set workload.nodes=2 \
  --set workload.ranksPerNode=8 \
  --set workload.tensorParallelSize=4 \
  --set workload.dataParallelSize=4 \
  --set workload.dpReplicateSize=1 \
  --set workload.contextParallelSize=1 \
  --set workload.pipelineParallelSize=1
```
