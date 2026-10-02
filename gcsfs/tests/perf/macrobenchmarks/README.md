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

| `_WORKLOAD` | Stack | Compute |
| :-- | :-- | :-- |
| `hf-datasets-pytorch-lightning` (default) | Hugging Face Datasets streaming → PyTorch Lightning, launched with `torchrun` | CPU only. See below. |
| `ray-data-ray-train-pytorch` | Ray Data → Ray Train `TorchTrainer` | CPU by default. GPU with `workload.gpu=true`. |

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

| Strategy | Lightning | Ray | Checkpoint |
| :-- | :-- | :-- | :-- |
| `ddp` | DDP | DDP | One file, written by rank 0 |
| `fsdp_full` | FSDP (`FSDPStrategy`) | FSDP2 | One file, written by rank 0 |
| `fsdp_sharded` | FSDP (`FSDPStrategy`) | FSDP2 | One shard per rank |
| `model_parallel_full` | `ModelParallelStrategy` | FSDP2 + tensor parallel | One file, written by rank 0 |
| `model_parallel_sharded` | `ModelParallelStrategy` | FSDP2 + tensor parallel | One shard per rank |

## Checkpoints

The following Cloud Build settings apply to both workloads:

- `_CHECKPOINT_INTERVAL` sets how often a checkpoint is saved.
- `_CKPT_TO_KEEP` sets how many checkpoints are kept. Older ones are deleted
  from GCS.
- `_SEED_CHECKPOINT=true` (the default) first runs a 1-step seed job that
  writes a checkpoint, and the measured run then restores from it.
- `_CHECKPOINT_LOAD_PATH`, if set, is restored instead of the seed checkpoint.

The two workloads save and restore differently:

| | Lightning | Ray |
| :-- | :-- | :-- |
| Save | Synchronous, through Lightning `ModelCheckpoint`. | Written to a local temp directory, then uploaded asynchronously by Ray Train. At most one upload is in flight at a time. |
| Restore | Full resume through `trainer.fit(ckpt_path=...)`. | Warm start: model, optimizer, and scheduler state are restored, but step and epoch restart at 0. |
| Compatibility | No explicit check. | These must match the new run: schema version, format, strategy, world size, TP/DP sizes, and SHA-256 hashes of the model `config.json` and tokenizer files. |

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
- **`_MODEL_ID`**: defaults to `gs://huggingface-model-weights/Llama-3.1-8B`.
  - A `gs://` model is copied to the pod with `gcloud storage cp`, not
    `gcsfs`.
  - A Hugging Face repo ID also works, together with `_HF_TOKEN`.
- **`_REQUIREMENTS`**: the `gcsfs` build under test. This is required. It is
  installed with `--no-cache-dir`.

## Node-local cache

Both charts mount the node directory `workload.hostCachePath` (default
`/var/lib/gcsfs-macrobench`) at `/workload/cache` in each pod. The measured
run on a node can then reuse what the seed run on that node downloaded. It
caches:

- pip wheels,
- the model, when the launcher stages it, and
- the gcloud SDK, when the image lacks `gcloud`.

Cloud Build deletes the cluster at the end of each build (unless
`_SKIP_CLEANUP=true`), and the cache is deleted with it.

To turn the cache off, set the Helm value `workload.hostCachePath=""`. Cloud
Build does not expose this setting.

## GPU

Cloud Build never sets `workload.gpu`, so Cloud Build runs are CPU only.

The Ray chart supports `workload.gpu=true` when you deploy it with Helm
directly onto a GPU node pool. It then requests `ranksPerNode` GPUs per pod and
adds a `nvidia.com/gpu` `NoSchedule` toleration.

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
└── ray-data-ray-train-pytorch/
    ├── helm_chart/
    │   ├── Chart.yaml
    │   ├── values_base.yaml
    │   ├── llama_3_1_8b_ray_train.py
    │   ├── metric_logging.py
    │   ├── launcher.sh
    │   ├── requirements.txt
    │   ├── requirements-cpu.txt
    │   └── templates/      # JobSet, headless Service, ConfigMaps
    └── tests/
```
