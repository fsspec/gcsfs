# GCSFS Macrobenchmarks Automation

## Introduction

This directory contains the Cloud Build automation that runs the GCSFS
macrobenchmark end to end: it provisions an **ephemeral** GKE cluster, runs the
Llama 3.1 8B PyTorch-Lightning CPU simulation as a Kubernetes JobSet, scrapes the
resulting metrics from Cloud Logging and Cloud Monitoring into a single summary
CSV, uploads it to a results bucket, and (via a second pipeline) ingests it into
BigQuery for historical analysis.

The workload being run -- what it simulates and what each metric means -- is
documented separately in
[`gcsfs/tests/perf/macrobenchmarks/README.md`](../../gcsfs/tests/perf/macrobenchmarks/README.md).
**This README is the operational guide: how to stand the benchmark up in your own
GCP project and get the metrics.**

There are two pipelines:

* **Run pipeline** (`macrobenchmarks-cloudbuild.yaml`) -- provisions infra, runs
  the workload, scrapes metrics, tears infra down.
* **Ingestion pipeline** (`macrobenchmarks-ingestion-cloudbuild.yaml`) -- loads
  the summary CSVs from the results bucket into BigQuery.

## Cost and runtime

> **A run bills real compute and storage in your own project.** With the
> defaults it provisions **2 × `c4-standard-192`** (192 vCPU each) plus a GKE
> control plane and a small system node pool, copies your entire dataset into a
> fresh per-run bucket, and can run for **up to 6 hours** (the pipeline timeout).
>
> The infrastructure is ephemeral and torn down at the end of every run (and a
> best-effort `cleanup-leaked-resources` step reaps orphans from prior failed
> builds), but a hard crash can still leave billable resources behind -- check
> your project after failures.
>
> **The footprint is tunable.** `c4-standard-192` is only the default. You can
> reduce cost with a smaller `_MACHINE_TYPE`, fewer `_NODES`, fewer
> `_RANKS_PER_NODE`, fewer `_DATALOADER_WORKERS`, or a smaller model -- all of
> which lower the CPU/memory the run needs. (Note the 4-ranks/node cap exists so
> a checkpoint-restoring run fits host RAM; see the substitutions notes below.)

## Prerequisites

Before creating the triggers, set up the following in your GCP project.

1. **APIs**: Cloud Build, GKE (`container`), Compute Engine, Cloud Storage, Cloud
   Logging, Cloud Monitoring, and BigQuery.

2. **A private Cloud Build worker pool** named **exactly**
   `cloud-build-worker-pool`, in the same `LOCATION` you run the builds from.
   Both pipeline YAMLs hardcode
   `options.pool.name = projects/${PROJECT_ID}/locations/${LOCATION}/workerPools/cloud-build-worker-pool`.

3. **A GKE node service account** (passed as `_GKE_SERVICE_ACCOUNT`). The nodes
   run with `cloud-platform` scope, but scope is not IAM -- grant it at minimum:
   * `roles/storage.objectAdmin` -- read the dataset, write/read/delete checkpoints.
   * `roles/logging.logWriter` -- pods emit the metric logs the scraper reads.
   * `roles/monitoring.metricWriter` -- system metrics.

4. **The Cloud Build service account** that executes the pipelines. Grant it at
   minimum:
   * `roles/container.admin` -- create the cluster and node pools.
   * `roles/compute.networkAdmin` -- create the dedicated VPC and subnet.
   * `roles/storage.admin` -- create/describe/delete the run buckets and upload results.
   * `roles/logging.viewer` and `roles/monitoring.viewer` -- scrape metrics.
   * `roles/iam.serviceAccountUser` on the GKE node service account -- so the
     build can attach it to the cluster.
   * `roles/bigquery.admin` (ingestion pipeline) -- create the dataset/tables and
     run the load query. `roles/bigquery.dataEditor` + `roles/bigquery.jobUser`
     is a tighter alternative.

   > These are starting points -- tighten to least privilege for your
   > environment.

5. **A pre-existing dataset bucket** (`_DATASET_PATH`) holding `*.parquet` shards
   with a `text` column. It must already match the run's **region** and
   **bucket type** -- `init-variables` hard-fails a mismatch before provisioning
   anything. (Regional/HNS buckets in `region`; zonal/RAPID buckets placed in
   `_ZONE`.)

6. **Gated Llama 3.1 8B access**: either set `_HF_TOKEN` and let `_MODEL_ID`
   point at the HuggingFace repo, or pre-stage the weights + tokenizer to a
   `gs://` path and set `_MODEL_ID` to it (the default).

7. **The `gcsfs` build under test**: supplied via `_REQUIREMENTS` (a
   pip-installable spec/URL), installed last on every pod so it overrides the
   chart's pinned versions. This is how you point the benchmark at the exact
   `gcsfs` you want to measure.

## Run pipeline flow

`macrobenchmarks-cloudbuild.yaml` runs these steps (each step's logic lives in
`scripts/<step>.sh`):

| Step | What it does |
| :--- | :----------- |
| `init-variables` | Validates all substitutions and the operator-supplied buckets, derives the region and run identifiers. **Fails fast before anything is provisioned or billed.** |
| `cleanup-leaked-resources` | Best-effort reap of orphaned infra from prior failed builds with the same `_INFRA_PREFIX`. |
| `create-buckets` | Creates the per-run checkpoint bucket and per-run dataset bucket (copying `_DATASET_PATH` in), and ensures the shared results bucket exists. |
| `create-cluster` | Creates the dedicated VPC/subnet, the GKE cluster + a `_MACHINE_TYPE` node pool (with TIER_1 networking when enabled), and installs the JobSet controller. |
| `seed-checkpoint` | When `_SEED_CHECKPOINT=true` and no external checkpoint is given, generates a per-run checkpoint so the measured run exercises the **restore** path. |
| `run-workload` | `helm install`s the chart and polls the JobSet to completion, recording start/end timestamps for the scrape. |
| `scrape-metrics` | Parses Cloud Logging into raw CSVs, aggregates them into one summary row (retrying for ingestion lag), folds in system metrics, and uploads the summary to the results bucket. |
| `cleanup-helm` / `cleanup-cluster` / `delete-buckets` | Tear down the workload, cluster/network, and per-run buckets (skipped when `_SKIP_CLEANUP=true`). |
| `check-failure` | Fails the build if any earlier step recorded a failure (steps run with `allowFailure` so cleanup always happens). |

## Substitutions reference

### Required (no default)

| Substitution | Description |
| :----------- | :---------- |
| `_INFRA_PREFIX` | Prefix for all created resources (cluster, network, buckets, BigQuery dataset). Also selects the shared results bucket `gs://<prefix>-macrobench-results`. |
| `_ZONE` | GCP zone for the cluster and zonal buckets (e.g. `us-central1-a`). The region is derived from it. |
| `_GKE_SERVICE_ACCOUNT` | Email of the node service account (see prerequisites). |
| `_DATASET_PATH` | `gs://` directory of `*.parquet` shards (with a `text` column) to train on. Must match the run's region and bucket type. |
| `_REQUIREMENTS` | Pip spec/URL of the `gcsfs` build under test (installed last, overriding pins). |
| `_HF_TOKEN` | HuggingFace token. Required only when `_MODEL_ID` is a gated HF repo (not a `gs://` path). |

### Tuning / optional (with defaults)

| Substitution | Default | Description |
| :----------- | :------ | :---------- |
| `_WORKLOAD` | `hf-pytorch-lightning-cpu` | Workload directory under `gcsfs/tests/perf/macrobenchmarks/workloads/`. |
| `_BUCKET_TYPE` | `regional` | `regional`, `zonal`, or `hns`. Must match the dataset bucket. |
| `_MACHINE_TYPE` | `c4-standard-192` | Node machine type for the workload pool. |
| `_ENABLE_TIER1_NETWORKING` | `true` | Enable TIER_1 high-bandwidth egress (requires gVNIC / C-series). |
| `_NODES` | `2` | Number of workload nodes. |
| `_RANKS_PER_NODE` | `4` | Processes per node (stand-ins for GPU chips). |
| `_STEPS` | `100` | Training steps. |
| `_CHECKPOINT_INTERVAL` | `25` | Steps between checkpoint writes. |
| `_CKPT_TO_KEEP` | `1` | Checkpoints retained (older ones are deleted). |
| `_MODEL_ID` | `gs://huggingface-model-weights/Llama-3.1-8B` | HF repo id or `gs://` pre-staged weights. |
| `_CHECKPOINT_LOAD_PATH` | `""` | External `gs://` checkpoint to resume from (exercises restore). |
| `_SEED_CHECKPOINT` | `true` | Auto-generate a per-run seed checkpoint and restore from it. |
| `_TRAINING_STRATEGY` | `ddp` | `ddp`, `fsdp_sharded`, or `fsdp_full`. |
| `_SIMULATED_STEP_COMPUTE_SECONDS` | `1.0` | Per-step sleep standing in for GPU compute. |
| `_PER_DEVICE_BATCH` | `8` | Per-rank micro-batch size. |
| `_GRAD_ACCUM` | `1` | Gradient-accumulation steps. |
| `_DATALOADER_WORKERS` | `16` | Dataloader worker processes per rank. |
| `_IMAGE` | `nvcr.io/nvidia/pytorch:26.05-py3` | Container image for the pods. |
| `_JOBSET_VERSION` | `v0.12.0` | JobSet controller release to install. |
| `_SKIP_CLEANUP` | `false` | Leave infra standing after the run (for debugging). |

### Interaction notes

* **Restore precedence**: `_CHECKPOINT_LOAD_PATH` (if set) wins and the
  `seed-checkpoint` step no-ops. Otherwise, with `_SEED_CHECKPOINT=true`, the run
  restores from the auto-generated per-run seed. With both empty/false it is a
  fresh run with no restore measured.
* **Ranks/node cap**: keep `_RANKS_PER_NODE` at 4 on `c4-standard-192`. A
  restoring run's per-rank peak RSS roughly doubles (resident model+optimizer
  plus the `torch.load`'d checkpoint, ~92GB); 8 ranks × 92GB OOMs the 720GB host.
* **Region/type coupling**: `_BUCKET_TYPE`, `_ZONE`, and `_DATASET_PATH` must
  agree, and the results bucket must be co-located with the build `LOCATION` (the
  BigQuery dataset is created there and cannot read a cross-region bucket).

## Setting up the triggers

Create two Cloud Build triggers (adjust the substitution values for your
project):

**Run trigger:**
```bash
gcloud builds triggers create manual \
  --name=gcsfs-macrobench-run \
  --region=<LOCATION> \
  --repo=<YOUR_REPO> --repo-type=<GITHUB|CLOUD_SOURCE_REPOSITORIES> \
  --branch=main \
  --build-config=cloudbuild/macrobenchmarks/macrobenchmarks-cloudbuild.yaml \
  --substitutions=_INFRA_PREFIX=gcsfs-macrobench,_ZONE=us-central1-a,_GKE_SERVICE_ACCOUNT=<SA_EMAIL>,_DATASET_PATH=gs://<YOUR_DATASET_BUCKET>/parquet,_REQUIREMENTS=gcsfs,_HF_TOKEN=<TOKEN_IF_NEEDED>
```

**Ingestion trigger** (run after the run pipeline, or on a schedule):
```bash
gcloud builds triggers create manual \
  --name=gcsfs-macrobench-ingest \
  --region=<LOCATION> \
  --repo=<YOUR_REPO> --repo-type=<GITHUB|CLOUD_SOURCE_REPOSITORIES> \
  --branch=main \
  --build-config=cloudbuild/macrobenchmarks/macrobenchmarks-ingestion-cloudbuild.yaml \
  --substitutions=_INFRA_PREFIX=gcsfs-macrobench,_DATASET_NAME=macrobenchmarks
```

`_INFRA_PREFIX` **must match** between the two triggers -- the ingestion pipeline
reads `gs://<_INFRA_PREFIX>-macrobench-results` and writes to a dataset named
`<_INFRA_PREFIX>_<_DATASET_NAME>` (hyphens collapsed to underscores).

## Metrics: summary schema, results, and ingestion

### Where results land

Each run uploads one summary CSV to:

```
gs://<_INFRA_PREFIX>-macrobench-results/branch=<branch>/<YYYYMMDD>/<run_id>/<timestamp>.csv
```

### Summary schema

The summary row is one wide record. Its columns -- and their BigQuery types --
are defined once in
[`macrobenchmarks_schema.json`](macrobenchmarks_schema.json) (the single source
of truth; the calculator derives its CSV header from it). The columns, by family:

* **Run configuration**: `run_id`, `workload_name`, `requirements`, `image`,
  `bucket_type`, `zone`, `region`, `machine_type`, `nodes`, `ranks_per_node`,
  `steps`, `checkpoint_interval`, `checkpoints_to_keep`, `dataset_path`,
  `model_id`, `training_strategy`, `simulated_step_compute_seconds`,
  `per_device_train_batch_size`, `gradient_accumulation_steps`,
  `global_batch_size`, `dataloader_num_workers`.
* **Step time**: `mean_step_time`, `stable_window_avg_step_time`,
  `stable_window_total_step_duration`, `training_window_avg_step_time`,
  `training_window_total_step_duration`.
* **Checkpoint write / restore / delete**: `checkpoint_<op>_time_{min,max,avg,stddev,p50,p90,p99,p100}`,
  `num_checkpoint_<op>_datapoints`, plus `checkpoint_restore_time_initial`.
* **Data loading**: `accelerator_blocked_time`, `accelerator_blocked_percent`.
* **System / resource**: `cpu_usage_{peak,mean}_cores`,
  `memory_usage_{peak,mean}_bytes`, `memory_limit_utilization_peak`,
  `cpu_limit_utilization_peak`,
  `network_{received,sent}_{peak,mean}_bytes_per_sec`.
* **Read amplification**: `checkpoint_read_bytes`, `checkpoint_read_request_count`,
  `checkpoint_restored_bytes`, `checkpoint_read_amplification_ratio`,
  `dataset_read_bytes`, `dataset_read_request_count`, `dataset_size_bytes`,
  `dataset_sample_count`, `dataset_read_amplification_ratio`.

The per-metric aggregation logic lives in `metrics/` (`calculate.py`,
`stats.py`, `parsers/hf.py`, `monitoring.py`).

### Ingestion into BigQuery

`macrobenchmarks-ingestion-cloudbuild.yaml`:

1. Ensures the dataset `<_INFRA_PREFIX>_<_DATASET_NAME>` exists (created in
   `LOCATION`).
2. Recreates an external **`staging`** table pointing at the results-bucket CSVs,
   using `macrobenchmarks_schema.json`.
3. Runs `ingest.sql`, which maintains a **`history`** table partitioned by
   `run_date`: it auto-evolves the schema when new columns appear, extracts
   metadata (`run_date`, `build_id`, `run_timestamp`) from the source path, and
   inserts new rows -- deduplicating on `source_uri` so re-runs don't double-count.

After ingestion, the metrics are available as rows in the **`history`** table of
the `<_INFRA_PREFIX>_<_DATASET_NAME>` BigQuery dataset.
