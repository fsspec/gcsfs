# GCSFS Subsystem Benchmarks Automation

## Introduction

This directory contains the Cloud Build automation for running GCSFS subsystem
benchmarks on an ephemeral Compute Engine VM and retaining their results for
historical analysis. It installs the selected benchmark group, runs every
selected case, uploads JSON and CSV artifacts to Cloud Storage, and uses a
separate pipeline to ingest CSV rows into BigQuery.

The workload itself -- what it exercises, its timing boundaries, and how to run
it directly for debugging -- is documented in the
[subsystem benchmark workload guide](../../gcsfs/tests/perf/subsystembenchmarks/README.md).
This README is the operational guide for provisioning and running it in a GCP
project.

There are two pipelines:

- **Run pipeline** (`subsystembenchmarks-cloudbuild.yaml`): provisions the VM,
  runs the selected group, uploads results, and cleans up ephemeral resources.
- **Ingestion pipeline** (`subsystembenchmarks-ingestion-cloudbuild.yaml`):
  exposes uploaded CSVs through a staging table and appends unseen results to a
  partitioned BigQuery history table.

## Cost and cleanup

> **A run creates billable resources in your project.** The default machine is
> one `c4-standard-192` VM with a 50 GB Hyperdisk Balanced boot disk,
> TIER_1 networking, and gVNIC. The pipeline may run for up to four hours. Each
> benchmark case also creates a temporary GCS bucket, uploads a synthetic
> corpus, and reads that corpus for every configured round.
>
> The VM, temporary OS Login key, and case buckets are cleaned up after normal
> success or failure. A best-effort preflight step deletes matching VMs older
> than ten hours, and the final bucket sweep removes case buckets left by a
> failed benchmark. A cancelled build or hard platform failure can still skip
> cleanup, so inspect Compute Engine and Cloud Storage after abnormal runs.

The results bucket, `gs://<_INFRA_PREFIX>-run-results`, is intentionally
persistent. Its objects continue to incur storage charges until removed.

## Prerequisites

Set up the following before creating the triggers.

1. **APIs:** Cloud Build, Compute Engine, Cloud Storage, Cloud Monitoring,
   BigQuery, IAM, and OS Login.

2. **Private worker pool:** both YAML files use a pool named exactly
   `cloud-build-worker-pool` in the build's `LOCATION`:

   ```text
   projects/<PROJECT_ID>/locations/<LOCATION>/workerPools/cloud-build-worker-pool
   ```

3. **Network reachability:** the run pipeline connects to the benchmark VM with
   `gcloud compute ssh --internal-ip`. The worker pool network must be able to
   reach the VM's VPC interface on TCP port 22. The YAML does not select a
   network, so Compute Engine uses the project's default network. The pipeline
   applies the `allow-ssh` network tag but does not create a firewall rule.

4. **A VM service account:** pass its email as `_VM_SERVICE_ACCOUNT`. The VM
   uses it to create, populate, and delete per-case buckets; upload artifacts to
   the persistent results bucket; and query Cloud Monitoring for read metrics.

5. **A trigger source that supplies branch metadata:** `BRANCH_NAME` must be set
   and must not resolve to `unknown`. The uploaded artifact path uses its
   sanitized value.

### Service accounts and IAM

The Cloud Build service account and the benchmark VM service account have
different responsibilities. The following predefined roles are practical
starting points, not a least-privilege prescription; tighten them for your
project.

**Cloud Build service account:**

- `roles/compute.instanceAdmin.v1` to create, inspect, and delete the benchmark
  VM.
- `roles/compute.osAdminLogin` to add the temporary OS Login key, connect to the
  VM, and run the setup script with `sudo`.
- `roles/storage.admin` to create the persistent results bucket and support the
  cleanup pipeline.
- `roles/iam.serviceAccountUser` on the VM service account so the build can
  attach it to the VM.
- `roles/bigquery.admin` for the ingestion pipeline. A narrower combination of
  BigQuery dataset/table write permissions and `roles/bigquery.jobUser` can be
  used instead.
- Read access to objects in the results bucket when the ingestion query reads
  the external table. `roles/storage.admin` already includes this permission;
  separate ingestion identities can use `roles/storage.objectViewer`.

Depending on the shared VPC and worker-pool setup, the build identity may also
need `roles/compute.networkUser` on the selected network or subnetwork.

**Benchmark VM service account:**

- `roles/storage.admin` to create and delete case buckets, read/write their
  objects, and upload run artifacts.
- `roles/monitoring.viewer` to query the bucket-level GCS metrics used for read
  amplification.

## Run pipeline flow

`subsystembenchmarks-cloudbuild.yaml` performs these steps:

| Step | What it does |
| :--- | :--- |
| `generate-ssh-key` | Creates a build-scoped SSH key and registers it with OS Login. |
| `init-variables` | Validates required substitutions and the bucket profile, derives region/resource names, and writes the remote environment file. |
| `cleanup-leaked-resources` | Best-effort deletion of VMs older than ten hours whose names match the infrastructure prefix. |
| `create-buckets` | Ensures the persistent HNS results bucket exists in the run region. |
| `create-vm` | Creates the Ubuntu benchmark VM with the selected machine type, gVNIC, and TIER_1 networking. |
| `run-subsystembenchmarks` | Waits for SSH, copies the source, creates a virtual environment, installs dependencies and overrides, runs the group, and uploads available artifacts. |
| `cleanup-ssh-key` | Removes the temporary OS Login key. |
| `cleanup-vm` | Deletes the benchmark VM. |
| `delete-buckets` | Sweeps case buckets matching this run's unique prefix. |
| `check-failure` | Fails the build after cleanup if an earlier step recorded a failure. |

The setup script installs the copied checkout in editable mode, then the
selected group's pinned `requirements.txt`, then `_REQUIREMENTS`. Installing the
override last makes it possible to test an exact pip-installable `gcsfs` build
instead of the copied checkout's version. The resolved environment and the
normalized override are published with every result row.

## Run substitutions

### Required

| Substitution | Description |
| :--- | :--- |
| `_INFRA_PREFIX` | Lowercase resource prefix used for the VM, temporary case-bucket prefix, and persistent `gs://<_INFRA_PREFIX>-run-results` bucket. |
| `_ZONE` | Compute Engine zone for the VM and zonal/RAPID case buckets, for example `us-central1-a`. The bucket region is derived from it. |
| `_VM_SERVICE_ACCOUNT` | Email of the service account attached to the benchmark VM. |

### Optional

| Substitution | Default | Description |
| :--- | :--- | :--- |
| `_GROUP` | `dataloading/huggingface_datasets` | Runnable `<subsystem>/<implementation>` group. Group discovery requires a `requirements.txt` in that directory. |
| `_BUCKET_TYPE` | `regional` | Case-bucket profile: `regional`, `zonal`, or `hns`. `zonal` creates RAPID/HNS buckets in `_ZONE`; `hns` creates regional HNS buckets. |
| `_MACHINE_TYPE` | `c4-standard-192` | Compute Engine machine type used for the benchmark VM. |
| `_REQUIREMENTS` | empty | Whitespace-separated pip requirement specs installed last. Use this to select the `gcsfs` build under test. |
| `_SWEEP_AXES` | empty | Whitespace-separated configuration-axis names. Empty runs all configured cases; any selection still includes the implicit baseline case. |

`PROJECT_ID`, `LOCATION`, `BUILD_ID`, `BRANCH_NAME`, and `COMMIT_SHA` are Cloud
Build built-ins rather than custom substitutions.

The "baseline" above is only the reference configuration for one-factor
experiments within this run. The pipeline does not download prior results,
perform historical comparisons, or fail because current performance is slower
than an earlier run.

## Creating the run trigger

Create a manual trigger and adjust the placeholders for your project and source
connection:

```bash
gcloud builds triggers create manual \
  --name=gcsfs-subsystembench-run \
  --region=<LOCATION> \
  --repo=<YOUR_REPO> \
  --repo-type=<GITHUB|CLOUD_SOURCE_REPOSITORIES> \
  --branch=main \
  --build-config=cloudbuild/subsystembenchmarks/subsystembenchmarks-cloudbuild.yaml \
  --substitutions=_INFRA_PREFIX=gcsfs-subsystembench,_ZONE=us-central1-a,_VM_SERVICE_ACCOUNT=<SA_EMAIL>
```

Trigger overrides can select a different bucket profile, machine, requirement,
or subset of configuration axes. Because `_SWEEP_AXES` contains spaces when
selecting more than one axis, quote the complete `--substitutions` argument in
shell invocations.

## Results and failure behavior

The remote upload script copies the complete local `__run__` directory to:

```text
gs://<_INFRA_PREFIX>-run-results/subsystembenchmarks/
`-- branch=<branch>/
    `-- <YYYYMMDD>/
        `-- <build_id>/
            `-- <YYYYMMDD-HHMMSS>/
                |-- results.json
                `-- results.csv
```

If benchmark execution returns nonzero but produced artifacts, the pipeline
records the failure and still attempts the upload before propagating the
original status. Cleanup steps run afterward. Upload or infrastructure failures
are also recorded and reported by `check-failure`.

The Cloud Build execution path passes `--require-amplification`, so eligible
rows that still lack GCS read metrics after the wait and retry fail the run. A
nonzero build therefore indicates execution, correctness, infrastructure,
upload, or required-metric failure. A historical performance change by itself
never fails the build.

## BigQuery ingestion

The ingestion pipeline requires both substitutions:

| Substitution | Description |
| :--- | :--- |
| `_DATASET_NAME` | BigQuery dataset to create or update in the build `LOCATION`. |
| `_INFRA_PREFIX` | Prefix of the run pipeline's results bucket. It must match the run trigger. |

Create its trigger separately:

```bash
gcloud builds triggers create manual \
  --name=gcsfs-subsystembench-ingest \
  --region=<LOCATION> \
  --repo=<YOUR_REPO> \
  --repo-type=<GITHUB|CLOUD_SOURCE_REPOSITORIES> \
  --branch=main \
  --build-config=cloudbuild/subsystembenchmarks/subsystembenchmarks-ingestion-cloudbuild.yaml \
  --substitutions=_INFRA_PREFIX=gcsfs-subsystembench,_DATASET_NAME=gcsfs_subsystembenchmarks
```

`subsystembenchmarks_schema.json` is the authoritative external-table schema and
artifact URI pattern. On every ingestion run, the pipeline:

1. Ensures `_DATASET_NAME` exists in `LOCATION`.
2. Recreates `staging_subsystem` as an external CSV table over the results
   bucket, so checked-in schema changes take effect.
3. Adds new staging columns to `history_subsystem` when necessary.
4. Inserts rows from previously unseen artifacts, deriving `run_date`,
   `build_id`, `run_timestamp`, `source_uri`, and `branch_name` from each object
   path.

`history_subsystem` is partitioned by `run_date` and deduplicated by
`source_uri`, so rerunning ingestion does not append the same artifact twice.
The complete evolving metric-column inventory lives in
[`subsystembenchmarks_schema.json`](subsystembenchmarks_schema.json).
