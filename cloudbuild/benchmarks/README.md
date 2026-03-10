# GCSFS Benchmarks Automation

## 1. Introduction
This directory contains Cloud Build configurations to automate the execution of GCSFS microbenchmarks and the ingestion of their results into BigQuery. This automation allows for nightly performance tracking, regression detection, and historical analysis of GCSFS performance across different bucket types (Regional, Zonal, HNS).

## 2. Benchmarks Run Pipeline
**File:** `benchmarks-cloudbuild.yaml`

This pipeline handles the end-to-end execution of the performance benchmarks. It is designed to be ephemeral, creating the necessary infrastructure on-the-fly and tearing it down after execution to minimize costs.

### High-Level Flow
1.  **Infrastructure Setup**:
    *   **SSH Key Generation**: Generates a temporary SSH key for secure communication with the test VM.
    *   **Bucket Creation**: Creates temporary GCS buckets based on the specified types (Regional, Zonal, HNS) and a persistent "Results Bucket" if it doesn't exist.
    *   **VM Provisioning**: Provisions a high-performance Compute Engine VM (e.g., `c4-standard-192`) optimized for networking to run the tests.
2.  **Benchmark Execution**:
    *   **Setup**: Copies the GCSFS source code to the VM and installs Python dependencies (including `gcsfs` and `google-cloud-storage`).
    *   **Execution**: Runs the benchmark suite (`run.py`) inside the VM based on the provided configuration. It iterates through the defined benchmark groups.
3.  **Result Handling**:
    *   **Upload**: Uploads the generated CSV results and JSON logs from the VM to the persistent GCS Results Bucket.
    *   **Path Format**: Results are stored at `gs://<results_bucket>/<date>/<run_id>/`.
4.  **Cleanup**:
    *   Deletes the temporary VM and the temporary test buckets (Regional, Zonal, HNS).
    *   Removes the temporary SSH keys.

## 3. Benchmarks Ingestion Pipeline
**File:** `benchmarks-ingestion-cloudbuild.yaml`

This pipeline loads the benchmark results from the GCS Results Bucket into BigQuery for analysis. It uses an External Table for staging and a native partitioned table for historical storage.

### Ingestion Process
1.  **Dataset Preparation**: Checks if the target BigQuery dataset exists and creates it if necessary.
2.  **Staging Table**: Creates or updates an External Table (`staging`) that points directly to the CSV files in the GCS Results Bucket. This table uses the schema defined in `benchmarks_schema.json`.
3.  **Data Ingestion (`ingest.sql`)**:
    *   **History Table**: Ensures a `history` table exists, partitioned by `run_date`.
    *   **Schema Evolution**: Automatically detects new columns in the `staging` table and adds them to the `history` table to support evolving benchmark metrics.
    *   **Insertion**: Inserts new records from `staging` into `history`.
    *   **Idempotency**: Uses the `source_uri` (file path) to prevent duplicate data insertion if the pipeline is re-run.
    *   **Metadata Extraction**: Extracts metadata like `run_date`, `build_id`, and `run_timestamp` directly from the source file path.

## 4. Cloud Build Triggers
To run this automation in your own GCP project, you need to set up atleast two Cloud Build triggers.

### Benchmarks Run Trigger
*   **Configuration File**: `cloudbuild/benchmarks/benchmarks-cloudbuild.yaml`
*   **Substitutions**:
    *   `_INFRA_PREFIX`: A prefix for created resources (e.g., `gcsfs-perf`).
    *   `_ZONE`: The GCP zone for the VM and Zonal buckets (e.g., `us-central1-a`).
    *   `_VM_SERVICE_ACCOUNT`: The service account email attached to the VM (must have GCS read/write permissions).
    *   `_BENCHMARK_CONFIG`: Space-separated list of benchmark groups/configs to run (e.g., `read:read_seq write:write_seq`).
    *   `_BUCKET_TYPES`: Space-separated list of bucket types to create and test against (e.g., `regional zonal hns`).

### Ingestion Trigger
*   **Configuration File**: `cloudbuild/benchmarks/benchmarks-ingestion-cloudbuild.yaml`
*   **Substitutions**:
    *   `_DATASET_NAME`: The name of the BigQuery dataset to store results (e.g., `gcsfs_benchmarks`).
*   **Trigger Event**: This trigger is typically scheduled to run after the benchmarks pipeline completes, or triggered manually.
