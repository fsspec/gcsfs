# GCSFS Subsystem Benchmarks

## Introduction

GCSFS subsystem benchmarks isolate training-relevant storage paths that are too
large for an operation-level
[microbenchmark](../microbenchmarks/README.md), but more focused than an
end-to-end [macrobenchmark](../macrobenchmarks/README.md). They preserve the
framework behavior around `gcsfs` while separating one subsystem from the rest
of a training workload.

The only currently runnable group is
`dataloading/huggingface_datasets`. It measures full-corpus streaming reads of a
synthetic dataset through Hugging Face Datasets, `fsspec`, and `gcsfs`, with a
PyTorch `DataLoader` consuming the stream.

> **This README describes the workload: what it runs, what is timed, and how to
> debug it directly.** The normal way to provision the benchmark VM, run the
> suite, upload results, and ingest them into BigQuery is documented in the
> [Cloud Build automation guide](../../../../cloudbuild/subsystembenchmarks/README.md).

## Workload architecture

Groups follow a `<subsystem>/<implementation>` layout. A group owns its pinned
requirements and configuration, while the package-level harness owns case
lifecycle, reporting, resource monitoring, and command-line execution. A new
implementation can therefore be added as another independently installable
group without changing an existing group's dependency set.

For `dataloading/huggingface_datasets`, the execution chain is:

1. `run.py` validates the selected group, exports run-level bucket settings, and
   starts the group's pytest-benchmark cases.
2. The Hugging Face configurator expands `configs.yaml` into an implicit
   baseline plus one-factor variants.
3. `read_case.py` manages the common data-loading lifecycle and delegates the
   actual streaming read to the Hugging Face driver.
4. The driver builds a streaming Hugging Face dataset backed by `gcsfs`, wraps
   it in a PyTorch `DataLoader`, and optionally splits it across local ranks.
5. The common report code converts pytest-benchmark JSON into a flat CSV and
   the runner enriches eligible rows with Cloud Monitoring read metrics.

## Per-case lifecycle

Each benchmark case is self-contained:

1. Create an isolated GCS bucket using the run's bucket profile.
2. Generate and upload a deterministic synthetic Parquet or JSONL corpus.
3. Build the streaming dataset and `DataLoader`.
4. Iterate the complete corpus for each measured round, optionally across
   multiple local ranks.
5. Verify that every round yielded the manifest's expected sample count. A
   partial read fails the case instead of reporting inflated throughput.
6. Publish workload, timing, resource, environment, and dependency provenance
   fields into the benchmark result.
7. Delete the case bucket. Cloud Build also sweeps leaked buckets after the run
   as a safety net.
8. After all cases finish, query Cloud Monitoring for each isolated bucket's
   read bytes and request count, then add read-amplification fields to the CSV.

The isolated bucket is important: GCS read metrics are bucket-scoped and sampled
on a 60-second grid. A bucket per case keeps the server-side observations
attributable to one configuration even when measurement windows overlap after
grid alignment.

## Measurement boundaries

Synthetic corpus generation and upload are setup work and are not part of the
timed read window. Dataset construction is also outside the full-corpus rounds,
but its duration is reported separately.

One round means one complete iteration over the generated corpus. For a
distributed case, its duration spans from the earliest rank start to the latest
rank finish, so launch skew and the slowest rank are included. Time to first
batch uses the same global boundary: it ends when the last rank has produced its
first batch.

The benchmark reports logical throughput from the stored corpus size and round
duration. Read amplification is a different, server-observed measurement:
Cloud Monitoring bytes sent by GCS are divided by the logical dataset bytes
expected across all measured rounds. Values above 1 indicate that GCS served
more bytes than the logical full-corpus reads required.

## Configuration

The group's
[`configs.yaml`](dataloading/huggingface_datasets/configs.yaml) is the source of
truth for current workload values and experiments. It defines:

- shared values applied to every case;
- an implicit baseline configuration; and
- variants that change one named configuration axis at a time.

`--sweep-axes` accepts a whitespace-separated set of axis names. The baseline
case is always included, which keeps each selected variant comparable within the
same run. Leaving the option empty runs every case currently defined in the
YAML.

Here, "baseline" means only the reference configuration in a one-factor run.
The suite does not retrieve historical results, compare against an earlier run,
or fail on a performance regression.

## Metrics and output

Each case produces one flat result row. The main metric families are:

| Family | Meaning |
| :--- | :--- |
| Logical read performance | Mean stored bytes and samples consumed per second across full-corpus rounds. |
| Latency and duration | Dataset initialization time, time to first batch, and full-corpus duration statistics. |
| Resource use | Peak process-tree CPU and resident memory, plus mean host network receive and send rates. |
| Configuration and provenance | Case identity, selected sweep axis, dataset shape, rank/worker settings, machine environment, source revision, and resolved Python requirements. |
| GCS read behavior | Server-observed read bytes, read requests, and logical-to-physical read amplification. |

The authoritative CSV and BigQuery column inventory is
[`subsystembenchmarks_schema.json`](../../../../cloudbuild/subsystembenchmarks/subsystembenchmarks_schema.json).
Keeping the schema in one place avoids duplicating a field list while the suite
is evolving.

Direct runs write timestamped artifacts under:

```text
gcsfs/tests/perf/subsystembenchmarks/__run__/<YYYYMMDD-HHMMSS>/
├── results.json
└── results.csv
```

The runner also prints the generated CSV as a Markdown table when the run
finishes.

## Running through Cloud Build

Cloud Build is the supported operational path. It provides the high-bandwidth
VM, installs the group requirements and the selected `gcsfs` build, uploads
available artifacts when possible after a case failure, enforces
read-amplification collection, and cleans up infrastructure.

See the [automation guide](../../../../cloudbuild/subsystembenchmarks/README.md)
for cost, prerequisites, substitutions, trigger setup, result storage, and
BigQuery ingestion.

## Running directly for debugging

> **A direct run uses real, billable GCP resources.** It creates and deletes one
> bucket per case, uploads a synthetic corpus, reads it for every configured
> round, and may wait for Cloud Monitoring ingestion. Use a unique lowercase
> bucket prefix and inspect the project for leaked buckets after interrupted
> runs.

From the repository root, install the package and the current group's pinned
dependencies:

```bash
python -m pip install -e .
python -m pip install -r \
  gcsfs/tests/perf/subsystembenchmarks/dataloading/huggingface_datasets/requirements.txt
```

Authenticate with Application Default Credentials that can create and delete
the case buckets. Monitoring read permission is also needed for amplification
enrichment. Then run:

```bash
python -m gcsfs.tests.perf.subsystembenchmarks.run \
  --group=dataloading/huggingface_datasets \
  --bucket-prefix=<UNIQUE_LOWERCASE_PREFIX> \
  --project=<PROJECT_ID> \
  --location=us-central1 \
  --bucket-type=regional
```

Useful optional arguments:

- `--sweep-axes="<AXIS> <AXIS>"` limits the run to the named axes plus the
  baseline.
- `--bucket-type=zonal --zone=<ZONE>` creates zonal RAPID/HNS case buckets; the
  zone is required for this profile.
- `--bucket-type=hns` creates regional hierarchical-namespace case buckets.
- `--require-amplification` fails the run if eligible rows still lack GCS read
  metrics after the configured wait and retry.

## Contributor checks

Run all subsystem benchmark infrastructure tests without executing the live GCS
benchmark case:

```bash
pytest gcsfs/tests/perf/subsystembenchmarks --run-benchmarks-infra
```

## Repository layout

```text
subsystembenchmarks/
├── README.md
├── run.py                         # CLI, group discovery, report enrichment.
├── conftest.py                    # Benchmark hooks and resource fixture.
├── _common/                       # Config loading, reporting, provenance, metrics.
├── dataloading/
│   ├── amplification.py           # Cloud Monitoring read-metric enrichment.
│   ├── bucket.py                  # Per-case GCS bucket lifecycle.
│   ├── datagen.py                 # Synthetic Parquet/JSONL corpus generation.
│   ├── driver.py                  # Read-driver contract and rank reduction.
│   ├── read_case.py               # Shared timed case lifecycle.
│   └── huggingface_datasets/
│       ├── configs.yaml           # Current baseline and one-factor variants.
│       ├── configs.py
│       ├── parameters.py
│       ├── requirements.txt
│       └── read/                  # Hugging Face streaming read driver and case.
└── tests/                         # Package-level infrastructure tests.
```
