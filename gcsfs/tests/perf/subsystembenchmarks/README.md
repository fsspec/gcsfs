# GCSFS Subsystem Benchmarks

## Introduction

GCSFS subsystem benchmarks isolate training-relevant storage paths that are too
large for an operation-level
[microbenchmark](../microbenchmarks/README.md), but more focused than an
end-to-end [macrobenchmark](../macrobenchmarks/README.md). They preserve the
framework behavior around `gcsfs` while separating one subsystem from the rest
of a training workload.

The currently runnable groups are:

- `dataloading/huggingface_datasets`: Measures full-corpus streaming reads of a synthetic dataset through Hugging Face Datasets, `fsspec`, and `gcsfs`, with a PyTorch `DataLoader` consuming the stream.
- `dataloading/ray_data`: Measures full-corpus streaming reads of a synthetic Parquet dataset through Ray Data, `pyarrow.fs`, `fsspec`, and `gcsfs` on CPU.
- `dataloading/webdataset`: Measures full-corpus streaming reads of synthetic image tar shards through WebDataset and a PyTorch `DataLoader`; `gs://` reads are routed to `gcsfs` by a registered opener. The default sweep keeps storage-bound axes; image-preparation and pipeline axes are parked with `enabled: false`.
- `checkpointing/pytorch_lightning`: Measures checkpoint write and read performance using PyTorch Lightning and various training strategies (DDP, FSDP, Model Parallel) on CPU-simulated environments.
- `checkpointing/pytorch`: Measures `torch.distributed.checkpoint.load` from GCS via `FsspecReader` and `gcsfs` across PyTorch parallelism strategies (FSDP2, HSDP, TP, FSDP+TP, HSDP+TP, PP, PP+FSDP, PP+FSDP+TP, PP+HSDP+TP) with identical save and load topology on CPU gloo, using Llama-3.1-8B built from config on meta.

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

### CPU and GPU hosts

Data-loading benchmarks auto-detect CUDA (published as
`compute_accelerator_type`); no configuration change is needed. On a CPU host,
batches end in host memory exactly as before. On a GPU host, every loader
delivers each rank's batches to a GPU the way training jobs do, via
`dataloading/device.py`: batches are pinned in host memory, copied with
`non_blocking=True`, and each round synchronizes once at its end so the round
duration includes the host-to-device transfer. CUDA tensors are never created
in loader worker processes, as PyTorch recommends.

| Loader | GPU per rank | What is transferred |
|---|---|---|
| Hugging Face Datasets | `rank % device_count` | Token and label tensors; text strings stay on the host. |
| WebDataset | `rank % device_count` when `decode: true`; none with the baseline `decode: false` | Decoded image tensors when `decode: true`. With `decode: false` samples are raw bytes with nothing to copy, so the rank neither binds a GPU nor pins, and runs as on a CPU host. |
| Ray Data | Ray assigns `num_gpus = min(1, gpus / world_size)` per consumer task (fractional when ranks outnumber GPUs) | `pretok_parquet` via Ray's native `iter_torch_batches(device=..., pin_memory=True)`; `text_parquet` labels are copied by the shared feed. |

When ranks outnumber GPUs, ranks share GPUs.

Each rank creates its CUDA context before timing starts, as a training job
already has by the time its data loop runs. The exception is Ray Data with
`split_by_node`: consumer tasks are dispatched inside the round, so creating
the context is charged to the round in which a Ray worker first runs
(normally round 1, since Ray reuses workers).

## Configuration

The group's
[`configs.yaml`](dataloading/huggingface_datasets/configs.yaml) is the source of
truth for current workload values and experiments. It defines:

- shared values applied to every case;
- an implicit baseline configuration; and
- variants that change one named configuration axis at a time.

A variant can set `enabled: false` to park it without deleting it. Parked
variants are still built, validated, and checked for duplicate benchmark IDs,
so they cannot break unnoticed, but benchmark runs skip them. The value must be
a YAML boolean; a string such as `"false"` is rejected. To run a parked variant
again, set `enabled: true` (or remove the key) in `configs.yaml`.

`--sweep-axes` accepts a whitespace-separated set of axis names. The baseline
case is always included, which keeps each selected variant comparable within the
same run. Leaving the option empty runs every enabled case defined in the YAML.
`--sweep-axes` only selects among enabled variants; it cannot bring back a
parked one.

Here, "baseline" means only the reference configuration in a one-factor run.
The suite does not retrieve historical results, compare against an earlier run,
or fail on a performance regression.

## Metrics and output

Each case produces one flat result row. The main metric families are:

| Family                       | Meaning                                                                                                                                          |
| :--------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------- |
| Logical read performance     | Mean stored bytes and samples consumed per second across full-corpus rounds.                                                                     |
| Latency and duration         | Dataset initialization time, time to first batch, and full-corpus duration statistics.                                                           |
| Resource use                 | Peak process-tree CPU and resident memory, plus mean host network receive and send rates.                                                        |
| Configuration and provenance | Case identity, selected sweep axis, dataset shape, rank/worker settings, machine environment, source revision, and resolved Python requirements. |
| GCS read behavior            | Server-observed read bytes, read requests, and logical-to-physical read amplification.                                                           |

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

For dataloading:

```bash
python -m gcsfs.tests.perf.subsystembenchmarks.run \
  --group=dataloading/huggingface_datasets \
  --bucket-prefix=<UNIQUE_LOWERCASE_PREFIX> \
  --project=<PROJECT_ID> \
  --location=us-central1 \
  --bucket-type=regional
```

For checkpointing:

```bash
python -m gcsfs.tests.perf.subsystembenchmarks.run \
  --group=checkpointing/pytorch_lightning \
  --bucket-prefix=<UNIQUE_LOWERCASE_PREFIX> \
  --project=<PROJECT_ID> \
  --location=us-central1 \
  --bucket-type=zonal \
  --zone=us-central1-b
```

Or for native PyTorch DCP checkpointing:

```bash
python -m gcsfs.tests.perf.subsystembenchmarks.run \
  --group=checkpointing/pytorch \
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
├── checkpointing/
│   ├── checkpoint_case.py         # Shared timed checkpoint write case lifecycle.
│   ├── configurator.py            # Checkpointing config loader.
│   ├── pytorch/
│   │   ├── configs.yaml           # Current baseline and strategy variants.
│   │   ├── configs.py
│   │   ├── parameters.py
│   │   ├── model.py               # Meta-init model, seeded init, AdamW state.
│   │   ├── parallelize.py         # Device mesh, FSDP2/TP/PP layouts.
│   │   ├── state.py               # FQN state dict, checksums, expected bytes.
│   │   ├── requirements.txt
│   │   ├── read/
│   │   │   ├── driver.py          # PyTorch DCP read driver.
│   │   │   └── test_checkpoint.py # PyTorch DCP checkpoint read benchmark case.
│   │   └── tests/
│   └── pytorch_lightning/
│       ├── configs.yaml           # Current baseline and strategy variants.
│       ├── configs.py
│       ├── parameters.py
│       ├── requirements.txt
│       └── write/
│           ├── driver.py          # PyTorch Lightning write driver (processes launcher).
│           └── test_checkpoint.py # PyTorch Lightning checkpoint write benchmark case.
└── tests/                         # Package-level infrastructure tests.
```
