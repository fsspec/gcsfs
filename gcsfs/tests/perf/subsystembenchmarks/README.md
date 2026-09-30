# GCSFS Subsystem Benchmarks

Subsystem benchmarks time one storage-heavy part of a training job, such as
data loading or checkpointing, through a real framework on top of `gcsfs`. They
sit between the per-operation [microbenchmarks](../microbenchmarks/README.md)
and the end-to-end [macrobenchmarks](../macrobenchmarks/README.md).

The usual way to run them is Cloud Build; see the
[automation guide](../../../../cloudbuild/subsystembenchmarks/README.md).

## Groups

A group is a `<subsystem>/<implementation>` directory with its own
`requirements.txt`. `run.py` treats every such directory as a runnable group.

| Group | Framework path | What is timed |
| :-- | :-- | :-- |
| `dataloading/huggingface_datasets` | Hugging Face Datasets (streaming) → PyTorch `DataLoader` | Full reads of a synthetic Parquet or JSONL corpus |
| `dataloading/ray_data` | Ray Data `read_parquet` → `iter_torch_batches` | Full reads of a synthetic Parquet corpus |
| `dataloading/webdataset` | WebDataset → PyTorch `DataLoader`; `gs://` opened by a `gcsfs` opener | Reads of synthetic image tar shards |
| `checkpointing/pytorch` | PyTorch `torch.distributed.checkpoint` (`FsspecReader`) | Sharded DCP checkpoint read of Llama-3.1-8B and OLMoE-1B-7B with AdamW state across FSDP2/HSDP/TP/PP/EP layouts |
| `checkpointing/pytorch_lightning` | PyTorch Lightning | Checkpoint write and read of Llama-3.1-8B with AdamW state |
| `checkpointing/ray_pytorch` | Ray actors with PyTorch `torch.save` / DCP | Checkpoint write and read of Llama-3.1-8B with AdamW state |

Data-loading groups detect CUDA automatically. On a GPU host, each round
includes copying batches to the GPU. The exception is WebDataset with
`decode: false`, where there is nothing to copy. Checkpointing groups always
run on CPU (`gloo`).

## How a case runs

Every case creates its own GCS bucket and tries to delete it when the case
ends.

**Data loading**

1. Generate and upload a synthetic corpus. This is not timed.
2. Build the dataset. Build time is reported separately.
3. Read the whole corpus once per round. With several ranks, a round lasts
   from the earliest rank start to the latest rank finish.
4. Fail the case if any round's sample count differs from the corpus's.

**Checkpointing**

- `checkpoint_write` times saving a checkpoint to GCS.
  - `pytorch_lightning` times `trainer.save_checkpoint()`.
  - `ray_pytorch` times only the upload from local staging to GCS.
- `checkpoint_read` first writes a checkpoint (not timed), then times reading
  it.
  - `pytorch` times `torch.distributed.checkpoint.load()` via `FsspecReader`.
  - `pytorch_lightning` times `trainer.strategy.load_checkpoint()`.
  - `ray_pytorch` times the download plus loading the state into the model.

**Read amplification**

After all cases finish, the runner asks Cloud Monitoring how many bytes GCS
served (`ReadObject`/`BidiReadObject`) from each case bucket.

- It divides that by the stored corpus or checkpoint size × rounds.
- It is intended for data-loading and `checkpoint_read` cases.
- For `pytorch_lightning` reads with `ddp`, `fsdp_full`, or
  `model_parallel_full`, every rank reads the whole checkpoint. The ratio does
  not account for this.

The monitoring window covers the driver run: all rounds plus dataset build or
model setup. It excludes the corpus upload and the untimed setup checkpoint.
It is widened to Cloud Monitoring's 60-second grid.

## Configuration

Each group's `configs.yaml` defines:

- `common`: shared settings plus a `baseline` configuration.
- `scenarios`: a list of entries, each with a `name`, a `scenario`, and
  `variants`. Each entry runs its own baseline plus its variants. Each variant
  names an `axis` and overrides one or more baseline values.

Rules:

- `enabled: false` parks a variant. It is still validated but not run. The
  value must be a YAML boolean.
- `--sweep-axes="a b"` runs the baseline plus the named axes. An axis name
  that is unknown, or whose variants are all parked, is an error.
- Without `--sweep-axes`, every enabled case runs.

"Baseline" only means the reference case within one run. The suite does not
compare against earlier runs.

## Output

```text
gcsfs/tests/perf/subsystembenchmarks/__run__/<YYYYMMDD-HHMMSS>/
├── results.json   # pytest-benchmark output
└── results.csv    # one row per case
```

The runner also prints the CSV as a table if `prettytable` is installed. For
column definitions, see
[`subsystembenchmarks_schema.json`](../../../../cloudbuild/subsystembenchmarks/subsystembenchmarks_schema.json).

## Running directly (debugging)

> [!WARNING]
> A direct run creates, uses, and deletes real GCS buckets in your project,
> which costs money. After an interrupted run, check the project for leftover
> buckets.

From the repository root:

```bash
python -m pip install -e .
python -m pip install -r gcsfs/tests/perf/subsystembenchmarks/<GROUP>/requirements.txt

python -m gcsfs.tests.perf.subsystembenchmarks.run \
  --group=<GROUP> \
  --bucket-prefix=<unique-lowercase-prefix> \
  --project=<PROJECT_ID> \
  --location=us-central1
```

You need Application Default Credentials that can create and delete buckets.
Read-amplification enrichment also needs Cloud Monitoring read access.

For checkpointing groups:

- The default model is `gs://huggingface-model-weights/Llama-3.1-8B` (or
  `gs://gcs-aiml-huggingface-model-weights/Llama-3.1-8B` for
  `checkpointing/pytorch`).
- A `gs://.../<name>` model is loaded from `/tmp/<name>`, so copy it there
  first:

  ```bash
  gcloud storage cp -r gs://huggingface-model-weights/Llama-3.1-8B /tmp/
  ```

- `--model-id` also accepts a Hugging Face repo ID or a local path.
- These groups need a very large-memory host. A code comment in
  `checkpointing/checkpoint_case.py` targets a 732 GB VM.

| Flag | Default | Meaning |
| :-- | :-- | :-- |
| `--group` | required | One group from the table above. |
| `--bucket-prefix` | required | Name prefix for the per-case buckets. |
| `--project` | required | GCP project for buckets and metrics. |
| `--location` | required | Bucket region. |
| `--bucket-type` | `regional` | `regional`, `zonal` (needs `--zone`), or `hns`. |
| `--zone` | none | Zone for `zonal` buckets. |
| `--sweep-axes` | all | Axes to run, in addition to the baseline. |
| `--filter` | none | pytest `-k` expression. |
| `--model-id` | from config | Checkpointing model override. |
| `--amplification-wait` | `300` | Seconds to wait before querying Cloud Monitoring. |
| `--amplification-retry-wait` | `60` | Seconds to wait before one retry of missing metrics. |
| `--require-amplification` | off | Fail if eligible rows still lack amplification metrics. |

## Infrastructure tests

These are unit tests only; they do not run the live GCS cases:

```bash
pytest gcsfs/tests/perf/subsystembenchmarks --run-benchmarks-infra
```

## Layout

```text
subsystembenchmarks/
├── run.py              # CLI, group discovery, amplification, table output
├── conftest.py
├── _common/            # pytest invocation, config loading, CSV report, metadata
├── dataloading/
│   ├── read_case.py    # shared data-loading case lifecycle
│   ├── driver.py       # read-driver interface, rank reduction
│   ├── configurator.py # config loading for read groups
│   ├── bucket.py       # per-case bucket (also used by checkpointing)
│   ├── amplification.py
│   ├── datagen.py      # synthetic Parquet/JSONL corpus
│   ├── device.py       # GPU batch delivery
│   ├── huggingface_datasets/
│   ├── ray_data/
│   └── webdataset/     # also gcsfs_opener.py, imagegen.py
├── checkpointing/
│   ├── checkpoint_case.py  # shared write/read case lifecycle
│   ├── driver.py           # checkpoint-driver interface
│   ├── configurator.py     # config loading for checkpoint groups
│   ├── _dist.py            # shared gloo spawn and round reduction
│   ├── _llama_tp.py        # shared LLaMA tensor-parallel plan
│   ├── pytorch/            # also model.py, parallelize.py, state.py
│   ├── pytorch_lightning/  # also common.py
│   └── ray_pytorch/        # also common.py
└── tests/
```

Each group directory has `configs.yaml`, `configs.py`, `parameters.py`,
`requirements.txt`, and `read/` and/or `write/` directories containing the
driver and the benchmark test.
