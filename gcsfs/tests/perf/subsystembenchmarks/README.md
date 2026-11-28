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
| `dataloading/ray_data` | Ray Data `read_parquet` → `iter_torch_batches` (`iter_batches` with feature columns) | Full reads of a synthetic Parquet corpus, or column-projected reads of a wide table |
| `dataloading/webdataset` | WebDataset → PyTorch `DataLoader`; `gs://` opened by a `gcsfs` opener | Reads of synthetic image tar shards |
| `checkpointing/pytorch_save` | PyTorch `torch.distributed.checkpoint` (`FsspecWriter`) | Sharded DCP checkpoint save of Llama-3.1-8B and OLMoE-1B-7B (real weights in fp32 + random AdamW moments) across FSDP2/HSDP/TP/PP/EP layouts |
| `checkpointing/pytorch_load` | PyTorch `torch.distributed.checkpoint` (`FsspecReader`) | Same-layout sharded DCP checkpoint load of Llama-3.1-8B and OLMoE-1B-7B (real weights in fp32 + random AdamW moments) across FSDP2/HSDP/TP/PP/EP layouts |
| `checkpointing/pytorch_reshard` | PyTorch `torch.distributed.checkpoint` (`FsspecReader`) | Cross-layout resharded DCP checkpoint load of Llama-3.1-8B and OLMoE-1B-7B (real weights in fp32 + random AdamW moments) across FSDP2/HSDP/TP/PP/EP source→target pairs |
| `checkpointing/pytorch_lightning` | PyTorch Lightning | Checkpoint write and read of Llama-3.1-8B with AdamW state |
| `checkpointing/ray_pytorch` | Ray actors with PyTorch `torch.save` / DCP | Checkpoint write and read of Llama-3.1-8B with AdamW state |

`--device` defaults to `auto`, which trusts `torch.accelerator.is_available()`
to select `cuda`, `tpu`, or `cpu`; passing an explicit `--device=cuda` or
`--device=tpu` fails fast if that accelerator is unavailable.
Data-loading groups detect CUDA automatically. On a GPU host, each round
includes copying batches to the GPU. The exceptions are WebDataset with
`decode: false` and Ray Data cases with feature columns, where there is nothing to copy.
Checkpointing groups run on the device that `--device` resolves (`gloo` on CPU,
`cpu:gloo,cuda:nccl` / `nccl` on CUDA GPU, `tpu_dist` on TPU), placing models,
optimizer states, and meshes on the active accelerator and synchronizing before
each round's end timestamp. Cases whose `world_size` exceeds one node (such as
the 16-rank DCP cases on 8-device nodes) run SPMD across
`ceil(world_size / devices_per_node)` nodes via `torchrun` when launched with
`--nodes`.

- **Skip rules:** without `--nodes`, a case that needs more devices than the host
  has skips (`needs W devices; host has D`; CPU hosts allow up to 16 ranks).
  Ranks own one device each, except data loading on GPU, where ranks share GPUs
  and any host with a GPU runs every case. Only
  `checkpointing/pytorch_{save,load,reshard}` cases span nodes; every other case
  needs one node. Known
  framework limitations on a specific accelerator are declared explicitly in
  `_common/limitations.py` and documented with repros in
  [`limitations.md`](limitations.md) (for example, `checkpointing/pytorch_lightning`
  skips on TPU because Lightning does not support TorchTPU).

### Wide-table projected reads (`dataloading/ray_data`)

`pretok_parquet` can carry extra binary feature columns, which models feature
tables that jobs read with a column projection (for example 286 of 1,134
columns). Such reads turn into hundreds of small range reads per file.

| Parameter | Meaning |
| :-- | :-- |
| `feature_column_count`, `feature_value_bytes` | Extra binary columns next to `tokens` and `label`, `feature_value_bytes` per value on average (default 10). 0 keeps the plain corpus. |
| `projected_feature_count` | Feature columns the read selects besides `tokens` and `label` (`select_columns`, pushed down into the scan). 0 reads all of them. |
| `projection_run_length` | Selected columns are picked in blocks of this many adjacent columns: 1 (default) scatters them, larger values cluster them. Blocks can merge or be cut. This sets how many separate reads Arrow issues per row group. |
| `cache_type`, `block_size` | gcsfs `open()` options for every file Ray opens, for any `ray_data` case. pyarrow's `FSSpecHandler` passes none, so the driver applies them as filesystem defaults. Unset keeps gcsfs's defaults. |

With feature columns, batches stay pyarrow tables (binary columns have no tensor
form), so these cases do not copy batches to the GPU.

With a projection, `dataset_read_throughput_mean_bytes_per_second` and
`dataset_read_amplification_ratio` are still relative to the whole stored table.
A read with no over-fetch scores an amplification of about the projected share of
the bytes, not 1, so compare projected rows with each other.

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
  - `pytorch_save` materializes real pretrained weights in fp32 plus seeded
    random fp32 AdamW `exp_avg`/`exp_avg_sq` moments, then times
    `torch.distributed.checkpoint.save()` via `FsspecWriter` into per-round
    `step-<r>/` prefixes.
  - `pytorch_lightning` times `trainer.save_checkpoint()`.
  - `ray_pytorch` times only the upload from local staging to GCS.
- `checkpoint_read` first writes a checkpoint (not timed), then times reading
  it.
  - `pytorch_load` (same layout) and `pytorch_reshard` (`source` → target
    layout) write a real-weight fp32 + random AdamW checkpoint in `setup()`,
    then time `torch.distributed.checkpoint.load()` via `FsspecReader`.
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

From the repository root, install the group's dependencies and then `gcsfs`
itself (installing `gcsfs` last preserves its `fsspec` floor over framework
caps).

- **CPU environment** (include the CPU `torch` pin):

  ```bash
  python -m pip install \
    -r gcsfs/tests/perf/subsystembenchmarks/requirements-cpu.txt \
    -r gcsfs/tests/perf/subsystembenchmarks/<GROUP>/requirements.txt
  python -m pip install -e .
  ```

- **Preinstalled GPU / TPU environment** (omit `requirements-cpu.txt` so the
  preinstalled CUDA or TorchTPU `torch` build is not overwritten):

  ```bash
  python -m pip install -r gcsfs/tests/perf/subsystembenchmarks/<GROUP>/requirements.txt
  python -m pip install -e .
  ```

Then run the suite:

```bash
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
  `checkpointing/pytorch_{save,load,reshard}`).
- All checkpointing groups stage a `gs://` model during `setup()` (via
  `checkpointing/model_staging.py`): it is downloaded once per host into
  `$GCSFS_SUBSYSTEM_MODEL_CACHE_DIR` (default: the system temp dir, such as
  `/tmp`) as `<name>-<hash of bucket/path>`, unless its `*.safetensors`
  weights are already there. Concurrent cases share it through a lock file.

- For `checkpointing/pytorch_{save,load,reshard}`, `--model-id` accepts a
  `gs://` URI or a local directory containing `*.safetensors`;
  `checkpointing/pytorch_lightning` and `checkpointing/ray_pytorch` also accept
  a Hugging Face repo ID. Variants that pin their own `model_id` (the OLMoE
  rows in `checkpointing/pytorch_{save,load,reshard}`) are skipped while
  `--model-id` is set, so the override runs exactly the model you asked for.
- On a CUDA host, `checkpointing/pytorch_{save,load,reshard}` rank capacity
  equals `torch.cuda.device_count()`, so `ws16` rows skip on an 8-GPU host.
- These groups need a very large-memory host. A code comment in
  `checkpointing/checkpoint_case.py` targets a 732 GB VM.

| Flag | Default | Meaning |
| :-- | :-- | :-- |
| `--group` | required | One group from the table above. |
| `--bucket-prefix` | required (unless listing) | Name prefix for the per-case buckets. |
| `--project` | required (unless listing) | GCP project for buckets and metrics. |
| `--location` | required (unless listing) | Bucket region. |
| `--bucket-type` | `regional` | `regional`, `zonal` (needs `--zone`), `hns`, `rapid_cache_cold` or `rapid_cache_warm` (need `--zone`), or an [RCU type](#rcu-bucket-types) (needs `--zone`). |
| `--zone` | none | Zone for `zonal` buckets, Anywhere Caches and Rapid Cache Ultra caches. |
| `--sweep-axes` | all | Axes to run, in addition to the baseline. |
| `--filter` | none | pytest `-k` expression. |
| `--cases` | none | Whitespace-separated exact case IDs to run; an unknown ID is an error. |
| `--model-id` | from config | Checkpointing model override. |
| `--device` | `auto` | `auto`, `cpu`, `cuda`, or `tpu`; `auto` trusts `torch.accelerator.is_available()` (falling back to `cpu`), while explicit `--device=cuda` or `--device=tpu` fails fast if unavailable. |
| `--nodes` | unset | Node count (`>= 1`) in a multi-node job; selects only the cases that need exactly this many nodes (see the skip rules above; DCP cases need `ceil(max(world_size, source world_size) / devices_per_node)`). |
| `--devices-per-node` | probed | Accelerators per node (`>= 1`) used for case sizing, and for listing off-cluster. |
| `--list-node-counts` | off | Print the sorted JSON list of distinct node counts needed by enabled, non-skipped cases and exit 0. |
| `--list-cases` | off | Print the JSON list of enabled, non-skipped cases as `{"id", "nodes"}` and exit 0. |
| `--preflight` | off | Before running cases, create and delete a bucket of `--bucket-type`, and list and read one file of every `gs://` `model_id`. |
| `--results-dir` | `__run__/<timestamp>` | Directory for `results.json` and `results.csv`; concurrent runs need distinct directories. |
| `--amplification-wait` | `300` | Seconds to wait before querying Cloud Monitoring. |
| `--amplification-retry-wait` | `60` | Seconds to wait before one retry of missing metrics. |
| `--require-amplification` | off | Fail if eligible rows still lack amplification metrics. |

### Multi-node environment variables

When running a multi-node job (`--nodes` > 1), set these environment variables on every node (multi-node runs also pass `-x` to `pytest` so all nodes fail fast on the first case error):

| Variable | Meaning |
| :-- | :-- |
| `GCSFS_SUBSYSTEM_NODES` | Total node count (`>= 1`); exported from `--nodes` when passed on the CLI. |
| `GCSFS_SUBSYSTEM_NODE_RANK` | 0-based rank of this node (`0` is the leader; required when `GCSFS_SUBSYSTEM_NODES > 1`). |
| `GCSFS_SUBSYSTEM_LEADER_ADDR` | Hostname or IP address of rank 0 for c10d rendezvous (required when `GCSFS_SUBSYSTEM_NODES > 1`). |
| `GCSFS_SUBSYSTEM_RDZV_PORT` | Base TCP port for c10d rendezvous (defaults to `29400`; incremented per multi-node launch). |
| `GCSFS_SUBSYSTEM_ACCELERATOR_MODEL` | Optional accelerator model label override for reporting metadata (for example TPU chip model). |
| `GCSFS_SUBSYSTEM_TPU_SLICE_TOPOLOGY` | TPU slice topology for multi-node TPU `torchrun` launches (forwarded to `TORCH_TPU_TOPOLOGY`). |
| `GCSFS_SUBSYSTEM_TPU_SLICE_ADDRESSES` | Comma-separated TPU slicebuilder addresses for multi-node TPU `torchrun` launches (forwarded to `TORCH_TPU_SLICEBUILDER_ADDRESSES`). |

### RCU bucket types

`standard_rcu_{z,warm,cold}` (flat bucket) and `hns_rcu_{z,warm,cold}` (HNS
bucket) add a Rapid Cache Ultra cache in `--zone` to each case's bucket and
disable it when the case ends. GCS refuses to delete the bucket for over an hour
after that, so Cloud Build's cleanup steps remove it later. The suffix sets the
cache state reads start from:

- `z`: the corpus or checkpoint is written as `RAPID` objects.
- `warm`: it is written as `STANDARD` objects, and the first MiB of each object
  is read once before timing, followed by a wait of 120 s + 1.5 s per GiB
  (`GCSFS_SUBSYSTEM_RCU_WARM_SETTLE_SECONDS` overrides it).
- `cold`: it is written as `STANDARD` objects and not read before timing.

A case fails if its staged objects have another storage class. Checkpoint saves
run on `*_rcu_z` only. `--zone` must be the VM's zone, because reads from
another zone never fill the cache:

```bash
python -m gcsfs.tests.perf.subsystembenchmarks.run \
  --group=dataloading/huggingface_datasets \
  --bucket-type=standard_rcu_warm \
  --zone=<vm-zone> \
  --bucket-prefix=<unique-lowercase-prefix> \
  --project=<PROJECT_ID> \
  --location=us-central1
```

## Infrastructure tests

These are unit tests only; they do not run the live GCS cases:

```bash
pytest gcsfs/tests/perf/subsystembenchmarks --run-benchmarks-infra
```

## Layout

```text
subsystembenchmarks/
├── run.py              # CLI, group discovery, node-count listing, preflight, amplification
├── conftest.py
├── limitations.md      # proven accelerator/framework limitations cited by _common/limitations.py
├── _common/            # accelerator, placement, limitations, multi-node identity & launcher, preflight, CLI, CSV report
├── dataloading/
│   ├── read_case.py    # shared data-loading case lifecycle
│   ├── driver.py       # read-driver interface, rank reduction
│   ├── configurator.py # config loading for read groups
│   ├── bucket.py       # per-case bucket (also used by checkpointing)
│   ├── amplification.py
│   ├── datagen.py      # synthetic Parquet/JSONL corpus, optional feature columns
│   ├── device.py       # GPU batch delivery
│   ├── huggingface_datasets/
│   ├── ray_data/
│   └── webdataset/     # also gcsfs_opener.py, imagegen.py
├── checkpointing/
│   ├── checkpoint_case.py  # shared write/read case lifecycle
│   ├── driver.py           # checkpoint-driver interface
│   ├── configurator.py     # config loading for checkpoint groups
│   ├── _dist.py            # shared distributed env setup and round reduction
│   ├── _llama_tp.py        # shared LLaMA tensor-parallel plan
│   ├── model_staging.py    # lazy, locked, atomic GCS model weight staging
│   ├── pytorch/            # shared DCP library (_worker, layout, model, parallelize, planners, reference, state, read/, write/)
│   ├── pytorch_save/       # runnable DCP save group
│   ├── pytorch_load/       # runnable same-layout DCP load group
│   ├── pytorch_reshard/    # runnable cross-layout DCP reshard group
│   ├── pytorch_lightning/  # also common.py, _worker.py
│   └── ray_pytorch/        # also common.py, _worker.py
└── tests/
```

Each runnable group directory has `configs.yaml`, `configs.py`,
`requirements.txt`, and benchmark tests (`pytorch_{save,load,reshard}` share
`parameters.py` and `read/`/`write/` drivers under `checkpointing/pytorch/`).
