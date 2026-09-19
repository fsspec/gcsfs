# Microbenchmarks & Subsystembenchmarks Config for `autoresearch`

`autoresearch` requires a `Verify` command whose **last non-empty stdout line is the single numeric metric** and a `Guard` command that exits `0` only if correctness tests pass.

> **Referee Rule**: Never modify `gcsfs/tests/**` (including `configs.yaml`, `rounds`, or `runtime`) inside an optimization loop. `Scope` must only include production code (`gcsfs/*.py` or target library source).

---

## 1. Microbenchmarks (`gcsfs/tests/perf/microbenchmarks`)

### Run Command

```bash
GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud/application_default_credentials.json \
GCSFS_TEST_PROJECT="$PROJECT" GOOGLE_CLOUD_PROJECT="$PROJECT" PYTHONPATH=. \
python gcsfs/tests/perf/microbenchmarks/run.py \
    --group=<group> --config=<scenario_name> \
    --regional-bucket="$REGIONAL_BUCKET"
```

- **Groups**: `cat`, `cat_ranges`, `comparison`, `delete`, `glob`, `info`, `listing`, `open`, `pipe`, `put`, `read`, `rename`, `write`.
- **Output**: `gcsfs/tests/perf/microbenchmarks/__run__/<DDMMYYYY-HHMMSS>/results.csv` (sort by `os.path.getmtime`, never lexically).

### Supported Perf Metrics & `Verify` Extractors

Chain the benchmark run and the extractor with `&&`:

| Perf Metric | CSV Columns / Calculation | `Direction` |
| :--- | :--- | :--- |
| **Throughput (MiB/s)** — fixed-duration (`read`, `write`) | `float(r['mean']) / float(r['runtime']) / 1048576` (`mean` is bytes) | `higher_is_better` |
| **Throughput (MiB/s)** — fixed-work (`cat`, `put`, `pipe`, …) | `float(r['total_bytes']) / float(r['mean']) / 1048576` (`mean` is seconds) | `higher_is_better` |
| **Latency (seconds)** — fixed-work groups | `float(r['mean'])` (or `median` / `min`) | `lower_is_better` |
| **Peak Memory (MiB)** | `float(r['mem_max'])` (or `mprof peak` output) | `lower_is_better` |
| **Peak CPU (%)** | `float(r['cpu_max_global'])` | `lower_is_better` |
| **GCS Requests per Op** | `float(r['requests_per_op'])` (`requests_download`, `requests_list`) | `lower_is_better` |

**Universal Microbenchmark `Verify` Extractor Tail:**

```bash
python3 -c "
import csv, glob, os, sys
d = max(glob.glob('gcsfs/tests/perf/microbenchmarks/__run__/*/'), key=os.path.getmtime)
r = next(x for x in csv.DictReader(open(os.path.join(d, 'results.csv'))) if sys.argv[1] in x['name'])
col = sys.argv[2]
if col == 'throughput_fixed_duration_mibs':
    print(float(r['mean']) / float(r['runtime']) / 1048576)
elif col == 'throughput_fixed_work_mibs':
    print(float(r['total_bytes']) / float(r['mean']) / 1048576)
else:
    print(float(r[col]))
" <test_name_substring> <column_or_mode>
```

---

## 2. Subsystembenchmarks (`gcsfs/tests/perf/subsystembenchmarks`)

### Run Command

Use `--sweep-axes=baseline` and `--amplification-wait=0 --amplification-retry-wait=0` inside the loop to avoid the 300s Cloud Monitoring sleep unless optimizing amplification metrics:

```bash
python -m gcsfs.tests.perf.subsystembenchmarks.run \
  --group=<group> \
  --sweep-axes=baseline \
  --bucket-prefix="$LOWERCASE_PREFIX" \
  --project="$PROJECT" --location=us-central1 --bucket-type=regional \
  --amplification-wait=0 --amplification-retry-wait=0
```

- **Groups**: `dataloading/huggingface_datasets`, `dataloading/ray_data`, `dataloading/webdataset`, `checkpointing/pytorch_lightning`.
- **Output**: `gcsfs/tests/perf/subsystembenchmarks/__run__/<YYYYMMDD-HHMMSS>/results.csv`.

### Supported Perf Metrics & `Verify` Extractor

| Perf Metric | CSV Column (`<column_name>`) | `Direction` |
| :--- | :--- | :--- |
| **Dataloading Read Throughput** | `dataset_read_throughput_mean_bytes_per_second` | `higher_is_better` |
| **Sample Rate** | `mean_samples_per_second` | `higher_is_better` |
| **Checkpoint Write / Read Throughput** | `checkpoint_write_throughput_mean_bytes_per_second` / `checkpoint_read_throughput_mean_bytes_per_second` | `higher_is_better` |
| **Time to First Batch / Build Time** | `time_to_first_batch_seconds` / `dataset_build_time` | `lower_is_better` |
| **Round Duration** | `round_duration_mean_seconds` (or `round_duration_p50_seconds`) | `lower_is_better` |
| **Peak Memory / Peak CPU** | `memory_usage_peak_bytes` / `cpu_usage_peak_cores` | `lower_is_better` |
| **Read Amplification / Request Count** | `dataset_read_amplification_ratio` / `dataset_read_request_count` (requires removing `--amplification-wait=0`) | `lower_is_better` |

**Universal Subsystembenchmark `Verify` Extractor Tail:**

```bash
python3 -c "
import csv, glob, os, sys
d = max(glob.glob('gcsfs/tests/perf/subsystembenchmarks/__run__/*/'), key=os.path.getmtime)
r = next(x for x in csv.DictReader(open(os.path.join(d, 'results.csv'))) if x['config_sweep_axis'] == 'baseline')
print(float(r[sys.argv[1]]))
" <column_name>
```

---

## 3. Correctness `Guard` Commands

Every `autoresearch` iteration must validate correctness before keeping a metric improvement:

```bash
# Fast unit & emulator tests (requires fake-gcs-server on :4443)
STORAGE_EMULATOR_HOST=http://localhost:4443 \
GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/gcsfs/tests/fake-service-account-credentials.json \
pytest -q -n 8 gcsfs/ -k "not test_get_control_plane_client_endpoint"

# Benchmark harness self-tests (no live GCS cost)
pytest -q gcsfs/tests/perf/microbenchmarks --run-benchmarks-infra
pytest -q gcsfs/tests/perf/subsystembenchmarks --run-benchmarks-infra
```
