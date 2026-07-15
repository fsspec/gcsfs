from metrics import calculate, raw_store
from metrics.parsers import hf


def _row(rank, duration, path="gs://ds/parquet-dir"):
    return {"global_rank": rank, "duration": duration, "dataset_path": path}


def test_bottleneck_rank_duration():
    rows = [_row(0, 1.5), _row(1, 3.2), _row(2, 2.0)]
    m = calculate.calc_dataset_build_metrics(rows)
    assert m["dataset_build_time"] == 3.2


def test_no_rows_yield_no_keys():
    assert calculate.calc_dataset_build_metrics([]) == {}


def test_dataset_build_roundtrip_through_raw_store(tmp_path):
    # Workload log line -> parser -> raw CSV -> calculator, end to end.
    lines = [
        "Dataset Build : Rank : 0 : Duration : 1.500000 seconds : "
        "Path: gs://ds/parquet-dir",
        "Dataset Build : Rank : 1 : Duration : 3.200000 seconds : "
        "Path: gs://ds/parquet-dir",
    ]
    entries = [hf.LogEntry(timestamp=float(i), message=m) for i, m in enumerate(lines)]
    parsed = hf.parse_entries(entries, run_id="r", checkpoint_location="gs://b/ckpt")
    raw_store.write_raw_metrics(parsed, str(tmp_path))
    tables = raw_store.read_raw_metrics(str(tmp_path))
    m = calculate.calc_dataset_build_metrics(tables.dataset_build_rows)
    assert m["dataset_build_time"] == 3.2
