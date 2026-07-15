import pytest
from metrics import calculate, raw_store
from metrics.parsers import hf

SETUP = "setup_train_dataloader"
FETCH = "[_TrainingEpochLoop].train_dataloader_next"


def _row(rank, idx, action, duration, total):
    return {
        "global_rank": rank,
        "fetch_index": idx,
        "action": action,
        "duration": duration,
        "cumulative_total": total,
    }


def test_bottleneck_rank_total_and_split():
    rows = [
        _row(0, 1, SETUP, 1.0, 1.0),
        _row(0, 2, FETCH, 0.5, 1.5),
        _row(1, 1, SETUP, 2.0, 2.0),
        _row(1, 2, FETCH, 0.5, 2.5),
        _row(1, 3, FETCH, 0.5, 3.0),
    ]
    m = calculate.calc_data_wait_metrics(rows)
    # rank 1 has the greater running total, so all fields come from rank 1.
    assert m["data_wait_total_time"] == 3.0
    assert m["data_wait_iterator_setup_time"] == 2.0
    assert m["data_wait_batch_fetch_time"] == 1.0
    assert m["num_data_wait_spans"] == 3


def test_headline_survives_lost_middle_lines():
    # The line for fetch 2 was lost to logging lag: the running total on the
    # last surviving line still carries the full blocked time, while the
    # setup/fetch split (summed durations) undercounts.
    rows = [
        _row(0, 1, SETUP, 2.0, 2.0),
        _row(0, 3, FETCH, 1.0, 4.0),
    ]
    m = calculate.calc_data_wait_metrics(rows)
    assert m["data_wait_total_time"] == 4.0
    assert m["data_wait_iterator_setup_time"] == 2.0
    assert m["data_wait_batch_fetch_time"] == 1.0


def test_no_rows_yield_no_keys():
    assert calculate.calc_data_wait_metrics([]) == {}


def test_validation_passes_with_both_span_kinds():
    calculate.validate_required_metrics(
        step_rows=[],
        write_rows=[],
        data_wait_rows=[
            _row(0, 1, SETUP, 1.0, 1.0),
            _row(0, 2, FETCH, 0.5, 1.5),
        ],
        require_data_wait=True,
    )


def test_validation_rejects_fetch_only_spans():
    # A fetch-only run means the image was built with stock lightning (no
    # setup_train_dataloader action), silently missing worker-spawn spans.
    with pytest.raises(SystemExit):
        calculate.validate_required_metrics(
            step_rows=[],
            write_rows=[],
            data_wait_rows=[_row(0, 1, FETCH, 0.5, 0.5)],
            require_data_wait=True,
        )


def test_validation_rejects_absent_spans():
    with pytest.raises(SystemExit):
        calculate.validate_required_metrics(
            step_rows=[],
            write_rows=[],
            data_wait_rows=[],
            require_data_wait=True,
        )


def test_data_wait_roundtrip_through_raw_store(tmp_path):
    # Workload log line -> parser -> raw CSV -> calculator, end to end.
    lines = [
        "Data Wait : Rank : 0 : Fetch : 1 : Action : setup_train_dataloader : "
        "Duration : 1.000000 seconds : Total : 1.000000 seconds",
        "Data Wait : Rank : 0 : Fetch : 2 : Action : "
        "[_TrainingEpochLoop].train_dataloader_next : "
        "Duration : 0.250000 seconds : Total : 1.250000 seconds",
    ]
    entries = [hf.LogEntry(timestamp=float(i), message=m) for i, m in enumerate(lines)]
    parsed = hf.parse_entries(entries, run_id="r", checkpoint_location="gs://b/ckpt")
    raw_store.write_raw_metrics(parsed, str(tmp_path))
    tables = raw_store.read_raw_metrics(str(tmp_path))
    m = calculate.calc_data_wait_metrics(tables.data_wait_rows)
    assert m["data_wait_total_time"] == 1.25
    assert m["data_wait_iterator_setup_time"] == 1.0
    assert m["data_wait_batch_fetch_time"] == 0.25
    assert m["num_data_wait_spans"] == 2
