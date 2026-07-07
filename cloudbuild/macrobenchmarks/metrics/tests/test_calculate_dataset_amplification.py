from metrics import calculate


def test_ratio_normalizes_by_samples_consumed_not_full_dataset():
    # size=1000 over 100 samples -> 10 bytes/sample. A run of 5 steps at
    # global_batch_size 8 consumes 40 samples, so a perfectly sharded single
    # pass would read 40*10 = 400 bytes. Actual read of 800 -> 2x amplification.
    ratio = calculate.dataset_read_amplification_ratio(
        dataset_read_bytes=800,
        dataset_size_bytes=1000,
        dataset_sample_count=100,
        executed_steps=5,
        global_batch_size=8,
    )
    assert ratio == 2.0


def test_ratio_independent_of_dataset_size_when_partial_pass():
    # Same run, but the dataset is 10x larger (only a tenth is consumed). The
    # naive read/du ratio would collapse to 0.2 and hide duplication; the
    # normalized ratio stays 2.0 because per-sample bytes are unchanged.
    ratio = calculate.dataset_read_amplification_ratio(
        dataset_read_bytes=800,
        dataset_size_bytes=10000,
        dataset_sample_count=1000,
        executed_steps=5,
        global_batch_size=8,
    )
    assert ratio == 2.0


def test_ratio_none_when_any_input_missing():
    for missing in (
        "dataset_read_bytes",
        "dataset_size_bytes",
        "dataset_sample_count",
        "executed_steps",
        "global_batch_size",
    ):
        kwargs = {
            "dataset_read_bytes": 800,
            "dataset_size_bytes": 1000,
            "dataset_sample_count": 100,
            "executed_steps": 5,
            "global_batch_size": 8,
        }
        kwargs[missing] = None
        assert calculate.dataset_read_amplification_ratio(**kwargs) is None, missing


def test_ratio_none_when_denominator_component_zero():
    # Zero sample count, zero steps, or zero batch size would divide by zero.
    for zeroed in ("dataset_sample_count", "executed_steps", "global_batch_size"):
        kwargs = {
            "dataset_read_bytes": 800,
            "dataset_size_bytes": 1000,
            "dataset_sample_count": 100,
            "executed_steps": 5,
            "global_batch_size": 8,
        }
        kwargs[zeroed] = 0
        assert calculate.dataset_read_amplification_ratio(**kwargs) is None, zeroed


def test_ratio_zero_when_no_bytes_read():
    # No egress is a valid measurement (ratio 0.0), not a missing one.
    ratio = calculate.dataset_read_amplification_ratio(
        dataset_read_bytes=0,
        dataset_size_bytes=1000,
        dataset_sample_count=100,
        executed_steps=5,
        global_batch_size=8,
    )
    assert ratio == 0.0


def test_executed_step_count_dedupes_steps_across_ranks():
    # Under DDP every rank emits a row per optimizer step; the run executed as
    # many distinct steps as there are unique step numbers, not rows.
    rows = [
        {"step": 0, "step_duration": 1.0},
        {"step": 1, "step_duration": 1.0},
        {"step": 0, "step_duration": 1.1},  # same step, second rank
        {"step": 1, "step_duration": 1.2},
        {"step": 2, "step_duration": 1.0},
    ]
    assert calculate.executed_step_count(rows) == 3


def test_executed_step_count_ignores_incomplete_rows():
    rows = [
        {"step": 0, "step_duration": 1.0},
        {"step": None, "step_duration": 1.0},
        {"step": 1, "step_duration": None},
        {"step": 2, "step_duration": 1.0},
    ]
    assert calculate.executed_step_count(rows) == 2


def test_executed_step_count_empty_is_zero():
    assert calculate.executed_step_count([]) == 0


def _dataset_system_rows(read_bytes=800, size_bytes=1000, sample_count=100):
    return [
        {
            "pod_name": "ds",
            "metric": "dataset_read_bytes",
            "peak": read_bytes,
            "mean": None,
        },
        {
            "pod_name": "ds",
            "metric": "dataset_size_bytes",
            "peak": size_bytes,
            "mean": None,
        },
        {
            "pod_name": "ds",
            "metric": "dataset_sample_count",
            "peak": sample_count,
            "mean": None,
        },
    ]


def test_build_summary_row_emits_dataset_read_amplification_ratio():
    # 5 executed steps * gbs 8 = 40 samples consumed; per-sample bytes = 1000/100
    # = 10, so a single sharded pass should read 400 bytes. Actual 800 -> 2.0.
    row = calculate.build_summary_row(
        run_id="r",
        workload_name="w",
        requirements="gcsfs==1.0",
        step_rows=[{"step": s, "step_duration": 1.0} for s in range(5)],
        write_rows=[],
        restore_rows=[],
        delete_rows=[],
        dl_rows=[],
        system_rows=_dataset_system_rows(),
        dimensions={"global_batch_size": 8},
    )
    assert row["dataset_read_amplification_ratio"] == 2.0


def test_build_summary_row_omits_ratio_without_global_batch_size():
    row = calculate.build_summary_row(
        run_id="r",
        workload_name="w",
        requirements="gcsfs==1.0",
        step_rows=[{"step": s, "step_duration": 1.0} for s in range(5)],
        write_rows=[],
        restore_rows=[],
        delete_rows=[],
        dl_rows=[],
        system_rows=_dataset_system_rows(),
        dimensions=None,  # no global_batch_size
    )
    assert "dataset_read_amplification_ratio" not in row


def test_build_summary_row_omits_ratio_without_sample_count():
    rows = [r for r in _dataset_system_rows() if r["metric"] != "dataset_sample_count"]
    row = calculate.build_summary_row(
        run_id="r",
        workload_name="w",
        requirements="gcsfs==1.0",
        step_rows=[{"step": s, "step_duration": 1.0} for s in range(5)],
        write_rows=[],
        restore_rows=[],
        delete_rows=[],
        dl_rows=[],
        system_rows=rows,
        dimensions={"global_batch_size": 8},
    )
    assert "dataset_read_amplification_ratio" not in row
