from dataclasses import asdict
from metrics import schema


def test_step_metrics_fieldnames():
    assert schema.fieldnames(schema.StepMetrics) == [
        "step", "step_duration", "step_end_time"
    ]


def test_write_duration_fieldnames():
    # Durations are derived (end_time - start_time), so no *_duration column.
    assert schema.fieldnames(schema.WriteDurationMetrics) == [
        "checkpoint_step", "checkpoint_location", "start_time", "end_time",
        "global_rank", "local_rank"
    ]


def test_restore_duration_fieldnames():
    # Restores are keyed by checkpoint_location (the loaded path); durations are
    # derived (end_time - start_time), so no *_duration or cached-flag column.
    assert schema.fieldnames(schema.RestoreDurationMetrics) == [
        "checkpoint_step", "checkpoint_location", "start_time", "end_time",
        "global_rank", "local_rank"
    ]


def test_data_loading_roundtrips_to_dict():
    row = schema.DataLoadingMetrics(
        run_id="r", epoch_idx=-1, accelerator_blocked_time=1.5,
        accelerator_blocked_percent=10.0)
    d = asdict(row)
    assert d["accelerator_blocked_time"] == 1.5
    assert d["accelerator_blocked_percent"] == 10.0
