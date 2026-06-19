from dataclasses import asdict

from metrics import schema


def test_data_loading_roundtrips_to_dict():
    row = schema.DataLoadingMetrics(
        run_id="r",
        epoch_idx=-1,
        accelerator_blocked_time=1.5,
        accelerator_blocked_percent=10.0,
    )
    d = asdict(row)
    assert d["accelerator_blocked_time"] == 1.5
    assert d["accelerator_blocked_percent"] == 10.0
