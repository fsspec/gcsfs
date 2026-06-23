import csv
import sys
import types

from metrics import raw_store, schema
from metrics.parsers import hf


def test_build_filter_shape():
    f = hf.build_filter(
        project="p",
        run_id="buildid-sample",
        start_time="2026-06-17T00:00:00Z",
        end_time="2026-06-17T01:00:00Z",
    )
    assert 'resource.type="k8s_container"' in f
    assert 'resource.labels.project_id="p"' in f
    assert 'resource.labels.pod_name:"buildid-sample-workload-0-"' in f
    assert 'resource.labels.pod_name:"buildid-sample-workload-0-0"' not in f
    assert "severity>=DEFAULT" in f
    assert 'timestamp>="2026-06-17T00:00:00Z"' in f
    assert 'timestamp<="2026-06-17T01:00:00Z"' in f
    # all 7 regexes OR'd into the filter
    for pat in hf.ALL_PATTERNS:
        assert pat in f


def test_cloud_logging_entries_requested_in_timestamp_order(monkeypatch, tmp_path):
    calls = []

    class FakeClient:
        def __init__(self, project):
            self.project = project

        def list_entries(self, **kwargs):
            calls.append(kwargs)
            return []

    fake_logging = types.SimpleNamespace(Client=FakeClient)
    monkeypatch.setitem(
        sys.modules,
        "google",
        types.SimpleNamespace(cloud=types.SimpleNamespace(logging=fake_logging)),
    )
    monkeypatch.setitem(
        sys.modules, "google.cloud", types.SimpleNamespace(logging=fake_logging)
    )

    hf.main(
        [
            "--run-id",
            "r",
            "--project",
            "p",
            "--start-time",
            "2026-06-17T00:00:00Z",
            "--end-time",
            "2026-06-17T01:00:00Z",
            "--checkpoint-location",
            "gs://b/ckpt",
            "--out-dir",
            str(tmp_path),
        ]
    )

    assert calls
    assert calls[0]["order_by"] == "timestamp asc"


def test_write_raw_csvs_layout(tmp_path):
    parsed = hf.ParsedRawMetrics()
    parsed.step_metrics.append(
        schema.StepMetrics(step=1, step_duration=1.0, step_end_time=10.0)
    )
    parsed.write_metrics[0].append(
        schema.WriteDurationMetrics(
            checkpoint_step=25,
            checkpoint_location="gs://b/ckpt",
            start_time=1.0,
            end_time=2.0,
            global_rank=0,
        )
    )
    parsed.delete_metrics[0].append(
        schema.DeleteDurationMetrics(
            checkpoint_step=50,
            checkpoint_location="gs://b/ckpt",
            start_time=1.0,
            end_time=2.0,
            global_rank=0,
        )
    )
    parsed.data_loading_metrics.append(
        schema.DataLoadingMetrics(
            run_id="r",
            epoch_idx=-1,
            accelerator_blocked_time=5.0,
            accelerator_blocked_percent=10.0,
        )
    )

    raw_store.write_raw_metrics(parsed, str(tmp_path), run_type="perf_optimization")

    step_csv = tmp_path / "training_time" / "step_time.csv"
    assert step_csv.exists()
    with open(step_csv) as fh:
        rows = list(csv.DictReader(fh))
    assert rows[0]["step"] == "1"
    assert rows[0]["step_duration"] == "1.0"

    assert (
        tmp_path
        / "checkpoint_write_time"
        / "persistent_storage"
        / "per_accelerator"
        / "0.csv"
    ).exists()
    assert (
        tmp_path / "perf_optimization" / "checkpoint_delete_time" / "0.csv"
    ).exists()
    assert (tmp_path / "calculated_metrics" / "data_loading_metrics.csv").exists()


def _restore_lines(rank, start, duration, end, path="gs://b/ckpt/s25.ckpt"):
    return [
        f"Checkpoint Restore Start : Rank : {rank} : Start time: {start} "
        f"seconds : Path: {path}",
        f"Finished restoring checkpoint : Rank : {rank} : Duration: "
        f"{duration} seconds : End Time: {end} seconds : Path: {path}",
    ]


def test_single_resume_across_ranks_parses_correctly(tmp_path):
    # Verify multiple ranks' restore log lines are parsed and stored as separate rows.
    lines = _restore_lines(0, "10.0", "8.00", "18.00") + _restore_lines(
        1, "11.0", "9.00", "20.00"
    )
    entries = [hf.LogEntry(timestamp=float(i), message=m) for i, m in enumerate(lines)]
    parsed = hf.parse_entries(entries, run_id="r", checkpoint_location="gs://b/ckpt")
    raw_store.write_raw_metrics(parsed, str(tmp_path))

    restore_rows = raw_store.read_raw_metrics(str(tmp_path)).restore_rows
    assert len(restore_rows) == 2
    restore_rows.sort(key=lambda r: r["global_rank"])

    assert restore_rows[0]["checkpoint_step"] == 0
    assert restore_rows[0]["checkpoint_location"] == "gs://b/ckpt/s25.ckpt"
    assert restore_rows[0]["start_time"] == 10.0
    assert restore_rows[0]["end_time"] == 18.0
    assert restore_rows[0]["global_rank"] == 0

    assert restore_rows[1]["checkpoint_step"] == 0
    assert restore_rows[1]["checkpoint_location"] == "gs://b/ckpt/s25.ckpt"
    assert restore_rows[1]["start_time"] == 11.0
    assert restore_rows[1]["end_time"] == 20.0
    assert restore_rows[1]["global_rank"] == 1


def test_distinct_restore_paths_survive_csv_roundtrip(tmp_path):
    # Two restores of different checkpoints must round-trip through the CSV store.
    parsed = hf.ParsedRawMetrics()
    parsed.restore_metrics[0].append(
        schema.RestoreDurationMetrics(
            checkpoint_step=0,
            checkpoint_location="gs://b/ckpt/a.ckpt",
            start_time=0.0,
            end_time=5.0,
            global_rank=0,
        )
    )
    parsed.restore_metrics[0].append(
        schema.RestoreDurationMetrics(
            checkpoint_step=0,
            checkpoint_location="gs://b/ckpt/b.ckpt",
            start_time=10.0,
            end_time=18.0,
            global_rank=0,
        )
    )

    raw_store.write_raw_metrics(parsed, str(tmp_path))

    restore_rows = raw_store.read_raw_metrics(str(tmp_path)).restore_rows
    assert len(restore_rows) == 2
    restore_rows.sort(key=lambda r: r["checkpoint_location"])

    assert restore_rows[0]["checkpoint_location"] == "gs://b/ckpt/a.ckpt"
    assert restore_rows[0]["start_time"] == 0.0
    assert restore_rows[0]["end_time"] == 5.0

    assert restore_rows[1]["checkpoint_location"] == "gs://b/ckpt/b.ckpt"
    assert restore_rows[1]["start_time"] == 10.0
    assert restore_rows[1]["end_time"] == 18.0


def test_distinct_restore_events_are_separate_datapoints(tmp_path):
    lines = _restore_lines(
        0, "10.0", "5.00", "15.0", path="gs://b/ckpt/a.ckpt"
    ) + _restore_lines(0, "100.0", "6.00", "106.0", path="gs://b/ckpt/b.ckpt")
    entries = [hf.LogEntry(timestamp=float(i), message=m) for i, m in enumerate(lines)]
    parsed = hf.parse_entries(entries, run_id="r", checkpoint_location="gs://b/ckpt")
    raw_store.write_raw_metrics(parsed, str(tmp_path))

    restore_rows = raw_store.read_raw_metrics(str(tmp_path)).restore_rows
    assert len(restore_rows) == 2
    restore_rows.sort(key=lambda r: r["checkpoint_location"])

    assert restore_rows[0]["checkpoint_location"] == "gs://b/ckpt/a.ckpt"
    assert restore_rows[0]["start_time"] == 10.0
    assert restore_rows[0]["end_time"] == 15.0

    assert restore_rows[1]["checkpoint_location"] == "gs://b/ckpt/b.ckpt"
    assert restore_rows[1]["start_time"] == 100.0
    assert restore_rows[1]["end_time"] == 106.0


def test_delete_metric_end_to_end(tmp_path):
    entries = [
        hf.LogEntry(
            timestamp=100.0,
            message="Finished deleting checkpoint gs://b/ckpt/old.ckpt in 3.00 "
            "seconds for global_step 50 from rank 0",
        )
    ]
    parsed = hf.parse_entries(entries, run_id="r", checkpoint_location="gs://b/ckpt")
    raw_store.write_raw_metrics(parsed, str(tmp_path))
    delete_rows = raw_store.read_raw_metrics(str(tmp_path)).delete_rows
    assert len(delete_rows) == 1
    assert delete_rows[0]["checkpoint_location"] == "gs://b/ckpt"
    assert delete_rows[0]["checkpoint_step"] == 50
    assert delete_rows[0]["end_time"] - delete_rows[0]["start_time"] == 3.0
