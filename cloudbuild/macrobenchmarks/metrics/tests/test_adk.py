import csv

import pytest
from metrics import calculate, raw_store, schema, summary_schema
from metrics.parsers import adk
from metrics.parsers.common import LogEntry


def _op(seq, op, latency_ms, size=0, ok="true"):
    return LogEntry(
        timestamp=1.0,
        message=(
            f"Artifact Op : Seq: {seq} : Op: {op} : "
            f"Latency: {latency_ms} ms : Bytes: {size} : Ok: {ok}"
        ),
    )


def _probe(op, requests):
    return LogEntry(
        timestamp=1.0,
        message=f"Artifact Probe : Op: {op} : Requests Per Op: {requests}",
    )


def _workload(
    cold="N/A",
    ops=10,
    checks=900,
    failures=0,
    start="2026-11-25T01:02:03Z",
    end="2026-11-25T01:04:06Z",
):
    return LogEntry(
        timestamp=2.0,
        message=(
            f"Artifact Workload : Start: {start} : End: {end} : Total: 123.456 s : "
            f"Cold First Op: {cold} ms : Sessions: 32 : Turns: 50 : "
            f"Ops: {ops} : "
            f"Untimed Read Bytes: 4000 : Checks: {checks} : Failures: {failures}"
        ),
    )


PROBES = {
    "save": 3.0,
    "load_latest": 4.333,
    "load_version": 2.0,
    "list_keys": 6.0,
    "list_versions": 1.0,
    "get_version": 1.0,
    "delete": 2.0,
}


def _entries(failures=0, ops=10):
    entries = [
        _op("main:1", "save", 10.0, size=2000),
        _op("main:2", "save", 30.0, size=6000),
        _op("main:3", "save", 50.0, size=0),  # 0-byte rewind tombstone
        _op("main:4", "save", 90.0, size=4000, ok="false"),
        _op("main:5", "load_latest", 20.0, size=5000),
        _op("main:6", "load_version", 40.0, size=3000),
        _op("main:7", "list_keys", 7.5),
        _op("main:8", "list_versions", 5.0),
        _op("main:9", "get_version", 6.0),
        _op("resume:1", "delete", 12.0),
        LogEntry(timestamp=1.5, message="unrelated launcher output"),
        LogEntry(timestamp=1.6, message="WARNING: Artifact experimental feature on"),
    ]
    entries += [_probe(op, requests) for op, requests in PROBES.items()]
    entries.append(_workload(cold="412.5", ops=ops, failures=failures))
    return entries


def test_parse_entries_reads_every_line_kind():
    parsed = adk.parse_entries(_entries())

    assert len(parsed.artifact_op_metrics) == 10
    first = parsed.artifact_op_metrics[0]
    assert (first.seq, first.op, first.latency_ms, first.bytes, first.ok) == (
        "main:1",
        "save",
        10.0,
        2000,
        1,
    )
    assert parsed.artifact_op_metrics[3].ok == 0
    assert {p.op: p.requests_per_op for p in parsed.artifact_probe_metrics} == PROBES
    (workload,) = parsed.artifact_workload_metrics
    assert (workload.window_start, workload.window_end) == (
        "2026-11-25T01:02:03Z",
        "2026-11-25T01:04:06Z",
    )
    assert workload.total_s == 123.456
    assert workload.cold_first_op_ms == 412.5
    assert (workload.sessions, workload.turns) == (32, 50)
    assert (workload.validation_checks, workload.validation_failures) == (900, 0)
    assert workload.ops == 10
    assert workload.untimed_read_bytes == 4000


def test_parse_entries_deduplicates_identical_and_rejects_conflicting_or_malformed():
    duplicated = _entries() + [
        _op("main:1", "save", 10.0, size=2000),
        _probe("save", 3.0),
        _workload(cold="412.5", ops=10, failures=0),
    ]
    parsed = adk.parse_entries(duplicated)
    assert len(parsed.artifact_op_metrics) == 10
    assert len(parsed.artifact_probe_metrics) == len(PROBES)
    assert len(parsed.artifact_workload_metrics) == 1

    with pytest.raises(adk.MetricEventError):
        adk.parse_entries([_op("main:1", "save", 10.0), _op("main:1", "save", 99.0)])

    with pytest.raises(adk.MetricEventError):
        adk.parse_entries([_workload(cold="412.5"), _workload(cold="999.0")])

    with pytest.raises(adk.MetricEventError):
        adk.parse_entries([LogEntry(timestamp=1.0, message="Artifact Op : truncated")])


def test_parse_entries_keeps_missing_cold_first_op_empty():
    (workload,) = adk.parse_entries([_workload()]).artifact_workload_metrics

    assert workload.cold_first_op_ms is None


def test_build_filter_matches_every_pod_of_the_release():
    filter_string = adk.build_filter(
        project="proj", run_id="buildid-1", start_time="t0", end_time="t1"
    )

    assert 'resource.labels.pod_name:"buildid-1-workload-"' in filter_string
    for prefix in adk.METRIC_PREFIXES:
        assert f'textPayload:"{prefix}"' in filter_string


def _write_raw(tmp_path, entries, system_rows=()):
    raw_dir = tmp_path / "raw"
    raw_store.write_artifact_metrics(adk.parse_entries(entries), str(raw_dir))
    raw_store.write_system_metrics(list(system_rows), str(raw_dir))
    return raw_dir


def _run_calculate(tmp_path, raw_dir, workload_name="adk-iterative-workspace"):
    out_file = tmp_path / "summary.csv"
    calculate.main(
        [
            "--run-id",
            "buildid-1",
            "--workload-name",
            workload_name,
            "--requirements",
            "gcsfs-under-test",
            "--in-dir",
            str(raw_dir),
            "--out-file",
            str(out_file),
            "--require-artifact-metrics",
            "--bucket-type",
            "zonal",
            "--zone",
            "us-central1-b",
            "--region",
            "us-central1",
            "--machine-type",
            "c4-standard-48",
            "--nodes",
            "1",
            "--ranks-per-node",
            "4",
            "--steps",
            "100",
            "--image",
            "python:3.12",
        ]
    )
    with open(out_file, newline="") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        return reader.fieldnames, rows


def test_calculate_writes_one_adk_row_in_the_shared_schema(tmp_path):
    system_rows = [
        schema.SystemMetric(
            pod_name="ckpt-bucket", metric="checkpoint_read_bytes", peak=16000
        ),
        schema.SystemMetric(
            pod_name="ckpt-bucket", metric="checkpoint_read_request_count", peak=40
        ),
        schema.SystemMetric(pod_name="pod-0", metric="cpu", peak=3.5, mean=1.25),
    ]
    raw_dir = _write_raw(tmp_path, _entries(), system_rows)

    fieldnames, (row,) = _run_calculate(tmp_path, raw_dir)

    assert fieldnames == summary_schema.fieldnames()
    assert row["workload_name"] == "adk-iterative-workspace"
    assert (row["bucket_type"], row["zone"], row["nodes"]) == (
        "zonal",
        "us-central1-b",
        "1",
    )
    # Failed calls are excluded from latency stats, and 0-byte tombstones still
    # count as save calls while being excluded from save byte-rate throughput.
    assert row["artifact_save_count"] == "3"
    assert float(row["artifact_save_latency_mean_ms"]) == 30.0
    assert float(row["artifact_save_latency_p50_ms"]) == 30.0
    assert float(row["artifact_save_requests_per_op"]) == 3.0
    assert float(row["artifact_load_latest_requests_per_op"]) == 4.333
    assert float(row["artifact_get_version_latency_mean_ms"]) == 6.0
    assert float(row["artifact_get_version_requests_per_op"]) == 1.0
    assert float(row["artifact_list_keys_requests_per_op"]) == 6.0
    assert float(row["artifact_save_throughput_bytes_per_sec"]) == 8000 / 0.04
    assert row["artifact_logical_read_bytes"] == "8000"
    assert row["artifact_gcs_read_bytes"] == "16000"
    assert row["artifact_gcs_read_request_count"] == "40"
    # 16000 bucket bytes / (8000 timed + 4000 untimed payload bytes).
    assert float(row["artifact_read_amplification_ratio"]) == pytest.approx(
        16000 / 12000
    )
    assert row["artifact_untimed_read_bytes"] == "4000"
    assert float(row["artifact_cold_first_op_ms"]) == 412.5
    assert (row["sessions"], row["turns"]) == ("32", "50")
    assert row["validation_failures"] == "0"
    assert float(row["cpu_usage_peak_cores"]) == 3.5
    # The artifact bucket's traffic is not reported as checkpoint traffic, and
    # training-only columns stay empty.
    for column in (
        "checkpoint_read_bytes",
        "ranks_per_node",
        "steps",
        "mean_step_time",
    ):
        assert row[column] == "N/A", column


def test_artifact_row_keys_are_schema_columns():
    parsed = adk.parse_entries(_entries())
    row = calculate.build_artifact_summary_row(
        run_id="r",
        workload_name="adk-genmedia-pipeline",
        requirements="gcsfs",
        op_rows=[vars(m) for m in parsed.artifact_op_metrics],
        probe_rows=[vars(m) for m in parsed.artifact_probe_metrics],
        workload_rows=[vars(m) for m in parsed.artifact_workload_metrics],
        system_rows=[],
        dimensions={"bucket_type": "regional"},
    )

    assert set(row) <= set(summary_schema.fieldnames())
    for op in calculate.ARTIFACT_OPS:
        assert f"artifact_{op}_latency_p95_ms" in row


def test_calculate_rejects_a_run_with_validation_failures(tmp_path):
    raw_dir = _write_raw(tmp_path, _entries(failures=2))

    with pytest.raises(SystemExit):
        _run_calculate(tmp_path, raw_dir)


def test_calculate_rejects_a_run_without_probe_results(tmp_path):
    entries = [e for e in _entries() if "Probe" not in e.message]
    raw_dir = _write_raw(tmp_path, entries)

    with pytest.raises(SystemExit):
        _run_calculate(tmp_path, raw_dir)


def test_calculate_waits_for_every_op_line(tmp_path):
    # The Workload line says 11 ops were printed, but only 10 were scraped.
    raw_dir = _write_raw(tmp_path, _entries(ops=11))

    with pytest.raises(SystemExit):
        _run_calculate(tmp_path, raw_dir)


def test_calculate_rejects_non_contiguous_op_sequences(tmp_path):
    # main:10 skips main:9 while total count still matches ops=9.
    entries = [
        _op("main:1", "save", 10.0, size=2000),
        _op("main:3", "load_latest", 20.0, size=5000),
    ]
    entries += [_probe(op, requests) for op, requests in PROBES.items()]
    entries.append(_workload(ops=2))
    raw_dir = _write_raw(tmp_path, entries)

    with pytest.raises(SystemExit):
        _run_calculate(tmp_path, raw_dir)


def test_calculate_rejects_duplicated_workload_lines(tmp_path):
    raw_dir = tmp_path / "raw"
    parsed = adk.parse_entries(_entries())
    parsed.artifact_workload_metrics.append(parsed.artifact_workload_metrics[0])
    raw_store.write_artifact_metrics(parsed, str(raw_dir))
    raw_store.write_system_metrics([], str(raw_dir))

    with pytest.raises(SystemExit):
        _run_calculate(tmp_path, raw_dir)


def test_calculate_writes_na_for_missing_cold_op_and_rejects_unknown_ops_or_null_failures(
    tmp_path,
):
    entries = _entries()
    entries[-1] = _workload(cold="N/A", ops=10, failures=0)
    raw_dir = _write_raw(tmp_path, entries)
    _, (row,) = _run_calculate(tmp_path, raw_dir)
    assert row["artifact_cold_first_op_ms"] == "N/A"

    with pytest.raises(SystemExit):
        calculate.validate_required_artifact_metrics(
            op_rows=[
                vars(m) for m in adk.parse_entries(_entries()).artifact_op_metrics
            ],
            probe_rows=[
                vars(m) for m in adk.parse_entries(_entries()).artifact_probe_metrics
            ],
            workload_rows=[{"validation_failures": None, "ops": 10}],
        )

    bad_entries = [_op("main:1", "renamed_save", 10.0, size=2000)]
    bad_entries += [_probe(op, requests) for op, requests in PROBES.items()]
    bad_entries.append(_workload(ops=1))
    bad_dir = _write_raw(tmp_path / "bad", bad_entries)
    with pytest.raises(SystemExit):
        _run_calculate(tmp_path / "bad", bad_dir)
