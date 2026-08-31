import pytest
from metrics.parsers import ray_train

RUN_ID = "run-1"
CHECKPOINT_ROOT = "gs://bucket/checkpoints"


def parse(entries):
    return ray_train.parse_entries(
        entries,
        run_id=RUN_ID,
        checkpoint_location=CHECKPOINT_ROOT,
    )


def test_parse_all_lightning_style_ray_metric_records():
    checkpoint = f"{CHECKPOINT_ROOT}/step-25.ckpt"
    parsed = parse(
        [
            ray_train.LogEntry(
                timestamp=12.5,
                message=(
                    "Global Rank: 0 | Step: 25 | Loss: 1.2500 | "
                    "Step Time: 1.5s | Throughput: 42 samples/s"
                ),
            ),
            ray_train.LogEntry(
                timestamp=20.0,
                message=(
                    "Dataset Build : Rank : 0 : Duration : 3.25 seconds : "
                    "Path: gs://bucket/dataset"
                ),
            ),
            ray_train.LogEntry(
                timestamp=26.0,
                message=(
                    "Checkpoint Save : Rank: 0 : Step: 25 : "
                    f"Start time: 26.0 seconds: Path: {checkpoint}"
                ),
            ),
            ray_train.LogEntry(
                timestamp=30.0,
                message=(
                    f"Finished saving checkpoint to {checkpoint} in 4.0 seconds "
                    "for global_step 25 from rank 0"
                ),
            ),
            ray_train.LogEntry(
                timestamp=30.0,
                message=(
                    "Checkpoint Size : Rank : 0 : Step : 25 : Bytes : 1234 : "
                    f"Path: {checkpoint}"
                ),
            ),
            ray_train.LogEntry(
                timestamp=50.0,
                message=(
                    "Checkpoint Restore Start : Rank : 0 : "
                    f"Start time: 50.0 seconds : Path: {checkpoint}"
                ),
            ),
            ray_train.LogEntry(
                timestamp=57.5,
                message=(
                    "Finished restoring checkpoint : Rank : 0 : "
                    "Duration: 7.5 seconds : End Time: 57.5 seconds : "
                    f"Path: {checkpoint}"
                ),
            ),
            ray_train.LogEntry(
                timestamp=80.0,
                message=(
                    f"Finished deleting checkpoint {checkpoint} in 2.0 seconds "
                    "for global_step 25 from rank 0"
                ),
            ),
            ray_train.LogEntry(
                timestamp=81.0,
                message=(
                    "Ray Data Iteration : Rank : 0 : Split : 0 : "
                    "Blocked : 10.0 seconds : Total : 20.0 seconds : "
                    "First Batch : 3.0 seconds : Blocked Calls : 5"
                ),
            ),
        ]
    )

    assert parsed.dataset_build_metrics[0].duration == 3.25
    assert parsed.write_metrics[0][0].start_time == 26.0
    assert parsed.write_metrics[0][0].end_time == 30.0
    assert parsed.checkpoint_sizes[0].size_bytes == 1234
    assert parsed.restore_metrics[0][0].start_time == 50.0
    assert parsed.restore_metrics[0][0].end_time == 57.5
    assert parsed.delete_metrics[0][0].start_time == 78.0
    assert parsed.delete_metrics[0][0].end_time == 80.0
    assert parsed.ray_data_iteration_metrics[0].blocked_calls == 5


def test_sharded_commit_preserves_every_rank_for_each_step():
    checkpoint = f"{CHECKPOINT_ROOT}/step-25.ckpt"
    durations = [2.0, 7.0, 3.0]
    starts = [28.0, 34.0, 52.0]
    entries = []
    for rank, (start, duration) in enumerate(zip(starts, durations)):
        entries.extend(
            [
                ray_train.LogEntry(
                    timestamp=start,
                    message=(
                        f"Checkpoint Save : Rank: {rank} : Step: 25 : "
                        f"Start time: {start} seconds: Path: {checkpoint}"
                    ),
                ),
                ray_train.LogEntry(
                    timestamp=start + duration,
                    message=(
                        f"Finished saving checkpoint to {checkpoint} in {duration} "
                        f"seconds for global_step 25 from rank {rank}"
                    ),
                ),
            ]
        )
    rows = parse(entries)

    assert set(rows.write_metrics) == {0, 1, 2}
    written = [rows.write_metrics[rank][0] for rank in range(3)]
    assert [row.global_rank for row in written] == [0, 1, 2]
    assert [row.end_time - row.start_time for row in written] == durations
    assert [row.end_time for row in written] == [30.0, 41.0, 55.0]


@pytest.mark.parametrize(
    "record_order",
    [
        ("start", "end", "end"),
        ("end", "start"),
    ],
)
def test_checkpoint_halves_are_order_independent_and_idempotent(record_order):
    checkpoint = f"{CHECKPOINT_ROOT}/step-25.ckpt"
    records = {
        "start": ray_train.LogEntry(
            timestamp=26.0,
            message=(
                "Checkpoint Save : Rank: 0 : Step: 25 : "
                f"Start time: 26.0 seconds: Path: {checkpoint}"
            ),
        ),
        "end": ray_train.LogEntry(
            timestamp=30.0,
            message=(
                f"Finished saving checkpoint to {checkpoint} in 4.0 seconds "
                "for global_step 25 from rank 0"
            ),
        ),
    }

    parsed = parse([records[kind] for kind in record_order])

    assert len(parsed.write_metrics[0]) == 1
    assert parsed.write_metrics[0][0].start_time == 26.0
    assert parsed.write_metrics[0][0].end_time == 30.0


def test_conflicting_duplicate_checkpoint_half_fails():
    checkpoint = f"{CHECKPOINT_ROOT}/step-25.ckpt"
    with pytest.raises(ray_train.MetricEventError, match="conflicting checkpoint"):
        parse(
            [
                ray_train.LogEntry(
                    timestamp=26.0,
                    message=(
                        "Checkpoint Save : Rank: 0 : Step: 25 : "
                        f"Start time: 26.0 seconds: Path: {checkpoint}"
                    ),
                ),
                ray_train.LogEntry(
                    timestamp=27.0,
                    message=(
                        "Checkpoint Save : Rank: 0 : Step: 25 : "
                        f"Start time: 27.0 seconds: Path: {checkpoint}"
                    ),
                ),
            ]
        )


def test_identical_duplicate_event_is_idempotent():
    item = ray_train.LogEntry(
        timestamp=10.0,
        message=(
            "Global Rank: 0 | Step: 1 | Loss: 1.0000 | "
            "Step Time: 1.0s | Throughput: 2.0 samples/s"
        ),
    )
    parsed = parse([item, item])
    assert len(parsed.step_metrics) == 1


def test_conflicting_duplicate_event_fails():
    with pytest.raises(ray_train.MetricEventError, match="conflicting duplicate"):
        parse(
            [
                ray_train.LogEntry(
                    timestamp=10.0,
                    message=(
                        "Global Rank: 0 | Step: 1 | Loss: 1.0000 | "
                        "Step Time: 1.0s | Throughput: 2.0 samples/s"
                    ),
                ),
                ray_train.LogEntry(
                    timestamp=11.0,
                    message=(
                        "Global Rank: 0 | Step: 1 | Loss: 1.0000 | "
                        "Step Time: 2.0s | Throughput: 2.0 samples/s"
                    ),
                ),
            ]
        )


@pytest.mark.parametrize(
    "message",
    [
        "Global Rank: x | Step: 1 | Loss: 1.0 | Step Time: 1.0s | Throughput: 2.0 samples/s",
        "Global Rank: 0 | Step: 1",
        (
            "Global Rank: 0 | Step: 1 | Loss: 1.0 | Step Time: 1.0s | "
            "Throughput: 2.0 samples/s trailing-junk"
        ),
        "unmarked",
    ],
)
def test_malformed_metric_records_fail(message):
    with pytest.raises(ray_train.MetricEventError, match="unrecognized"):
        parse([ray_train.LogEntry(timestamp=0.0, message=message)])


@pytest.mark.parametrize(
    "entries,match",
    [
        (
            [
                ray_train.LogEntry(
                    timestamp=1.0,
                    message=(
                        "Checkpoint Restore Start : Rank : 0 : Start time: 1.0 "
                        "seconds : Path: gs://bucket/checkpoint"
                    ),
                )
            ],
            "unmatched restore start",
        ),
        (
            [
                ray_train.LogEntry(
                    timestamp=2.0,
                    message=(
                        "Finished restoring checkpoint : Rank : 0 : Duration: 1.0 "
                        "seconds : End Time: 2.0 seconds : Path: gs://bucket/checkpoint"
                    ),
                )
            ],
            "unmatched restore completion",
        ),
    ],
)
def test_unmatched_restore_halves_fail(entries, match):
    with pytest.raises(ray_train.MetricEventError, match=match):
        parse(entries)


@pytest.mark.parametrize("changed", ["global_rank", "checkpoint_location"])
def test_restore_pair_identity_must_match(changed):
    completion_rank = 1 if changed == "global_rank" else 0
    completion_path = (
        "gs://bucket/other"
        if changed == "checkpoint_location"
        else "gs://bucket/checkpoint"
    )
    with pytest.raises(ray_train.MetricEventError, match="unmatched restore"):
        parse(
            [
                ray_train.LogEntry(
                    timestamp=1.0,
                    message=(
                        "Checkpoint Restore Start : Rank : 0 : Start time: 1.0 "
                        "seconds : Path: gs://bucket/checkpoint"
                    ),
                ),
                ray_train.LogEntry(
                    timestamp=2.0,
                    message=(
                        "Finished restoring checkpoint : Rank : "
                        f"{completion_rank} : Duration: 1.0 seconds : End Time: "
                        f"2.0 seconds : Path: {completion_path}"
                    ),
                ),
            ]
        )


def test_build_filter_scopes_to_the_run_pods_and_time_window():
    filter_string = ray_train.build_filter(
        project="proj",
        run_id="run-1",
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-01T01:00:00Z",
    )
    assert 'resource.labels.project_id="proj"' in filter_string
    assert 'resource.labels.pod_name:"run-1-workload-0-"' in filter_string
    assert 'timestamp>="2026-01-01T00:00:00Z"' in filter_string
    assert 'timestamp<="2026-01-01T01:00:00Z"' in filter_string


def test_build_filter_selects_metric_prefixes_for_strict_parser_validation():
    filter_string = ray_train.build_filter(
        project="proj",
        run_id="run-1",
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-01T01:00:00Z",
    )

    for prefix in ray_train.METRIC_PREFIXES:
        assert f'textPayload:"{prefix}"' in filter_string
    assert "textPayload =~" not in filter_string
    assert "GCSFS_METRIC" not in filter_string


def test_checkpoint_events_must_sit_under_the_supplied_checkpoint_root():
    entries = [
        ray_train.LogEntry(
            timestamp=1.0,
            message=(
                "Checkpoint Size : Rank : 0 : Step : 1 : Bytes : 10 : "
                "Path: other-bucket/elsewhere/step-1.ckpt"
            ),
        )
    ]
    with pytest.raises(ray_train.MetricEventError, match="not under the run's"):
        parse(entries)


def test_restore_locations_are_exempt_from_the_checkpoint_root():
    """A warm start reads a seed checkpoint written outside the run's root."""
    seed = "bucket/seed/other-run/step-1.ckpt"
    parsed = parse(
        [
            ray_train.LogEntry(
                timestamp=1.0,
                message=(
                    "Checkpoint Restore Start : Rank : 0 : Start time: 1.0 "
                    f"seconds : Path: {seed}"
                ),
            ),
            ray_train.LogEntry(
                timestamp=2.0,
                message=(
                    "Finished restoring checkpoint : Rank : 0 : Duration: 1.0 "
                    f"seconds : End Time: 2.0 seconds : Path: {seed}"
                ),
            ),
        ]
    )
    assert parsed.restore_metrics[0][0].start_time == 1.0
