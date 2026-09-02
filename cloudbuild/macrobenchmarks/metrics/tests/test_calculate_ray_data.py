import pytest
from metrics import calculate


def ray_data_row(*, rank, split, blocked, total, setup, blocked_calls):
    return {
        "run_id": "run-1",
        "global_rank": rank,
        "split_index": split,
        "total_blocked_s": blocked,
        "total_s": total,
        "time_to_first_batch_s": setup,
        "blocked_calls": blocked_calls,
    }


def test_ray_data_reducer_carries_bottleneck_row_values_together():
    rows = [
        ray_data_row(
            rank=0, split=0, blocked=4.0, total=20.0, setup=1.0, blocked_calls=2
        ),
        ray_data_row(
            rank=1, split=1, blocked=10.0, total=25.0, setup=3.0, blocked_calls=5
        ),
    ]
    assert calculate.calc_ray_data_metrics(rows) == {
        "data_wait_total_time": 10.0,
        "data_wait_iterator_setup_time": 3.0,
        "data_wait_batch_fetch_time": 7.0,
        "accelerator_blocked_time": 10.0,
        "accelerator_blocked_percent": 40.0,
    }


def test_ray_data_reducer_leaves_the_lightning_span_count_alone():
    """Ray's per-batch fetch count is not the profiler-span count that column holds."""
    rows = [
        ray_data_row(
            rank=0, split=0, blocked=4.0, total=20.0, setup=1.0, blocked_calls=2
        )
    ]
    assert "num_data_wait_spans" not in calculate.calc_ray_data_metrics(rows)


def test_ray_data_reducer_accepts_zero_lifetime_when_unblocked():
    rows = [
        ray_data_row(
            rank=0, split=0, blocked=0.0, total=0.0, setup=0.0, blocked_calls=0
        )
    ]
    assert calculate.calc_ray_data_metrics(rows)["accelerator_blocked_percent"] == 0.0


@pytest.mark.parametrize(
    "overrides,match",
    [
        ({"total_blocked_s": 1.0, "total_s": 0.0}, "zero lifetime"),
        ({"total_blocked_s": 11.0, "total_s": 10.0}, "exceeds 100"),
        (
            {"total_blocked_s": 1.0, "time_to_first_batch_s": 2.0},
            "first-batch time exceeds blocked time",
        ),
        ({"total_blocked_s": float("nan")}, "finite nonnegative"),
        ({"time_to_first_batch_s": -1.0}, "finite nonnegative"),
        ({"blocked_calls": 1.5}, "blocked_calls"),
    ],
)
def test_ray_data_reducer_rejects_invalid_snapshots(overrides, match, capsys):
    row = ray_data_row(
        rank=0, split=0, blocked=1.0, total=10.0, setup=0.5, blocked_calls=1
    )
    row.update(overrides)
    with pytest.raises(SystemExit) as exc:
        calculate.calc_ray_data_metrics([row])
    assert exc.value.code == 1
    assert match in capsys.readouterr().err


def complete_metrics(*, model_parallel=False, resume=False):
    checkpoint = "gs://bucket/checkpoints/checkpoint_000004"
    rows = {
        "step_rows": [
            {
                "step": step,
                "step_duration": 1.0,
                "step_end_time": float(step),
            }
            for step in range(1, 5)
        ],
        "write_rows": [
            {
                "checkpoint_step": step,
                "checkpoint_location": checkpoint.replace("4", str(step)),
                "start_time": float(step),
                "end_time": float(step) + 1.0,
                "global_rank": 0,
            }
            for step in (2, 4)
        ],
        "restore_rows": [],
        "delete_rows": [
            {
                "checkpoint_step": 2,
                "checkpoint_location": checkpoint.replace("4", "2"),
                "start_time": 5.0,
                "end_time": 6.0,
                "global_rank": 0,
            }
        ],
        "size_rows": [
            {
                "checkpoint_step": step,
                "checkpoint_location": checkpoint.replace("4", str(step)),
                "size_bytes": 1000,
                "global_rank": 0,
            }
            for step in (2, 4)
        ],
        "dataset_build_rows": [
            {"global_rank": 0, "duration": 3.0, "dataset_path": "gs://dataset"}
        ],
        "ray_data_iteration_rows": [
            ray_data_row(
                rank=rank,
                split=split,
                blocked=2.0 + split,
                total=10.0,
                setup=1.0,
                blocked_calls=2,
            )
            for split, rank in enumerate((0, 2) if model_parallel else (0, 1))
        ],
        "expected_steps": 4,
        "nodes": 1,
        "ranks_per_node": 4 if model_parallel else 2,
        "checkpoint_interval": 2,
        "checkpoints_to_keep": 1,
        "training_strategy": "model_parallel_sharded" if model_parallel else "ddp",
        "data_parallel_size": 2 if model_parallel else None,
        "resume_run": resume,
    }
    if resume:
        rows["restore_rows"] = [
            {
                "checkpoint_step": 0,
                "checkpoint_location": "gs://bucket/seed",
                "start_time": 0.0,
                "end_time": 1.0,
                "global_rank": rank,
            }
            for rank in range(rows["nodes"] * rows["ranks_per_node"])
        ]
    if rows["training_strategy"].endswith("_sharded"):
        rows["write_rows"] = [
            {**row, "global_rank": rank}
            for row in rows["write_rows"]
            for rank in range(rows["nodes"] * rows["ranks_per_node"])
        ]
    return rows


def test_strict_ray_validation_accepts_complete_ddp_metrics():
    calculate.validate_required_ray_metrics(**complete_metrics())


def test_strict_ray_validation_accepts_complete_model_parallel_resume_metrics():
    calculate.validate_required_ray_metrics(
        **complete_metrics(model_parallel=True, resume=True)
    )


def test_strict_ray_validation_rejects_missing_sharded_checkpoint_rank():
    rows = complete_metrics(model_parallel=True)
    rows["write_rows"].pop()

    with pytest.raises(SystemExit) as exc:
        calculate.validate_required_ray_metrics(**rows)

    assert exc.value.code == 1


def test_strict_ray_validation_rejects_nonzero_rank_for_full_checkpoint():
    rows = complete_metrics()
    rows["write_rows"][0]["global_rank"] = 1

    with pytest.raises(SystemExit) as exc:
        calculate.validate_required_ray_metrics(**rows)

    assert exc.value.code == 1


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda rows: rows["step_rows"].pop(1), "step IDs"),
        (lambda rows: rows["write_rows"].pop(), "checkpoint commits"),
        (lambda rows: rows["write_rows"].append(rows["write_rows"][0]), "commit"),
        (lambda rows: rows["size_rows"].pop(), "checkpoint sizes"),
        (lambda rows: rows["dataset_build_rows"].clear(), "dataset-build"),
        (
            lambda rows: rows["dataset_build_rows"].append(
                rows["dataset_build_rows"][0]
            ),
            "dataset-build",
        ),
        (lambda rows: rows["ray_data_iteration_rows"].pop(), "Ray Data"),
        (lambda rows: rows["delete_rows"].clear(), "deletion"),
        (lambda rows: rows["delete_rows"].append(rows["delete_rows"][0]), "deletion"),
    ],
)
def test_strict_ray_validation_rejects_incomplete_or_duplicate_metrics(mutate, match):
    rows = complete_metrics()
    mutate(rows)
    with pytest.raises(SystemExit) as exc:
        calculate.validate_required_ray_metrics(**rows)
    assert exc.value.code == 1


def test_strict_ray_validation_requires_every_restore_rank():
    rows = complete_metrics(resume=True)
    rows["restore_rows"].pop()
    with pytest.raises(SystemExit) as exc:
        calculate.validate_required_ray_metrics(**rows)
    assert exc.value.code == 1


def test_strict_ray_validation_rejects_wrong_data_split_identity():
    rows = complete_metrics()
    rows["ray_data_iteration_rows"][1]["split_index"] = 0
    with pytest.raises(SystemExit) as exc:
        calculate.validate_required_ray_metrics(**rows)
    assert exc.value.code == 1


def test_epoch_bounded_run_accepts_a_step_tail_inside_the_last_interval():
    """steps=-1 ends wherever the data ran out, which need not be a boundary."""
    rows = complete_metrics()
    rows["expected_steps"] = -1
    rows["step_rows"].append({"step": 5, "step_duration": 1.0, "step_end_time": 5.0})
    calculate.validate_required_ray_metrics(**rows)


def test_epoch_bounded_run_rejects_a_step_tail_lost_past_the_last_checkpoint(capsys):
    """Without this the surviving records would define their own expectation."""
    rows = complete_metrics()
    rows["expected_steps"] = -1
    # Steps 4+ lost, but the step-4 checkpoint proves the run got that far.
    rows["step_rows"] = [row for row in rows["step_rows"] if row["step"] < 4]
    with pytest.raises(SystemExit) as exc:
        calculate.validate_required_ray_metrics(**rows)
    assert exc.value.code == 1
    assert "step records end at 3" in capsys.readouterr().err


def test_epoch_bounded_run_rejects_steps_a_full_interval_past_the_last_checkpoint(
    capsys,
):
    rows = complete_metrics()
    rows["expected_steps"] = -1
    rows["step_rows"].extend(
        {"step": step, "step_duration": 1.0, "step_end_time": float(step)}
        for step in (5, 6)
    )
    with pytest.raises(SystemExit) as exc:
        calculate.validate_required_ray_metrics(**rows)
    assert exc.value.code == 1
    assert "step records end at 6" in capsys.readouterr().err
