import csv

import pytest
from metrics import calculate


def test_data_loading_passthrough_uses_run_wide_row():
    rows = [
        {
            "run_id": "r",
            "epoch_idx": 0,
            "accelerator_blocked_time": 1.0,
            "accelerator_blocked_percent": 2.0,
        },
        {
            "run_id": "r",
            "epoch_idx": -1,
            "accelerator_blocked_time": 12.5,
            "accelerator_blocked_percent": 4.2,
        },
    ]
    m = calculate.calc_data_loading_metrics(rows)
    assert m["accelerator_blocked_time"] == 12.5
    assert m["accelerator_blocked_percent"] == 4.2


def test_data_loading_uses_bottleneck_rank():
    # Every rank emits a run-wide row; the distributed step is gated by the
    # slowest rank, so report the row with the greatest blocked time (and its
    # paired percent), deterministically regardless of row order.
    rows = [
        {
            "epoch_idx": -1,
            "accelerator_blocked_time": 5.0,
            "accelerator_blocked_percent": 2.0,
        },
        {
            "epoch_idx": -1,
            "accelerator_blocked_time": 12.5,
            "accelerator_blocked_percent": 4.2,
        },
        {
            "epoch_idx": -1,
            "accelerator_blocked_time": 9.0,
            "accelerator_blocked_percent": 3.0,
        },
    ]
    m = calculate.calc_data_loading_metrics(rows)
    assert m["accelerator_blocked_time"] == 12.5
    assert m["accelerator_blocked_percent"] == 4.2


def test_build_summary_row_keys_subset_of_fieldnames():
    row = calculate.build_summary_row(
        run_id="r",
        workload_name="hf-pytorch-lightning-cpu",
        requirements="gcsfs==1.0",
        step_rows=[{"step": 0, "step_duration": 1.0, "step_end_time": 1.0}],
        write_rows=[],
        restore_rows=[],
        delete_rows=[],
        dl_rows=[],
    )
    assert row["run_id"] == "r"
    assert row["workload_name"] == "hf-pytorch-lightning-cpu"
    assert row["requirements"] == "gcsfs==1.0"
    assert set(row).issubset(set(calculate.SUMMARY_FIELDNAMES))


def test_main_writes_one_row_summary(tmp_path):
    in_dir = tmp_path / "raw"
    (in_dir / "training_time").mkdir(parents=True)
    with open(in_dir / "training_time" / "step_time.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["step", "step_duration", "step_end_time"])
        w.writerow([0, 1.0, 1.0])
        w.writerow([1, 1.0, 2.0])
    out_file = tmp_path / "summary.csv"
    calculate.main(
        [
            "--run-id",
            "r",
            "--workload-name",
            "hf-pytorch-lightning-cpu",
            "--requirements",
            "gcsfs==1.0",
            "--in-dir",
            str(in_dir),
            "--out-file",
            str(out_file),
        ]
    )
    with open(out_file) as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["run_id"] == "r"
    assert rows[0]["mean_step_time"] == "1.0"
    # columns present and ordered as SUMMARY_FIELDNAMES
    with open(out_file) as f:
        header = f.readline().strip().split(",")
    assert header == calculate.SUMMARY_FIELDNAMES


def test_main_fails_when_required_step_metrics_are_missing(tmp_path):
    out_file = tmp_path / "summary.csv"
    with pytest.raises(SystemExit) as exc:
        calculate.main(
            [
                "--run-id",
                "r",
                "--workload-name",
                "hf-pytorch-lightning-cpu",
                "--requirements",
                "gcsfs==1.0",
                "--in-dir",
                str(tmp_path / "raw"),
                "--out-file",
                str(out_file),
                "--expected-steps",
                "2",
            ]
        )
    assert exc.value.code == 1
    assert not out_file.exists()


def test_main_fails_when_required_write_metrics_are_missing(tmp_path):
    in_dir = tmp_path / "raw"
    (in_dir / "training_time").mkdir(parents=True)
    with open(in_dir / "training_time" / "step_time.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["step", "step_duration", "step_end_time"])
        w.writerow([0, 1.0, 1.0])
        w.writerow([1, 1.0, 2.0])
    out_file = tmp_path / "summary.csv"
    with pytest.raises(SystemExit) as exc:
        calculate.main(
            [
                "--run-id",
                "r",
                "--workload-name",
                "hf-pytorch-lightning-cpu",
                "--requirements",
                "gcsfs==1.0",
                "--in-dir",
                str(in_dir),
                "--out-file",
                str(out_file),
                "--expected-steps",
                "2",
                "--min-write-datapoints",
                "1",
            ]
        )
    assert exc.value.code == 1
    assert not out_file.exists()


def test_resume_validation_accepts_final_step_target_and_observed_writes():
    # A resumed Lightning run stops at the configured global max_steps. If it
    # resumes from step 25 and targets 100, it only emits steps 26..100 and
    # writes checkpoints at 50/75/100. Validation should use that observed range
    # instead of expecting 100 fresh step rows and 4 fresh writes.
    step_rows = [
        {"step": step, "step_duration": 1.0, "step_end_time": float(step)}
        for step in range(26, 101)
    ]
    write_rows = [
        {
            "checkpoint_step": step,
            "checkpoint_location": "gs://b/ckpt",
            "start_time": float(step),
            "end_time": float(step) + 1.0,
        }
        for step in (50, 75, 100)
    ]

    calculate.validate_required_metrics(
        step_rows=step_rows,
        write_rows=write_rows,
        expected_steps=100,
        min_write_datapoints=4,
        checkpoint_interval=25,
        resume_run=True,
    )


def test_resume_validation_rejects_missing_observed_checkpoint_write():
    step_rows = [
        {"step": step, "step_duration": 1.0, "step_end_time": float(step)}
        for step in range(26, 101)
    ]
    write_rows = [
        {
            "checkpoint_step": step,
            "checkpoint_location": "gs://b/ckpt",
            "start_time": float(step),
            "end_time": float(step) + 1.0,
        }
        for step in (50, 75)
    ]

    with pytest.raises(SystemExit) as exc:
        calculate.validate_required_metrics(
            step_rows=step_rows,
            write_rows=write_rows,
            expected_steps=100,
            min_write_datapoints=4,
            checkpoint_interval=25,
            resume_run=True,
        )
    assert exc.value.code == 1


def _write_step_csv(in_dir):
    (in_dir / "training_time").mkdir(parents=True)
    with open(in_dir / "training_time" / "step_time.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["step", "step_duration", "step_end_time"])
        w.writerow([0, 1.0, 1.0])
        w.writerow([1, 1.0, 2.0])


def _write_data_loading_csv(in_dir, time="12.5", percent="4.2"):
    (in_dir / "calculated_metrics").mkdir(parents=True)
    path = in_dir / "calculated_metrics" / "data_loading_metrics.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "run_id",
                "epoch_idx",
                "accelerator_blocked_time",
                "accelerator_blocked_percent",
                "update_timestamp",
            ]
        )
        w.writerow(["r", -1, time, percent, ""])


def _write_restore_csv(in_dir):
    restore_dir = (
        in_dir / "checkpoint_restore_time" / "persistent_storage" / "per_accelerator"
    )
    restore_dir.mkdir(parents=True)
    with open(restore_dir / "0.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "checkpoint_step",
                "checkpoint_location",
                "start_time",
                "end_time",
                "global_rank",
                "local_rank",
            ]
        )
        w.writerow([0, "gs://b/ckpt", 10.0, 18.0, 0, ""])


def _write_system_metrics_csv(in_dir):
    (in_dir / "system_metrics").mkdir(parents=True)
    path = in_dir / "system_metrics" / "system_metrics.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["pod_name", "metric", "peak", "mean"])
        w.writerow(["p0", "cpu", 3.0, 1.0])
        w.writerow(["p1", "cpu", 5.0, 4.0])
        w.writerow(["p0", "memory", 2048.0, ""])
        w.writerow(["p0", "network_received", 10.0, 2.0])


def test_main_emits_system_metric_columns(tmp_path):
    # Verify system metrics are reduced to bottleneck pod and typed correctly.
    in_dir = tmp_path / "raw"
    _write_step_csv(in_dir)
    _write_data_loading_csv(in_dir)
    _write_system_metrics_csv(in_dir)
    out_file = tmp_path / "summary.csv"
    calculate.main(
        [
            "--run-id",
            "r",
            "--workload-name",
            "hf-pytorch-lightning-cpu",
            "--requirements",
            "gcsfs==1.0",
            "--in-dir",
            str(in_dir),
            "--out-file",
            str(out_file),
            "--require-data-loading-metrics",
        ]
    )
    with open(out_file) as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["cpu_usage_peak_cores"] == "5.0"
    assert rows[0]["cpu_usage_mean_cores"] == "4.0"  # max of per-pod means
    assert rows[0]["memory_usage_peak_bytes"] == "2048"  # int-typed
    assert rows[0]["network_received_peak_bytes_per_sec"] == "10.0"
    assert rows[0]["network_received_mean_bytes_per_sec"] == "2.0"
    assert rows[0]["network_sent_peak_bytes_per_sec"] == "N/A"


def _write_dataset_system_metrics_csv(in_dir):
    (in_dir / "system_metrics").mkdir(parents=True)
    path = in_dir / "system_metrics" / "system_metrics.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["pod_name", "metric", "peak", "mean"])
        w.writerow(["ds", "dataset_read_bytes", 80.0, ""])
        w.writerow(["ds", "dataset_size_bytes", 1000.0, ""])
        w.writerow(["ds", "dataset_sample_count", 100.0, ""])


def test_main_emits_dataset_read_amplification_ratio(tmp_path):
    # Two executed steps (0, 1) * global_batch_size 2 = 4 samples consumed;
    # per-sample bytes = 1000/100 = 10, so an ideal single sharded pass reads
    # 40 bytes. Actual egress 80 -> ratio 2.0, end to end through the CSV.
    in_dir = tmp_path / "raw"
    _write_step_csv(in_dir)  # steps 0 and 1
    _write_data_loading_csv(in_dir)
    _write_dataset_system_metrics_csv(in_dir)
    out_file = tmp_path / "summary.csv"
    calculate.main(
        [
            "--run-id",
            "r",
            "--workload-name",
            "hf-pytorch-lightning-cpu",
            "--requirements",
            "gcsfs==1.0",
            "--in-dir",
            str(in_dir),
            "--out-file",
            str(out_file),
            "--require-data-loading-metrics",
            "--per-device-batch",
            "2",
            "--grad-accum",
            "1",
            "--nodes",
            "1",
            "--ranks-per-node",
            "1",
        ]
    )
    with open(out_file) as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["global_batch_size"] == "2"
    assert rows[0]["dataset_sample_count"] == "100"
    assert rows[0]["dataset_read_amplification_ratio"] == "2.0"
    assert "dataset_read_amplification_ratio" in calculate.SUMMARY_FIELDNAMES


def test_main_fails_when_required_data_loading_metrics_are_missing(tmp_path):
    # step metrics present, but no data_loading_metrics.csv -> must fail when
    # --require-data-loading-metrics is set (the profiler summary is required).
    in_dir = tmp_path / "raw"
    _write_step_csv(in_dir)
    out_file = tmp_path / "summary.csv"
    with pytest.raises(SystemExit) as exc:
        calculate.main(
            [
                "--run-id",
                "r",
                "--workload-name",
                "hf-pytorch-lightning-cpu",
                "--requirements",
                "gcsfs==1.0",
                "--in-dir",
                str(in_dir),
                "--out-file",
                str(out_file),
                "--require-data-loading-metrics",
            ]
        )
    assert exc.value.code == 1
    assert not out_file.exists()


def test_main_fails_when_required_restore_metrics_are_missing(tmp_path):
    in_dir = tmp_path / "raw"
    _write_step_csv(in_dir)
    _write_data_loading_csv(in_dir)
    out_file = tmp_path / "summary.csv"
    with pytest.raises(SystemExit) as exc:
        calculate.main(
            [
                "--run-id",
                "r",
                "--workload-name",
                "hf-pytorch-lightning-cpu",
                "--requirements",
                "gcsfs==1.0",
                "--in-dir",
                str(in_dir),
                "--out-file",
                str(out_file),
                "--min-restore-datapoints",
                "1",
            ]
        )
    assert exc.value.code == 1
    assert not out_file.exists()


def test_main_succeeds_when_required_restore_metrics_are_present(tmp_path):
    in_dir = tmp_path / "raw"
    _write_step_csv(in_dir)
    _write_data_loading_csv(in_dir)
    _write_restore_csv(in_dir)
    out_file = tmp_path / "summary.csv"
    calculate.main(
        [
            "--run-id",
            "r",
            "--workload-name",
            "hf-pytorch-lightning-cpu",
            "--requirements",
            "gcsfs==1.0",
            "--in-dir",
            str(in_dir),
            "--out-file",
            str(out_file),
            "--min-restore-datapoints",
            "1",
        ]
    )
    with open(out_file) as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["num_checkpoint_restore_datapoints"] == "1"


def test_main_fails_when_data_loading_metrics_present_but_null(tmp_path):
    # A row exists but the accelerator-blocked fields are N/A (parser miss /
    # Cloud Logging lag): still must fail rather than upload N/A metrics.
    in_dir = tmp_path / "raw"
    _write_step_csv(in_dir)
    _write_data_loading_csv(in_dir, time="N/A", percent="N/A")
    out_file = tmp_path / "summary.csv"
    with pytest.raises(SystemExit) as exc:
        calculate.main(
            [
                "--run-id",
                "r",
                "--workload-name",
                "hf-pytorch-lightning-cpu",
                "--requirements",
                "gcsfs==1.0",
                "--in-dir",
                str(in_dir),
                "--out-file",
                str(out_file),
                "--require-data-loading-metrics",
            ]
        )
    assert exc.value.code == 1
    assert not out_file.exists()


def test_main_emits_run_dimension_columns(tmp_path):
    in_dir = tmp_path / "raw"
    _write_step_csv(in_dir)
    _write_data_loading_csv(in_dir)
    out_file = tmp_path / "summary.csv"
    calculate.main(
        [
            "--run-id",
            "r",
            "--workload-name",
            "hf-pytorch-lightning-cpu",
            "--requirements",
            "gcsfs==1.0",
            "--in-dir",
            str(in_dir),
            "--out-file",
            str(out_file),
            "--require-data-loading-metrics",
            "--bucket-type",
            "zonal",
            "--zone",
            "us-central1-a",
            "--region",
            "us-central1",
            "--nodes",
            "2",
            "--steps",
            "100",
            "--checkpoint-interval",
            "25",
            "--dataset-path",
            "gs://ds/parquet",
            "--model-id",
            "gs://huggingface-model-weights/Llama-3.1-8B",
        ]
    )
    with open(out_file) as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["bucket_type"] == "zonal"
    assert rows[0]["zone"] == "us-central1-a"
    assert rows[0]["region"] == "us-central1"
    assert rows[0]["nodes"] == "2"
    assert rows[0]["steps"] == "100"
    assert rows[0]["checkpoint_interval"] == "25"
    assert rows[0]["dataset_path"] == "gs://ds/parquet"
    assert rows[0]["model_id"] == "gs://huggingface-model-weights/Llama-3.1-8B"


def test_main_emits_training_strategy_column(tmp_path):
    in_dir = tmp_path / "raw"
    _write_step_csv(in_dir)
    _write_data_loading_csv(in_dir)
    out_file = tmp_path / "summary.csv"
    calculate.main(
        [
            "--run-id",
            "r",
            "--workload-name",
            "hf-pytorch-lightning-cpu",
            "--requirements",
            "gcsfs==1.0",
            "--in-dir",
            str(in_dir),
            "--out-file",
            str(out_file),
            "--require-data-loading-metrics",
            "--training-strategy",
            "ddp",
        ]
    )
    with open(out_file) as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["training_strategy"] == "ddp"
    assert "training_strategy" in calculate.SUMMARY_FIELDNAMES


def test_main_emits_simulated_step_compute_seconds_column(tmp_path):
    in_dir = tmp_path / "raw"
    _write_step_csv(in_dir)
    _write_data_loading_csv(in_dir)
    out_file = tmp_path / "summary.csv"
    calculate.main(
        [
            "--run-id",
            "r",
            "--workload-name",
            "hf-pytorch-lightning-cpu",
            "--requirements",
            "gcsfs==1.0",
            "--in-dir",
            str(in_dir),
            "--out-file",
            str(out_file),
            "--require-data-loading-metrics",
            "--simulated-step-compute-seconds",
            "2.5",
        ]
    )
    with open(out_file) as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["simulated_step_compute_seconds"] == "2.5"
    assert "simulated_step_compute_seconds" in calculate.SUMMARY_FIELDNAMES


def test_main_emits_new_config_dimension_columns(tmp_path):
    in_dir = tmp_path / "raw"
    _write_step_csv(in_dir)
    _write_data_loading_csv(in_dir)
    out_file = tmp_path / "summary.csv"
    calculate.main(
        [
            "--run-id",
            "r",
            "--workload-name",
            "hf-pytorch-lightning-cpu",
            "--requirements",
            "gcsfs==1.0",
            "--in-dir",
            str(in_dir),
            "--out-file",
            str(out_file),
            "--require-data-loading-metrics",
            "--nodes",
            "2",
            "--ranks-per-node",
            "4",
            "--machine-type",
            "c4-standard-192",
            "--per-device-batch",
            "8",
            "--grad-accum",
            "4",
            "--dataloader-workers",
            "16",
            "--checkpoints-to-keep",
            "1",
            "--image",
            "nvcr.io/nvidia/pytorch:25.01-py3",
        ]
    )
    with open(out_file) as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["ranks_per_node"] == "4"
    assert rows[0]["machine_type"] == "c4-standard-192"
    assert rows[0]["per_device_train_batch_size"] == "8"
    assert rows[0]["gradient_accumulation_steps"] == "4"
    assert rows[0]["dataloader_num_workers"] == "16"
    assert rows[0]["checkpoints_to_keep"] == "1"
    assert rows[0]["image"] == "nvcr.io/nvidia/pytorch:25.01-py3"
    # global_batch_size = per_device_batch * grad_accum * nodes * ranks_per_node
    # = 8 * 4 * 2 * 4 = 256 (mirrors the sim's per_device * grad_accum *
    # world_size, with world_size = nodes * ranks_per_node).
    assert rows[0]["global_batch_size"] == "256"
    for col in (
        "ranks_per_node",
        "machine_type",
        "per_device_train_batch_size",
        "gradient_accumulation_steps",
        "global_batch_size",
        "dataloader_num_workers",
        "checkpoints_to_keep",
        "image",
    ):
        assert col in calculate.SUMMARY_FIELDNAMES


def test_global_batch_size_omitted_when_components_missing(tmp_path):
    # global_batch_size is derived from four dimension flags; if any is absent
    # it must be left N/A rather than crash or report a partial product.
    in_dir = tmp_path / "raw"
    _write_step_csv(in_dir)
    _write_data_loading_csv(in_dir)
    out_file = tmp_path / "summary.csv"
    calculate.main(
        [
            "--run-id",
            "r",
            "--workload-name",
            "hf-pytorch-lightning-cpu",
            "--requirements",
            "gcsfs==1.0",
            "--in-dir",
            str(in_dir),
            "--out-file",
            str(out_file),
            "--require-data-loading-metrics",
            "--nodes",
            "2",
            "--per-device-batch",
            "8",
            "--grad-accum",
            "4",
        ]
    )
    with open(out_file) as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["global_batch_size"] == "N/A"


def test_main_succeeds_with_required_data_loading_metrics(tmp_path):
    in_dir = tmp_path / "raw"
    _write_step_csv(in_dir)
    _write_data_loading_csv(in_dir)
    out_file = tmp_path / "summary.csv"
    calculate.main(
        [
            "--run-id",
            "r",
            "--workload-name",
            "hf-pytorch-lightning-cpu",
            "--requirements",
            "gcsfs==1.0",
            "--in-dir",
            str(in_dir),
            "--out-file",
            str(out_file),
            "--require-data-loading-metrics",
        ]
    )
    with open(out_file) as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["accelerator_blocked_time"] == "12.5"
    assert rows[0]["accelerator_blocked_percent"] == "4.2"
