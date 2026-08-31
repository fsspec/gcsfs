import importlib.util
import logging
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

MODULE_PATH = Path(__file__).parents[1] / "helm_chart" / "metric_logging.py"
CHART_PATH = MODULE_PATH.parent


def _load_metric_logging():
    if not MODULE_PATH.exists():
        pytest.fail("the Ray workload has no direct metric logging module")
    spec = importlib.util.spec_from_file_location("metric_logging", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_handler_rejects_records_that_cannot_be_atomic(tmp_path):
    metric_logging = _load_metric_logging()
    output = tmp_path / "container-stdout"
    output.touch()
    handler = metric_logging.AtomicContainerHandler(output)
    logger = logging.Logger("test-oversized-ray-metrics")
    logger.addHandler(handler)

    with pytest.raises(ValueError, match="atomic metric record"):
        logger.info("x" * 4096)

    handler.close()


def test_tiny_metric_values_stay_regex_parseable(tmp_path):
    metric_logging = _load_metric_logging()
    output = tmp_path / "container-stdout"
    output.touch()
    handler = metric_logging.AtomicContainerHandler(output)
    logger = logging.Logger("test-tiny-ray-metrics")
    logger.addHandler(handler)

    metric_logging.emit_metric(
        logger,
        event="step",
        global_rank=0,
        step=1,
        loss=0.0000001,
        duration_s=0.0000001,
        samples_per_second=0.0000001,
    )
    handler.close()

    assert output.read_text() == (
        "Global Rank: 0 | Step: 1 | Loss: 0.0000 | "
        "Step Time: 0.000000100s | Throughput: 0.000000 samples/s\n"
    )


def test_helm_chart_mounts_metric_logging_module():
    helm = shutil.which("helm")
    if helm is None:
        pytest.skip("helm is required to render the workload chart")
    rendered = subprocess.run(
        [
            helm,
            "template",
            "ray-metrics-test",
            str(CHART_PATH),
            "-f",
            str(CHART_PATH / "values_base.yaml"),
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    resources = list(yaml.safe_load_all(rendered))
    config_map = next(
        resource
        for resource in resources
        if resource.get("kind") == "ConfigMap"
        and resource["metadata"]["name"].endswith("-config")
    )
    jobset = next(
        resource for resource in resources if resource.get("kind") == "JobSet"
    )

    compile(config_map["data"]["metric_logging.py"], "metric_logging.py", "exec")
    items = jobset["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"][
        "volumes"
    ][0]["configMap"]["items"]
    assert {"key": "metric_logging.py", "path": "metric_logging.py"} in items


@pytest.mark.parametrize(
    ("event", "fields", "expected_lines"),
    [
        (
            "step",
            {
                "global_rank": 0,
                "step": 25,
                "loss": 1.25,
                "duration_s": 1.5,
                "samples_per_second": 42.0,
            },
            [
                "Global Rank: 0 | Step: 25 | Loss: 1.2500 | "
                "Step Time: 1.500000000s | Throughput: 42.000000 samples/s"
            ],
        ),
        (
            "dataset_build",
            {
                "global_rank": 0,
                "duration_s": 3.25,
                "dataset_path": "gs://bucket/dataset",
            },
            [
                "Dataset Build : Rank : 0 : Duration : 3.250000000 seconds : "
                "Path: gs://bucket/dataset"
            ],
        ),
        (
            "checkpoint_committed",
            {
                "global_rank": 0,
                "checkpoint_step": 25,
                "checkpoint_location": "bucket/checkpoints/step-25.ckpt",
                "start_time_s": 26.0,
                "duration_s": 4.0,
            },
            [
                "Checkpoint Save : Rank: 0 : Step: 25 : Start time: 26.000000 "
                "seconds: Path: bucket/checkpoints/step-25.ckpt",
                "Finished saving checkpoint to bucket/checkpoints/step-25.ckpt "
                "in 4.000000000 seconds for global_step 25 from rank 0",
            ],
        ),
        (
            "checkpoint_size",
            {
                "checkpoint_step": 25,
                "checkpoint_location": "bucket/checkpoints/step-25.ckpt",
                "size_bytes": 1234,
            },
            [
                "Checkpoint Size : Rank : 0 : Step : 25 : Bytes : 1234 : "
                "Path: bucket/checkpoints/step-25.ckpt"
            ],
        ),
        (
            "checkpoint_restore_started",
            {
                "global_rank": 0,
                "checkpoint_location": "bucket/seed/step-1.ckpt",
                "time_s": 50.0,
            },
            [
                "Checkpoint Restore Start : Rank : 0 : Start time: 50.000000 "
                "seconds : Path: bucket/seed/step-1.ckpt"
            ],
        ),
        (
            "checkpoint_restore_completed",
            {
                "global_rank": 0,
                "checkpoint_location": "bucket/seed/step-1.ckpt",
                "duration_s": 7.5,
                "time_s": 57.5,
            },
            [
                "Finished restoring checkpoint : Rank : 0 : Duration: 7.500000000 "
                "seconds : End Time: 57.500000 seconds : Path: "
                "bucket/seed/step-1.ckpt"
            ],
        ),
        (
            "checkpoint_deleted",
            {
                "checkpoint_step": 25,
                "checkpoint_location": "bucket/checkpoints/step-25.ckpt",
                "duration_s": 2.0,
                "success": True,
            },
            [
                "Finished deleting checkpoint bucket/checkpoints/step-25.ckpt "
                "in 2.000000000 seconds for global_step 25 from rank 0"
            ],
        ),
        (
            "ray_data_iteration",
            {
                "global_rank": 0,
                "split_index": 0,
                "total_blocked_s": 10.0,
                "total_s": 20.0,
                "time_to_first_batch_s": 3.0,
                "blocked_calls": 5,
            },
            [
                "Ray Data Iteration : Rank : 0 : Split : 0 : Blocked : "
                "10.000000000 seconds : Total : 20.000000000 seconds : "
                "First Batch : 3.000000000 seconds : Blocked Calls : 5"
            ],
        ),
    ],
)
def test_emit_metric_uses_lightning_record_formats(
    tmp_path, event, fields, expected_lines
):
    metric_logging = _load_metric_logging()
    output = tmp_path / "container-stdout"
    output.touch()
    handler = metric_logging.AtomicContainerHandler(output)
    logger = logging.Logger(f"test-{event}")
    logger.addHandler(handler)

    metric_logging.emit_metric(
        logger,
        event=event,
        **fields,
    )
    handler.close()

    assert output.read_text().splitlines() == expected_lines
