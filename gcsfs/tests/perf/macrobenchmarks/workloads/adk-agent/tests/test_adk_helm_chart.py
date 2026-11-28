"""Checks for the adk-agent Helm chart and launcher that need no ADK install."""

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

HELM_CHART_DIR = Path(__file__).resolve().parents[1] / "helm_chart"
REQUEST_COUNTER = (
    Path(__file__).resolve().parents[4] / "microbenchmarks" / "request_counter.py"
)


def _render(*sets: str) -> list:
    helm_bin = shutil.which("helm")
    if not helm_bin:
        pytest.skip("helm binary not available")
    command = [
        helm_bin,
        "template",
        "buildid-test",
        str(HELM_CHART_DIR),
        "-f",
        str(HELM_CHART_DIR / "values_base.yaml"),
    ]
    for value in sets:
        command += ["--set-literal", value]
    rendered = subprocess.run(
        command, check=True, capture_output=True, text=True
    ).stdout
    return [doc for doc in yaml.safe_load_all(rendered) if doc]


def test_request_counter_is_the_microbenchmark_module():
    chart_copy = HELM_CHART_DIR / "request_counter.py"

    assert chart_copy.read_text() == REQUEST_COUNTER.read_text()


def test_launcher_is_valid_bash():
    subprocess.run(["bash", "-n", str(HELM_CHART_DIR / "launcher.sh")], check=True)


def test_chart_ships_the_driver_and_passes_the_scenario():
    resources = _render(
        "workload.scenario=hitl-handoff",
        "workload.requirements=google-adk[fsspec,eval]==9.9.9 gcsfs>=2026.9.0,<2027",
        "gcsfs.ckptWritePath=gs://ckpt-bucket/checkpoints",
    )

    config = next(
        r
        for r in resources
        if r["kind"] == "ConfigMap" and r["metadata"]["name"].endswith("-config")
    )
    assert config["data"]["adk_bench.py"].strip() == (
        (HELM_CHART_DIR / "adk_bench.py").read_text().strip()
    )
    assert config["data"]["request_counter.py"].strip() == (
        REQUEST_COUNTER.read_text().strip()
    )
    assert config["data"]["requirements.txt"].strip() == (
        (HELM_CHART_DIR / "requirements.txt").read_text().strip()
    )
    jobset = next(r for r in resources if r["kind"] == "JobSet")
    job = jobset["spec"]["replicatedJobs"][0]["template"]["spec"]
    assert job["parallelism"] == 1
    assert job["template"]["spec"]["nodeSelector"] == {
        "cloud.google.com/gke-nodepool": "c4-standard-48"
    }
    (container,) = job["template"]["spec"]["containers"]
    env = {item["name"]: item.get("value") for item in container["env"]}
    assert env["SCENARIO"] == "hitl-handoff"
    assert env["REQUIREMENTS"] == "google-adk[fsspec,eval]==9.9.9 gcsfs>=2026.9.0,<2027"
    assert "ADK_REQUIREMENTS" not in env
    assert env["CKPT_WRITE_PATH"] == "gs://ckpt-bucket/checkpoints"
    assert container["image"] == "python:3.12"
