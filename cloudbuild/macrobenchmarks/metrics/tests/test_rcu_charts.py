"""gcsfs.rcuRapidWrite in the workload charts: *_rcu_z runs (seed and
measured) set it so gcsfs writes RAPID objects to RCU buckets over gRPC; by
default writes go over HTTP (STANDARD)."""

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

WORKLOADS = (
    Path(__file__).resolve().parents[4] / "gcsfs/tests/perf/macrobenchmarks/workloads"
)
CHARTS = ["hf-datasets-pytorch-lightning", "ray-data-ray-train-pytorch"]


def _workload_env(chart, *sets):
    helm = shutil.which("helm")
    if not helm:
        pytest.skip("helm binary not available")
    chart_dir = WORKLOADS / chart / "helm_chart"
    cmd = [
        helm,
        "template",
        "t",
        str(chart_dir),
        "-f",
        str(chart_dir / "values_base.yaml"),
    ]
    for value in sets:
        cmd += ["--set", value]
    rendered = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
    jobset = next(
        d for d in yaml.safe_load_all(rendered) if d and d["kind"] == "JobSet"
    )
    pod = jobset["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]
    (container,) = pod["containers"]
    return {e["name"]: e.get("value") for e in container["env"]}


@pytest.mark.parametrize("chart", CHARTS)
def test_rcu_rapid_write_sets_fsspec_env_var(chart):
    env = _workload_env(chart, "gcsfs.rcuRapidWrite=true")

    assert env["FSSPEC_GCS_RCU_RAPID_WRITE"] == "true"
    assert "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT" not in env


@pytest.mark.parametrize("chart", CHARTS)
@pytest.mark.parametrize(
    "sets", [(), ("gcsfs.rcuRapidWrite=false",)], ids=["default", "false"]
)
def test_rcu_rapid_write_is_omitted_by_default_and_when_false(chart, sets):
    env = _workload_env(chart, *sets)

    assert "FSSPEC_GCS_RCU_RAPID_WRITE" not in env
    assert "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT" not in env
