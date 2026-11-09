"""gcsfs.forceHttp in the workload charts: the seed release of *_rcu_warm and
*_rcu_cold sets it so gcsfs writes STANDARD objects over HTTP."""

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
def test_force_http_turns_off_the_extended_filesystem(chart):
    env = _workload_env(chart, "gcsfs.forceHttp=true")

    assert env["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] == "false"


@pytest.mark.parametrize("chart", CHARTS)
@pytest.mark.parametrize(
    "sets", [(), ("gcsfs.forceHttp=false",)], ids=["default", "false"]
)
def test_extended_filesystem_stays_on_without_force_http(chart, sets):
    env = _workload_env(chart, *sets)

    assert "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT" not in env
