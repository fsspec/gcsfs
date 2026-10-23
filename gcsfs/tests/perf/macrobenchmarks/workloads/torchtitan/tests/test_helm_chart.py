"""Torch-free checks for the TorchTitan Helm chart and launcher.

Kept separate from ``test_torchtitan_workload.py`` (which needs TorchTitan
``main`` + Grain and is skipped in CI) so these always run under
``pytest gcsfs/``.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

HELM_CHART_DIR = Path(__file__).resolve().parents[1] / "helm_chart"
LAUNCHER = HELM_CHART_DIR / "launcher.sh"


def _render(*sets: str) -> list[dict]:
    helm_bin = shutil.which("helm")
    if not helm_bin:
        pytest.skip("helm binary not available")
    cmd = [
        helm_bin,
        "template",
        "tt-llama70b",
        str(HELM_CHART_DIR),
        "-f",
        str(HELM_CHART_DIR / "values_base.yaml"),
    ]
    for value in sets:
        cmd += ["--set", value]
    rendered = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
    return [doc for doc in yaml.safe_load_all(rendered) if doc]


def _config_map(resources: list[dict], suffix: str) -> dict:
    return next(
        r
        for r in resources
        if r.get("kind") == "ConfigMap" and r["metadata"]["name"].endswith(suffix)
    )


def _container_env(resources: list[dict]) -> dict[str, str]:
    jobset = next(r for r in resources if r.get("kind") == "JobSet")
    pod = jobset["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]
    container = next(c for c in pod["containers"] if "command" in c)
    return {e["name"]: e.get("value") for e in container["env"]}


class TestHelmChartRendering:
    def test_renders_and_mounts_recipe(self):
        resources = _render(
            "workload.nodes=2",
            "workload.ranksPerNode=8",
            "gcsfs.datasetPath=gs://b/dataset16",
            "gcsfs.ckptWritePath=gs://b/checkpoints",
        )
        kinds = {r["kind"] for r in resources}
        assert {"JobSet", "ConfigMap", "Service"} <= kinds

        config = _config_map(resources, "-config")
        compile(config["data"]["torchtitan_train.py"], "torchtitan_train.py", "exec")
        assert (
            "torchtitan @ git+https://github.com/pytorch/torchtitan.git@main"
            in config["data"]["requirements.txt"]
        )
        launcher = _config_map(resources, "-launcher")["data"]["launcher.sh"]
        assert "-m torchtitan.train" in launcher

        jobset = next(r for r in resources if r["kind"] == "JobSet")
        replicated = jobset["spec"]["replicatedJobs"][0]["template"]["spec"]
        assert replicated["parallelism"] == 2
        pod = replicated["template"]["spec"]
        container = next(c for c in pod["containers"] if "command" in c)
        assert container["resources"]["limits"]["nvidia.com/gpu"] == 8
        assert any(c["name"] == "nccl-plugin-installer" for c in pod["initContainers"])

        env = _container_env(resources)
        assert env["ATTN_BACKEND"] == "flex"
        assert env["TORCH_NIGHTLY_INDEX"] == (
            "https://download.pytorch.org/whl/nightly/cu132"
        )
        assert env["DATASET_PATH"] == "gs://b/dataset16"
        assert env["CKPT_WRITE_PATH"] == "gs://b/checkpoints"
        for removed in (
            "TORCH_DISTRIBUTED_TARGET",
            "DATALOADER_NUM_WORKERS",
            "SHUFFLE_MAX_BUFFER_INPUT_SHARDS",
        ):
            assert removed not in env

    def test_run_id_is_unique_per_install(self):
        # Upstream resumes from the latest step-N under <ckptWritePath>/<RUN_ID>,
        # so RUN_ID must not be the bare release name.
        env = _container_env(_render())
        assert re.fullmatch(r"tt-llama70b-\d{8}-\d{6}", env["RUN_ID"])

    def test_defaults_do_not_point_at_personal_buckets(self):
        env = _container_env(_render())
        assert env["DATASET_PATH"] == ""
        assert env["CKPT_WRITE_PATH"] == ""
        assert env["MODEL_ID"] == "gs://huggingface-model-weights/Llama-3.1-8B"

    def test_default_checkpoint_schedule_exercises_purge(self):
        values = yaml.safe_load((HELM_CHART_DIR / "values_base.yaml").read_text())
        workload = values["workload"]
        steps = int(workload["steps"])
        interval = int(workload["ckptWriterInterval"])
        keep = int(workload["ckptToKeep"])
        assert keep == 0 or keep >= 2
        saves = len(set(range(interval, steps + 1, interval)) | {steps})
        # Upstream keeps keep-1 old checkpoints before each save.
        assert saves >= keep + 1


class TestLauncherFailFast:
    BASE = {
        "DATASET_PATH": "gs://b/dataset16",
        "CKPT_WRITE_PATH": "gs://b/checkpoints",
        "CKPT_LOAD_PATH": "",
        "MODEL_ID": "gs://b/models/Llama-3.1-8B",
    }

    def _run(self, **overrides: str) -> subprocess.CompletedProcess:
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
        env.update(self.BASE)
        env.update(overrides)
        # Every case below must exit at validation, before pip/NCCL setup.
        return subprocess.run(
            ["bash", str(LAUNCHER)],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

    def test_syntax(self):
        subprocess.run(["bash", "-n", str(LAUNCHER)], check=True)

    @pytest.mark.parametrize(
        ("overrides", "message"),
        [
            ({"DATASET_PATH": ""}, "gcsfs.datasetPath"),
            ({"CKPT_WRITE_PATH": ""}, "gcsfs.ckptWritePath"),
            ({"MODEL_ID": "meta-llama/Llama-3.1-8B"}, "must be a gs:// path"),
            ({"MODEL_ID": ""}, "must be a gs:// path"),
        ],
    )
    def test_rejects_missing_inputs(self, overrides, message):
        result = self._run(**overrides)
        assert result.returncode == 1
        assert message in result.stderr
        assert "pip install" not in result.stderr
