"""Tests for the adk-agent branches of the macrobenchmark pipeline steps.

Reuses the stubbed Cloud Build step runner from ``test_rcu_scripts.py``: each
step runs its real script under bash with only the env entries its
``macrobenchmarks-cloudbuild.yaml`` step passes.
"""

from pathlib import Path

import pytest
from metrics.tests.test_rcu_scripts import (  # noqa: F401 (env is a fixture)
    _build_var,
    _calls,
    _run_step,
    env,
    init_env,
)

SCENARIOS = ["iterative-workspace", "genmedia-pipeline", "hitl-handoff"]


@pytest.fixture
def adk_init_env(init_env):  # noqa: F811
    return {
        **init_env,
        "_WORKLOAD": "adk-agent",
        "_SCENARIO": "iterative-workspace",
        "_SEED_CHECKPOINT": "false",
        "_DATASET_PATH": "",
        "_NODES": "1",
    }


@pytest.mark.parametrize("scenario", SCENARIOS)
@pytest.mark.parametrize("bucket_type", ["regional", "zonal", "hns"])
def test_init_accepts_adk_without_a_dataset(adk_init_env, scenario, bucket_type):
    proc, calls = _run_step(
        "init-variables",
        adk_init_env,
        _SCENARIO=scenario,
        _BUCKET_TYPE=bucket_type,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _build_var(adk_init_env, "RUN_ID") == "buildid-1234abcd"
    # No dataset bucket is described for the ADK workload.
    assert not [c for c in _calls(calls, "gcloud storage buckets describe")]


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"_SCENARIO": "chat"}, "_SCENARIO"),
        ({"_SEED_CHECKPOINT": "true"}, "_SEED_CHECKPOINT=false"),
        ({"_CHECKPOINT_LOAD_PATH": "gs://somewhere/ckpt"}, "_CHECKPOINT_LOAD_PATH"),
        ({"_BUCKET_TYPE": "standard_rcu_z"}, "_BUCKET_TYPE"),
        ({"_NODES": "2"}, "_NODES=1"),
    ],
)
def test_init_rejects_bad_adk_settings_before_any_gcloud_call(
    adk_init_env, override, message
):
    proc, calls = _run_step("init-variables", adk_init_env, **override)

    assert proc.returncode == 1
    assert message in proc.stdout
    assert _calls(calls, "gcloud") == []


def test_init_still_requires_a_dataset_for_training_workloads(init_env):  # noqa: F811
    proc, _ = _run_step("init-variables", init_env, _DATASET_PATH="")

    assert proc.returncode == 1
    assert "_DATASET_PATH" in proc.stdout


def test_create_buckets_creates_only_the_artifact_bucket_for_adk(env):  # noqa: F811
    proc, calls = _run_step("create-buckets", env, _WORKLOAD="adk-agent")

    assert proc.returncode == 0, proc.stderr
    creates = _calls(calls, "gcloud storage buckets create")
    assert any("gs://ckpt-bucket" in c for c in creates)
    assert not any("gs://dataset-bucket" in c for c in creates)
    assert not _calls(calls, "gcloud storage rsync")
    assert not _calls(calls, "gcloud storage cp")
    assert _build_var(env, "RUN_DATASET_PATH") is None


def test_run_workload_installs_the_adk_chart_with_the_scenario(env):  # noqa: F811
    proc, calls = _run_step(
        "run-workload",
        env,
        _WORKLOAD="adk-agent",
        _SCENARIO="hitl-handoff",
        _REQUIREMENTS="google-adk[fsspec]==9.9.9 gcsfs==9.9.9",
        _SEED_CHECKPOINT="false",
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    (install,) = _calls(calls, "helm install")
    assert "gcsfs/tests/perf/macrobenchmarks/workloads/adk-agent/helm_chart" in install
    assert "workload.scenario=hitl-handoff" in install
    assert "workload.requirements=google-adk[fsspec]==9.9.9 gcsfs==9.9.9" in install
    assert "adkRequirements" not in install
    assert "gcsfs.ckptWritePath=gs://ckpt-bucket/checkpoints" in install


def test_scrape_metrics_uses_the_adk_parser_and_calculator(env, tmp_path):  # noqa: F811
    bin_dir = Path(env["PATH"].split(":", 1)[0])
    (bin_dir / "python3").write_text(
        '#!/usr/bin/env bash\necho "python3 $*" >> "$STUB_LOG"\n'
    )
    (tmp_path / "start_time.txt").write_text("2026-11-23T00:00:00Z")
    (tmp_path / "end_time.txt").write_text("2026-11-23T01:00:00Z")

    proc, calls = _run_step(
        "scrape-metrics",
        env,
        _WORKLOAD="adk-agent",
        _SCENARIO="genmedia-pipeline",
        _SEED_CHECKPOINT="false",
        _STEPS="100",
        _CHECKPOINT_INTERVAL="25",
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    python_calls = _calls(calls, "python3")
    assert any("-m metrics.parsers.adk" in c for c in python_calls)
    calculate = [c for c in python_calls if "-m metrics.calculate" in c]
    assert calculate
    assert all("--workload-name adk-genmedia-pipeline" in c for c in calculate)
    assert all("--require-artifact-metrics" in c for c in calculate)
    assert not any("--resume-run" in c for c in calculate)
    assert _build_var(env, "SUMMARY_URI").startswith("gs://results-bucket/branch=")
    (monitoring,) = [c for c in python_calls if "-m metrics.monitoring" in c]
    assert "--checkpoint-bucket ckpt-bucket" in monitoring
    # No dataset bucket exists for the ADK workload, so none is monitored.
    assert "dataset-bucket dataset-bucket" not in monitoring
