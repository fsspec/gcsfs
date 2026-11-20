"""Tests for checkpoint path validation and shell quoting in seed-checkpoint
and rcu-prepare-reads.

Each step runs its real bash script from the repo root with stub ``gcloud`` /
``kubectl`` / ``helm`` / ``curl`` / ``sleep`` binaries on PATH, matching Cloud
Build's execution model.
"""

import os
import re
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from .test_rcu_scripts import HELM_STUB

REPO = Path(__file__).resolve().parents[4]
STEPS = {
    step["id"]: step
    for step in yaml.safe_load(
        (
            REPO / "cloudbuild/macrobenchmarks/macrobenchmarks-cloudbuild.yaml"
        ).read_text()
    )["steps"]
}
PLUMBING = (
    "PATH",
    "STUB_LOG",
    "STUB_MANIFESTS",
    "FAILED_FILE",
    "BUILD_VARS_FILE",
    "WORKSPACE_DIR",
)

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")

GCLOUD_STUB = r"""#!/usr/bin/env bash
echo "gcloud $*" >> "$STUB_LOG"
case "$*" in
  "storage objects list"*) [ -n "$STUB_LIST" ] && printf '%s\n' "$STUB_LIST" ;;
  "storage ls"*) [ -n "$STUB_LS" ] && printf '%s\n' "$STUB_LS" ;;
esac
exit 0
"""

KUBECTL_STUB = r"""#!/usr/bin/env bash
echo "kubectl $*" >> "$STUB_LOG"
case "$*" in
  "apply -f -"*) { cat; echo "---"; } >> "$STUB_MANIFESTS" ;;
  "get jobset "*Completed*) echo True ;;
  "get job "*) echo "1/" ;;
esac
exit 0
"""


def _stub(body):
    return f"#!/usr/bin/env bash\n{body}\n"


@pytest.fixture
def env(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in {
        "gcloud": GCLOUD_STUB,
        "kubectl": KUBECTL_STUB,
        "helm": HELM_STUB,
        "curl": _stub('echo "curl $*" >> "$STUB_LOG"'),
        "sleep": _stub('echo "sleep $*" >> "$STUB_LOG"'),
    }.items():
        stub = bin_dir / name
        stub.write_text(body)
        stub.chmod(0o755)

    build_vars = tmp_path / "build_vars.env"
    build_vars.write_text(
        "export RUN_ID=buildid-test\n"
        "export CLUSTER_NAME=test-cluster\n"
        "export CLUSTER_LOCATION=us-central1-a\n"
        "export CHECKPOINT_BUCKET=ckpt-bucket\n"
        "export DATASET_BUCKET=dataset-bucket\n"
        "export RESULTS_BUCKET=results-bucket\n"
        "export REGION=us-central1\n"
        "export RUN_DATASET_PATH=gs://dataset-bucket/data\n"
        "export DATASET_SRC_IS_RAPID=no\n"
    )
    return {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "STUB_LOG": str(tmp_path / "calls.log"),
        "STUB_MANIFESTS": str(tmp_path / "manifests.yaml"),
        "FAILED_FILE": str(tmp_path / "FAILED"),
        "BUILD_VARS_FILE": str(build_vars),
        "WORKSPACE_DIR": str(tmp_path),
        "PROJECT_ID": "test-project",
        "LOCATION": "us-central1",
        "_INFRA_PREFIX": "pfx",
        "_ZONE": "us-central1-a",
        "_MACHINE_TYPE": "test-machine",
        "_IMAGE": "test-image:1",
        "_REQUIREMENTS": "gcsfs-under-test",
        "_DATASET_PATH": "gs://src-bucket/data",
        "_BUCKET_TYPE": "regional",
        "_SEED_CHECKPOINT": "true",
        "_CHECKPOINT_LOAD_PATH": "",
        "_WORKLOAD": "hf-datasets-pytorch-lightning",
        "_TRAINING_STRATEGY": "ddp",
        "_TENSOR_PARALLEL_SIZE": "4",
        "_DATA_PARALLEL_SIZE": "2",
        "_NODES": "2",
        "_RANKS_PER_NODE": "4",
        "_STEPS": "10",
        "_CHECKPOINT_INTERVAL": "5",
        "_CKPT_TO_KEEP": "1",
        "_PER_DEVICE_BATCH": "8",
        "_GRAD_ACCUM": "1",
        "_DATALOADER_WORKERS": "4",
        "_EPOCHS": "1",
        "_SHUFFLE_BUFFER_SIZE": "1000",
        "_SHUFFLE_MAX_BUFFER_INPUT_SHARDS": "10",
        "_DATALOADER_PREFETCH_FACTOR": "2",
        "_SIMULATED_STEP_COMPUTE_SECONDS": "0.0",
        "_RCU_WARMUP_SETTLE_SECONDS": "0",
    }


def _run_step(step_id, env, **overrides):
    values = {**env, **overrides}
    log = Path(values["STUB_LOG"])
    before = len(log.read_text().splitlines()) if log.exists() else 0
    step = STEPS[step_id]
    run_env = {k: values[k] for k in PLUMBING}
    run_env.update({k: v for k, v in values.items() if k.startswith("STUB_")})
    for entry in step.get("env", []):
        name, template = entry.split("=", 1)
        run_env[name] = re.sub(
            r"\$\{(\w+)\}", lambda m: values.get(m.group(1), ""), template
        )
    proc = subprocess.run(
        ["bash", *step["args"]], cwd=REPO, env=run_env, capture_output=True, text=True
    )
    calls = log.read_text().splitlines()[before:] if log.exists() else []
    return proc, calls


def _calls(calls, prefix):
    return [c for c in calls if c.startswith(prefix)]


def _failures(env):
    failed = Path(env["FAILED_FILE"])
    return failed.read_text().split() if failed.exists() else []


def _source_var(build_vars_file, var_name):
    proc = subprocess.run(
        [
            "bash",
            "-c",
            f'set -e; source "$1"; printf "%s" "${{{var_name}-__UNSET__}}"',
            "_",
            str(build_vars_file),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return None if proc.stdout == "__UNSET__" else proc.stdout


def _bash_printf_q(value):
    return subprocess.run(
        ["bash", "-c", 'printf "export SEEDED_CKPT_PATH=%q\\n" "$1"', "_", value],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


@pytest.mark.parametrize(
    ("case_id", "ls_template"),
    [
        ("cmd_subst", "gs://ckpt-bucket/seed/buildid-test-seed/$(touch {marker}).ckpt"),
        (
            "semicolon",
            "gs://ckpt-bucket/seed/buildid-test-seed/x; touch {marker} #.ckpt",
        ),
        ("backticks", "gs://ckpt-bucket/seed/buildid-test-seed/`touch {marker}`.ckpt"),
        (
            "double_quotes",
            'gs://ckpt-bucket/seed/buildid-test-seed/"$(touch {marker})".ckpt',
        ),
        ("single_quotes", "gs://ckpt-bucket/seed/buildid-test-seed/'quoted'.ckpt"),
        ("spaces", "gs://ckpt-bucket/seed/buildid-test-seed/bad space.ckpt"),
        ("dotdot_leading", "gs://ckpt-bucket/seed/buildid-test-seed/../other/m.ckpt"),
        ("dotdot_nested", "gs://ckpt-bucket/seed/buildid-test-seed/sub/../m.ckpt"),
        (
            "double_slash_boundary",
            "gs://ckpt-bucket/seed/buildid-test-seed//model.ckpt",
        ),
        (
            "double_slash_nested",
            "gs://ckpt-bucket/seed/buildid-test-seed/sub//model.ckpt",
        ),
        (
            "newline_inject_ckpt",
            "gs://ckpt-bucket/seed/buildid-test-seed/prefix\n$(touch {marker}).ckpt",
        ),
        (
            "newline_two_ckpts",
            "gs://ckpt-bucket/seed/buildid-test-seed/ok.ckpt\n"
            "gs://ckpt-bucket/seed/buildid-test-seed/$(touch {marker}).ckpt",
        ),
        (
            "newline_trailing_line",
            "gs://ckpt-bucket/seed/buildid-test-seed/ok.ckpt\n$(touch {marker})",
        ),
        ("wrong_bucket", "gs://other-bucket/seed/buildid-test-seed/model.ckpt"),
        ("wrong_run_prefix", "gs://ckpt-bucket/seed/other-run-seed/model.ckpt"),
        ("non_ascii", "gs://ckpt-bucket/seed/buildid-test-seed/café.ckpt"),
        ("bare_dot_ckpt", "gs://ckpt-bucket/seed/buildid-test-seed/.ckpt"),
    ],
)
def test_seed_checkpoint_rejects_crafted_or_invalid_ckpt_paths(
    env, tmp_path, case_id, ls_template
):
    marker = tmp_path / f"pwned-{case_id}"
    stub_ls = ls_template.format(marker=marker)

    proc, calls = _run_step("seed-checkpoint", env, STUB_LS=stub_ls)

    assert proc.returncode != 0, proc.stdout + proc.stderr
    assert "ERROR:" in proc.stdout + proc.stderr
    assert _failures(env) == ["seed-checkpoint"]
    assert "SEEDED_CKPT_PATH" not in Path(env["BUILD_VARS_FILE"]).read_text()
    assert _calls(calls, "helm uninstall buildid-test-seed")
    assert _source_var(env["BUILD_VARS_FILE"], "SEEDED_CKPT_PATH") is None
    assert not marker.exists()


def test_seed_checkpoint_missing_ckpt_fails_closed(env):
    proc, calls = _run_step(
        "seed-checkpoint",
        env,
        STUB_LS="gs://ckpt-bucket/seed/buildid-test-seed/experiment_state.json",
    )

    assert proc.returncode != 0
    assert _failures(env) == ["seed-checkpoint"]
    assert "SEEDED_CKPT_PATH" not in Path(env["BUILD_VARS_FILE"]).read_text()
    assert _calls(calls, "helm uninstall buildid-test-seed")


def test_seed_checkpoint_shell_escapes_seeded_ckpt_path_with_printf_q(env, tmp_path):
    marker = tmp_path / "pwned-seed-quote"
    tricky_run_id = f"buildid-test$(touch {marker})"
    build_vars = Path(env["BUILD_VARS_FILE"])
    build_vars.write_text(
        build_vars.read_text().replace(
            "export RUN_ID=buildid-test\n",
            f"export RUN_ID={shlex.quote(tricky_run_id)}\n",
        )
    )
    expected = f"gs://ckpt-bucket/seed/{tricky_run_id}-seed/epoch=0-step=1.ckpt"

    proc, _ = _run_step("seed-checkpoint", env, STUB_LS=expected)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert build_vars.read_text().endswith(_bash_printf_q(expected))
    assert _source_var(build_vars, "SEEDED_CKPT_PATH") == expected
    assert not marker.exists()


@pytest.mark.parametrize(
    ("ls_output", "expected_seed", "expected_cold"),
    [
        (
            "gs://ckpt-bucket/seed/buildid-test-seed/sub/epoch=0-step=1.ckpt",
            "gs://ckpt-bucket/seed/buildid-test-seed/sub/epoch=0-step=1.ckpt",
            "gs://ckpt-bucket/seed-cold/epoch=0-step=1.ckpt",
        ),
        (
            "gs://ckpt-bucket/seed/buildid-test-seed/llama-epoch=00-step=01.ckpt",
            "gs://ckpt-bucket/seed/buildid-test-seed/llama-epoch=00-step=01.ckpt",
            "gs://ckpt-bucket/seed-cold/llama-epoch=00-step=01.ckpt",
        ),
        (
            "gs://ckpt-bucket/seed/buildid-test-seed/sub/step_1.ckpt/",
            "gs://ckpt-bucket/seed/buildid-test-seed/sub/step_1.ckpt",
            "gs://ckpt-bucket/seed-cold/step_1.ckpt",
        ),
        (
            "gs://ckpt-bucket/seed/buildid-test-seed/\n"
            "gs://ckpt-bucket/seed/buildid-test-seed/experiment_state.json\n"
            "gs://ckpt-bucket/seed/buildid-test-seed/TorchTrainer_001/step-1.ckpt/",
            "gs://ckpt-bucket/seed/buildid-test-seed/TorchTrainer_001/step-1.ckpt",
            "gs://ckpt-bucket/seed-cold/step-1.ckpt",
        ),
    ],
    ids=["ddp-nested", "ddp-lightning", "fsdp-dir", "ray-dir"],
)
@pytest.mark.parametrize(
    "bucket_type", ["regional", "standard_rcu_warm", "standard_rcu_cold"]
)
def test_seed_checkpoint_valid_ddp_and_fsdp_round_trip(
    env, ls_output, expected_seed, expected_cold, bucket_type
):
    build_vars = Path(env["BUILD_VARS_FILE"])

    proc, calls = _run_step(
        "seed-checkpoint", env, _BUCKET_TYPE=bucket_type, STUB_LS=ls_output
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _failures(env) == []
    assert build_vars.read_text().endswith(_bash_printf_q(expected_seed))
    assert _source_var(build_vars, "SEEDED_CKPT_PATH") == expected_seed
    assert _calls(calls, "helm uninstall --wait buildid-test-seed")

    proc, _ = _run_step(
        "rcu-prepare-reads",
        env,
        _BUCKET_TYPE=bucket_type,
        STUB_LIST="obj\tSTANDARD",
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _failures(env) == []

    expected_load = (
        expected_cold if bucket_type == "standard_rcu_cold" else expected_seed
    )
    assert _source_var(build_vars, "SEEDED_CKPT_PATH") == expected_load

    proc, calls = _run_step("run-workload", env, _BUCKET_TYPE=bucket_type)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _failures(env) == []
    (install,) = _calls(calls, "helm install ")
    assert f"--set-string gcsfs.ckptLoadPath={expected_load}" in install


@pytest.mark.parametrize(
    ("case_id", "leaf"),
    [
        ("cmd_subst", 'x$(touch "$STUB_MARKER").ckpt'),
        ("semicolon", 'x; touch "$STUB_MARKER" #.ckpt'),
        ("backticks", 'x`touch "$STUB_MARKER"`.ckpt'),
        ("spaces", "my model.ckpt"),
    ],
)
def test_rcu_prepare_reads_cold_shell_escapes_seeded_ckpt_path(
    env, tmp_path, case_id, leaf
):
    marker = tmp_path / f"pwned-cold-{case_id}"
    seed_path = f"gs://ckpt-bucket/seed/buildid-test-seed/{leaf}"
    expected_cold = f"gs://ckpt-bucket/seed-cold/{leaf}"
    build_vars = Path(env["BUILD_VARS_FILE"])
    with open(build_vars, "a") as f:
        f.write(_bash_printf_q(seed_path))

    proc, _ = _run_step(
        "rcu-prepare-reads",
        env,
        _BUCKET_TYPE="standard_rcu_cold",
        STUB_LIST="obj\tSTANDARD",
        STUB_MARKER=str(marker),
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _failures(env) == []
    assert build_vars.read_text().endswith(_bash_printf_q(expected_cold))
    assert _source_var(build_vars, "SEEDED_CKPT_PATH") == expected_cold
    assert not marker.exists()

    proc, calls = _run_step(
        "run-workload",
        env,
        _BUCKET_TYPE="standard_rcu_cold",
        STUB_MARKER=str(marker),
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _failures(env) == []
    assert not marker.exists()
    (install,) = _calls(calls, "helm install ")
    assert f"--set-string gcsfs.ckptLoadPath={expected_cold}" in install
