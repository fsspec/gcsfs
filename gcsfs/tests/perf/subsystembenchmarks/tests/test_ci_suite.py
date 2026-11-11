"""Guardrails for the per-PR CI suite (cloudbuild/subsystembenchmarks/scripts/run-ci.sh).

The CI suite exists to catch framework-lifecycle bugs (#1009, #1025, #1031, #1104), so its
shrunken configs must keep the lifecycle features those bugs needed.
"""

import importlib
import itertools
import os
import re
import subprocess
import sys
import textwrap
import time

import pytest
import yaml

from gcsfs.tests.perf.subsystembenchmarks import run

# group -> (configurator class, uses PyTorch DataLoader workers)
_CI_DATALOADING_GROUPS = {
    "huggingface_datasets": ("HuggingFaceReadConfigurator", True),
    "ray_data": ("RayDataReadConfigurator", False),
    "webdataset": ("WebDatasetReadConfigurator", True),
}


@pytest.mark.parametrize("group", sorted(_CI_DATALOADING_GROUPS))
def test_ci_config_yields_one_lifecycle_case(group, monkeypatch):
    monkeypatch.setenv("GCSFS_SUBSYSTEM_CONFIG", "configs.ci.yaml")
    monkeypatch.setenv("GCSFS_SUBSYSTEM_SWEEP_AXES", "baseline")  # run-ci.sh passes it
    monkeypatch.delenv("GCSFS_BENCHMARK_FILTER", raising=False)
    configs = importlib.import_module(
        f"gcsfs.tests.perf.subsystembenchmarks.dataloading.{group}.configs"
    )
    class_name, uses_dataloader_workers = _CI_DATALOADING_GROUPS[group]

    cases = getattr(configs, class_name)(configs.__file__).generate_cases()

    assert len(cases) == 1
    (case,) = cases
    assert case.rounds >= 2  # crosses an epoch boundary (#1104)
    assert case.world_size >= 2  # multi-rank
    if uses_dataloader_workers:
        # num_workers > 0 makes the drivers set persistent_workers=True (#1009/#1025/#1031)
        assert case.num_workers >= 1


_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
)
_RUN_CI = os.path.join(
    _REPO_ROOT, "cloudbuild", "subsystembenchmarks", "scripts", "run-ci.sh"
)


def _ci_cases():
    """Parse the `CASES=( "name|group|bucket_type|filter" ... )` table in run-ci.sh."""
    with open(_RUN_CI) as f:
        text = f.read()
    block = text.split("CASES=(", 1)[1].split("\n)", 1)[0]
    return [
        tuple(line.strip().strip('"').split("|"))
        for line in block.splitlines()
        if line.strip().startswith('"')
    ]


def test_run_ci_case_table_matches_the_design():
    cases = _ci_cases()
    names = [case[0] for case in cases]
    assert len(cases) == 11
    assert len(set(names)) == len(names)
    groups = set(run.discover_groups())
    for name, group, bucket_type, filter_expr in cases:
        assert group in groups, name
        assert bucket_type in ("regional", "zonal", "hns"), name
        if group.startswith("checkpointing/"):
            # Without a filter a checkpoint group runs both its read and write tests.
            # Case ids start with load-/save- (checkpointing/configurator.py).
            assert filter_expr in ("save", "load"), name
        else:
            assert filter_expr == "", name


# Case names encode what each row runs: ckpt-<read|write>-<pl|ray>-<bucket> and
# dl-<hf|ray|wds>-<bucket>. Tying every column to the name catches a row that silently
# runs the wrong test (e.g. a "read" case filtered to save) or the wrong bucket.
_NAME_TO_CKPT_GROUP = {
    "pl": "checkpointing/pytorch_lightning",
    "ray": "checkpointing/ray_pytorch",
}
_NAME_TO_CKPT_FILTER = {"read": "load", "write": "save"}
_NAME_TO_DL_GROUP = {
    "hf": "dataloading/huggingface_datasets",
    "ray": "dataloading/ray_data",
    "wds": "dataloading/webdataset",
}


def test_run_ci_case_names_match_their_group_bucket_and_filter():
    for name, group, bucket_type, filter_expr in _ci_cases():
        kind, *middle, bucket_token = name.split("-")
        assert bucket_token == bucket_type, name
        if kind == "ckpt":
            op, framework = middle
            expected = (_NAME_TO_CKPT_GROUP[framework], _NAME_TO_CKPT_FILTER[op])
        else:
            assert kind == "dl", name
            (framework,) = middle
            expected = (_NAME_TO_DL_GROUP[framework], "")
        assert (group, filter_expr) == expected, name


def test_run_ci_covers_every_checkpoint_combination():
    # Save and load, for both frameworks, on Rapid (zonal) and Standard (regional).
    got = sorted(
        tuple(name.split("-")[1:])
        for name, *_ in _ci_cases()
        if name.startswith("ckpt-")
    )
    want = sorted(
        itertools.product(("read", "write"), ("pl", "ray"), ("zonal", "regional"))
    )
    assert got == want


def test_run_ci_groups_have_ci_configs():
    ssb_root = os.path.dirname(run.__file__)
    for name, group, _, _ in _ci_cases():
        assert os.path.exists(os.path.join(ssb_root, group, "configs.ci.yaml")), name


def test_run_ci_uses_ci_config_for_every_group():
    with open(_RUN_CI) as f:
        text = f.read()
    assert 'GCSFS_SUBSYSTEM_CONFIG="configs.ci.yaml"' in text
    assert 'config="configs.yaml"' not in text


# group -> configurator class
_CI_CHECKPOINT_GROUPS = {
    "pytorch_lightning": "PyTorchLightningCheckpointConfigurator",
    "ray_pytorch": "RayCheckpointConfigurator",
}


@pytest.mark.parametrize("group", sorted(_CI_CHECKPOINT_GROUPS))
def test_ci_checkpoint_config_yields_multi_rank_save_and_load(group, monkeypatch):
    monkeypatch.setenv("GCSFS_SUBSYSTEM_CONFIG", "configs.ci.yaml")
    monkeypatch.setenv("GCSFS_SUBSYSTEM_SWEEP_AXES", "baseline")  # run-ci.sh passes it
    for key in (
        "GCSFS_BENCHMARK_FILTER",
        "GCSFS_SUBSYSTEM_MODEL_ID",
    ):
        monkeypatch.delenv(key, raising=False)
    configs = importlib.import_module(
        f"gcsfs.tests.perf.subsystembenchmarks.checkpointing.{group}.configs"
    )

    cases = getattr(configs, _CI_CHECKPOINT_GROUPS[group])(
        configs.__file__
    ).generate_cases()

    assert sorted(c.scenario for c in cases) == ["checkpoint_read", "checkpoint_write"]
    for case in cases:
        # ddp spawns real ranks without the Llama-only FSDP/TP wrapping (gpt2 has no
        # .model.layers), so the multi-process save/load path runs on the small model.
        assert case.strategy == "ddp"
        assert case.world_size >= 2
        assert case.rounds >= 2  # a second save/load on the same filesystem


def _run_ci_text():
    with open(_RUN_CI) as f:
        return f.read()


def test_run_ci_installs_the_code_under_test_after_framework_requirements():
    # datasets caps fsspec below what gcsfs needs; installing the code under test last
    # makes gcsfs's own floors win (otherwise fsspec is downgraded and gcsfs breaks).
    text = _run_ci_text()
    requirements = text.index('-r "$SSB/$group/requirements.txt"')
    upstream = text.index('install --quiet "$UPSTREAM/fsspec"')
    gcsfs = text.index('"$venv/bin/pip" install --quiet -e .')
    assert requirements < upstream < gcsfs


def test_run_ci_tests_against_upstream_fsspec_and_storage_main():
    text = _run_ci_text()
    assert "https://github.com/fsspec/filesystem_spec.git" in text
    assert "https://github.com/googleapis/google-cloud-python.git" in text
    assert "packages/google-cloud-storage" in text
    # Exported, the pretend version would also stamp the fsspec checkout as 0.0.0.
    assert "export SETUPTOOLS_SCM_PRETEND_VERSION" not in text
    assert 'env "${GCSFS_BUILD_ENV[@]}"' in text


def test_run_ci_gives_each_case_its_own_port_block():
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing import _dist

    text = _run_ci_text()
    assert f"PORT_BLOCK_SIZE={_dist.PORT_BLOCK_SIZE}" in text
    assert (
        'GCSFS_SUBSYSTEM_PORT_BASE="$((PORT_BLOCK_START + i * PORT_BLOCK_SIZE))"'
        in text
    )
    start = int(text.split("PORT_BLOCK_START=", 1)[1].split("\n", 1)[0])
    last = start + len(_ci_cases()) * _dist.PORT_BLOCK_SIZE
    # Above Ray's default worker ports, below the Linux ephemeral range.
    assert 19999 < start and last <= 32768


def test_run_ci_is_valid_bash():
    subprocess.run(["bash", "-n", _RUN_CI], check=True)


_CLOUDBUILD = os.path.join(
    _REPO_ROOT,
    "cloudbuild",
    "subsystembenchmarks",
    "subsystembenchmarks-cloudbuild.yaml",
)


def _cloudbuild_yaml():
    with open(_CLOUDBUILD) as f:
        return f.read()


def test_cloudbuild_mode_defaults_to_nightly_and_rejects_unknown_modes():
    cb = _cloudbuild_yaml()
    assert '_MODE: "nightly"' in cb
    assert "nightly|ci) ;;" in cb
    assert "printf 'export MODE=%q\\n' \"$${_MODE}\"" in cb


def test_cloudbuild_ci_mode_runs_run_ci_and_nightly_keeps_setup():
    cb = _cloudbuild_yaml()
    branch = cb.split('if [ "$$MODE" = "ci" ]; then', 1)[1].split("\n        fi\n", 1)[
        0
    ]
    ci_branch, nightly_branch = branch.split("\n        else\n", 1)
    assert "scripts/run-ci.sh" in ci_branch
    assert "scripts/setup.sh" not in ci_branch
    assert "scripts/setup.sh" in nightly_branch
    assert "scripts/run-benchmarks.sh" in nightly_branch


def test_cloudbuild_passes_the_ci_model_id_to_run_ci():
    # Each CI project points the suite at a model copy its VM service account can read.
    cb = _cloudbuild_yaml()
    assert '_CI_MODEL_ID: ""' in cb
    assert '"_CI_MODEL_ID=${_CI_MODEL_ID}"' in cb
    assert "printf 'export CI_MODEL_ID=%q\\n' \"$${_CI_MODEL_ID}\"" in cb
    text = _run_ci_text()
    assert 'CI_MODEL_ID="${CI_MODEL_ID:-gs://huggingface-model-weights/gpt2}"' in text


def test_run_ci_stages_the_model_under_its_own_basename():
    # checkpointing/*/common.py loads a gs:// model id from /tmp/<basename>.
    text = _run_ci_text()
    assert 'MODEL_DIR=$(basename "${CI_MODEL_ID%/}")' in text
    assert 'mv "$HOME/$MODEL_DIR" "/tmp/$MODEL_DIR"' in text
    assert '"$HOME/gpt2"' not in text


def test_upload_results_works_without_nightly_venv():
    path = os.path.join(
        _REPO_ROOT, "cloudbuild", "subsystembenchmarks", "scripts", "upload-results.sh"
    )
    with open(path) as f:
        text = f.read()
    assert "if [[ -f env/bin/activate ]]; then" in text


# --- run_case_with_timeout (run-ci.sh) ----------------------------------------------

# run.py -> pytest (subprocess) -> spawn Manager + 2 ranks, all stuck, like a hung case.
# Each process appends its pid to argv[1] so the test can check none survives.
_HUNG_CASE = textwrap.dedent(
    """
    import multiprocessing as mp
    import os
    import subprocess
    import sys
    import threading
    import time


    def record(pid_file, pid):
        with open(pid_file, "a") as f:
            f.write(f"{pid}\\n")


    def rank(pid_file):
        record(pid_file, os.getpid())
        threading.Thread(target=time.sleep, args=(600,), daemon=True).start()
        time.sleep(600)


    if __name__ == "__main__":
        pid_file = sys.argv[1]
        record(pid_file, os.getpid())
        if sys.argv[2:] == ["pytest"]:
            ctx = mp.get_context("spawn")
            manager = ctx.Manager()
            record(pid_file, manager._process.pid)
            ranks = [ctx.Process(target=rank, args=(pid_file,)) for _ in range(2)]
            for r in ranks:
                r.start()
            for r in ranks:
                r.join()
        else:
            cmd = [sys.executable, __file__, pid_file, "pytest"]
            sys.exit(subprocess.run(cmd).returncode)
    """
)


def _run_case_with_timeout(tmp_path, timeout_s, *cmd):
    """Runs run-ci.sh's run_case_with_timeout under the script's own shell options."""
    text = _run_ci_text()
    body = text.split("\nrun_case_with_timeout() {\n", 1)[1].split("\n}\n", 1)[0]
    script = (
        "set -euo pipefail\n"
        f"run_case_with_timeout() {{\n{body}\n}}\n"
        'rc=0; run_case_with_timeout "$@" || rc=$?; echo "rc=$rc"\n'
    )
    marker = tmp_path / "timed_out"
    env = dict(os.environ, PYTHONFAULTHANDLER="1")
    proc = subprocess.run(
        ["bash", "-c", script, "bash", str(timeout_s), str(marker), *cmd],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    out = proc.stdout + proc.stderr
    rc = int(re.findall(r"^rc=(\d+)$", out, re.M)[-1])
    return rc, out, marker


def test_run_case_with_timeout_returns_the_command_status(tmp_path):
    rc, _, marker = _run_case_with_timeout(tmp_path, 60, "bash", "-c", "exit 3")
    assert rc == 3
    assert not marker.exists()
    rc, _, _ = _run_case_with_timeout(tmp_path, 60, "true")
    assert rc == 0


def test_run_case_with_timeout_dumps_each_process_alone_then_kills_all(tmp_path):
    case = tmp_path / "hung_case.py"
    case.write_text(_HUNG_CASE)
    pid_file = tmp_path / "pids"

    rc, out, marker = _run_case_with_timeout(
        tmp_path, 3, sys.executable, str(case), str(pid_file)
    )

    assert rc == 124
    assert marker.exists()
    sections = re.findall(r"^===== pid \d+: ", out, re.M)
    dumps = re.findall(r"^Fatal Python error: Aborted$", out, re.M)
    # run.py, pytest, the Manager, 2 ranks (+ the resource tracker), one dump each.
    assert len(sections) >= 5
    assert len(dumps) == len(sections)
    # `timeout --signal=ABRT` aborts them all at once and the dumps interleave mid-line.
    mashed = re.compile(r"Fatal Python error.*Fatal Python error|Thread 0x.*Thread 0x")
    assert not [line for line in out.splitlines() if mashed.search(line)]
    pids = [int(p) for p in pid_file.read_text().split()]
    assert len(pids) == 5
    assert _wait_until_gone(pids) == []


def _wait_until_gone(pids, timeout_s=10):
    """Returns the pids still running (zombies count as gone) after up to timeout_s."""

    def running(pid):
        try:
            with open(f"/proc/{pid}/stat") as f:
                return f.read().rsplit(")", 1)[1].split()[0] != "Z"
        except FileNotFoundError:
            return False

    deadline = time.monotonic() + timeout_s
    while (alive := [p for p in pids if running(p)]) and time.monotonic() < deadline:
        time.sleep(0.1)
    return alive


# --- Cloud Build configuration -------------------------------------------------------


def _cloudbuild_step_script(step_id):
    with open(_CLOUDBUILD) as f:
        steps = yaml.safe_load(f)["steps"]
    (step,) = [s for s in steps if s["id"] == step_id]
    return step["args"][-1]


def _init_variables(tmp_path, **subs):
    """Runs the init-variables step and returns the build_vars.env it writes."""
    env_file = tmp_path / "build_vars.env"
    script = (
        _cloudbuild_step_script("init-variables")
        .replace("$$", "$")
        .replace("/workspace/build_vars.env", str(env_file))
    )
    env = {
        "PATH": os.environ["PATH"],
        "BUILD_ID": "62b31d50-92de-4aa3-9095-82ada77a76b6",
        "BRANCH_NAME": "feat/x",
        "_INFRA_PREFIX": "gcsfs-ci-ssb",
        "_ZONE": "us-central1-b",
        "_MACHINE_TYPE": "c4-highmem-96",
        "_BUCKET_TYPE": "regional",
        "_MODE": "nightly",
        **subs,
    }
    proc = subprocess.run(
        ["bash", "-c", script], env=env, capture_output=True, text=True
    )
    if proc.returncode:
        return None, proc.stdout + proc.stderr
    exports = dict(re.findall(r"^export (\w+)=(.*)$", env_file.read_text(), re.M))
    return exports, ""


@pytest.mark.parametrize("bucket_type", ["regional", "zonal", "", "bogus"])
def test_cloudbuild_ci_mode_ignores_bucket_type(tmp_path, bucket_type):
    # run-ci.sh picks each case's bucket type, so ci bucket names must not claim one.
    exports, err = _init_variables(tmp_path, _MODE="ci", _BUCKET_TYPE=bucket_type)
    assert exports is not None, err
    assert exports["BUCKET_PREFIX"] == "gcsfs-ci-ssb-ci-62b31d50"


def test_cloudbuild_nightly_mode_names_and_checks_the_bucket_type(tmp_path):
    exports, _ = _init_variables(tmp_path, _BUCKET_TYPE="zonal")
    assert exports["BUCKET_PREFIX"] == "gcsfs-ci-ssb-zonal-62b31d50"
    exports, err = _init_variables(tmp_path, _BUCKET_TYPE="")
    assert exports is None
    assert "unknown _BUCKET_TYPE" in err


def test_cloudbuild_reaps_leaked_ci_and_nightly_case_buckets_only():
    script = _cloudbuild_step_script("cleanup-leaked-resources")
    pattern = re.search(r"=~ (\^\$\{_INFRA_PREFIX\}\S+) \]\]", script).group(1)
    reaped = re.compile(pattern.replace("${_INFRA_PREFIX}", "gcsfs-ci-ssb")).match
    assert reaped("gcsfs-ci-ssb-ci-62b31d50-load-gpt2-ddp-ws2-zon-5782f6cc")
    assert reaped("gcsfs-ci-ssb-zonal-62b31d50-read-hf-txpq-a2bbe381")
    assert reaped("gcsfs-ci-ssb-standard_rcu_z-62b31d50-read-hf-txpq-a2bbe381")
    assert reaped("gcsfs-ci-ssb-hns_rcu_warm-62b31d50-load-gpt2-dcp-5782f6cc")
    assert not reaped("gcsfs-ci-ssb-run-results")
    assert not reaped("gcsfs-ci-ssb-models")


def _run_step_with_fake_gcloud(tmp_path, step_id, build_vars, gcloud_body=""):
    """Runs a cloud-sdk step against a fake gcloud (and a no-op sleep).

    The step must succeed. Returns the gcloud calls in order.
    """
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "build_vars.env").write_text(
        "".join(f"export {k}={v}\n" for k, v in build_vars.items())
    )
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "gcloud_calls"
    calls.touch()
    (bin_dir / "gcloud").write_text(
        f'#!/bin/bash\nGCLOUD_CALLS={calls}\necho "$*" >> "$GCLOUD_CALLS"\n'
        f"{gcloud_body}\n"
    )
    (bin_dir / "gcloud").chmod(0o755)
    (bin_dir / "sleep").write_text("#!/bin/sh\n")
    (bin_dir / "sleep").chmod(0o755)
    script = (
        _cloudbuild_step_script(step_id)
        .replace("$$", "$")
        .replace("/workspace/", f"{workspace}/")
    )
    env = {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        # Cloud Build substitutions, which bash sees as plain variables here.
        "PROJECT_ID": "proj",
        "_INFRA_PREFIX": "gcsfs-ci-ssb",
    }
    proc = subprocess.run(
        ["bash", "-c", script], env=env, capture_output=True, text=True
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not (workspace / "FAILED").exists(), proc.stdout + proc.stderr
    return calls.read_text().splitlines()


_LEAKED_RCU_BUCKET = "gcsfs-ci-ssb-hns_rcu_cold-62b31d50-read-hf-x-a2bbe381"
# A bucket listing line (as `gcloud storage ls` and `buckets list` print it) and a
# running cache on it.
_FAKE_RCU_GCLOUD = f"""
case "$*" in
  "storage ls"*) echo "gs://{_LEAKED_RCU_BUCKET}/";;
  "storage buckets list"*) echo "gs://{_LEAKED_RCU_BUCKET}/ 2026-01-01T00:00:00+00:00";;
  "alpha storage buckets rapid-caches list"*)
    printf '{_LEAKED_RCU_BUCKET}/us-central1-b\\trunning\\n';;
esac
"""


@pytest.mark.parametrize("step_id", ["delete-buckets", "cleanup-leaked-resources"])
def test_cloudbuild_disables_rcu_caches_before_removing_their_bucket(tmp_path, step_id):
    calls = _run_step_with_fake_gcloud(
        tmp_path,
        step_id,
        {
            "BUCKET_PREFIX": "gcsfs-ci-ssb-hns_rcu_cold-62b31d50",
            "RESULTS_BUCKET": "gcsfs-ci-ssb-run-results",
        },
        _FAKE_RCU_GCLOUD,
    )
    disables = [
        i
        for i, c in enumerate(calls)
        if c.startswith(
            f"alpha storage buckets rapid-caches disable {_LEAKED_RCU_BUCKET}/us-central1-b"
        )
    ]
    removals = [
        i
        for i, c in enumerate(calls)
        if c.startswith(f"storage rm --recursive gs://{_LEAKED_RCU_BUCKET}")
        or c.startswith(f"storage buckets delete gs://{_LEAKED_RCU_BUCKET}")
    ]
    assert len(disables) == 1, calls
    assert removals and disables[0] < min(removals), calls


@pytest.mark.parametrize(
    "mode, root", [("ci", "subsystembenchmarks-ci"), ("nightly", "subsystembenchmarks")]
)
def test_upload_results_keeps_ci_artifacts_out_of_bigquery_ingestion(
    tmp_path, mode, root
):
    # The ingestion external table reads gs://<prefix>-run-results/subsystembenchmarks/*.csv.
    home = tmp_path / "home"
    (home / "gcsfs/gcsfs/tests/perf/subsystembenchmarks/__run__/ci").mkdir(parents=True)
    (home / "ssb_cloudbuild.env").write_text(
        f"export MODE={mode}\nexport RESULTS_BUCKET=b-run-results\n"
        "export BRANCH_NAME=feat-x\nexport RUN_ID=build-1\n"
    )
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "gcloud_calls"
    (bin_dir / "gcloud").write_text(f'#!/bin/sh\necho "$@" >> {calls}\n')
    (bin_dir / "gcloud").chmod(0o755)
    script = os.path.join(
        _REPO_ROOT, "cloudbuild", "subsystembenchmarks", "scripts", "upload-results.sh"
    )
    env = dict(os.environ, HOME=str(home), PATH=f"{bin_dir}:{os.environ['PATH']}")

    subprocess.run(["bash", script], env=env, check=True, capture_output=True)

    dest = calls.read_text().split()[-1]
    assert re.fullmatch(
        rf"gs://b-run-results/{root}/branch=feat-x/\d{{8}}/build-1/", dest
    )
