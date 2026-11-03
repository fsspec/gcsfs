"""Guardrails for the per-PR CI suite (cloudbuild/subsystembenchmarks/scripts/run-ci.sh).

The CI suite exists to catch framework-lifecycle bugs (#1009, #1025, #1031, #1104), so its
shrunken configs must keep the lifecycle features those bugs needed.
"""

import importlib
import os
import subprocess

import pytest

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
    monkeypatch.delenv("GCSFS_SUBSYSTEM_SWEEP_AXES", raising=False)
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
    for key in (
        "GCSFS_SUBSYSTEM_SWEEP_AXES",
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


def test_upload_results_works_without_nightly_venv():
    path = os.path.join(
        _REPO_ROOT, "cloudbuild", "subsystembenchmarks", "scripts", "upload-results.sh"
    )
    with open(path) as f:
        text = f.read()
    assert "if [[ -f env/bin/activate ]]; then" in text
