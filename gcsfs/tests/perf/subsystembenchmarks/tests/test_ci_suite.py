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
            assert filter_expr in ("save", "load"), name
        else:
            assert filter_expr == "", name


def test_run_ci_dataloading_groups_have_ci_configs():
    ssb_root = os.path.dirname(run.__file__)
    for name, group, _, _ in _ci_cases():
        if group.startswith("dataloading/"):
            assert os.path.exists(
                os.path.join(ssb_root, group, "configs.ci.yaml")
            ), name


def test_run_ci_is_valid_bash():
    subprocess.run(["bash", "-n", _RUN_CI], check=True)

