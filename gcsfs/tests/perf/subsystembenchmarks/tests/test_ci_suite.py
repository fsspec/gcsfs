"""Guardrails for the per-PR CI suite (cloudbuild/subsystembenchmarks/scripts/run-ci.sh).

The CI suite exists to catch framework-lifecycle bugs (#1009, #1025, #1031, #1104), so its
shrunken configs must keep the lifecycle features those bugs needed.
"""

import importlib

import pytest

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
