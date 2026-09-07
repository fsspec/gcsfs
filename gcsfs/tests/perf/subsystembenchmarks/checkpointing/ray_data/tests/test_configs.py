from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_data import configs
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_data.configs import (
    RayCheckpointConfigurator,
)

CONFIG = configs.__file__


def _cases():
    return RayCheckpointConfigurator(CONFIG).generate_cases()


def test_case_ids_unique_and_named():
    cases = _cases()
    names = [c.name for c in cases]
    assert len(names) == len(set(names))
    for c in cases:
        assert c.name.startswith("save-")
        assert c.scenario == "checkpoint_write"


def test_default_model_id():
    cases = _cases()
    assert all(
        c.model_id == "gs://huggingface-model-weights/Llama-3.1-8B" for c in cases
    )


def test_model_id_override(monkeypatch):
    monkeypatch.setenv("GCSFS_SUBSYSTEM_MODEL_ID", "custom-model-id")
    cases = _cases()
    assert all(c.model_id == "custom-model-id" for c in cases)
    assert all("custom_model_id" in c.name for c in cases)


def test_expected_strategies_present():
    cases = _cases()
    strategies = {c.strategy for c in cases}
    expected = {
        "single",
        "ddp",
        "fsdp_sharded",
        "fsdp_full",
        "model_parallel_sharded",
        "model_parallel_full",
    }
    assert expected.issubset(strategies)


def test_model_parallel_topology():
    cases = _cases()
    mp_cases = [c for c in cases if c.strategy.startswith("model_parallel")]
    assert len(mp_cases) == 2
    for c in mp_cases:
        assert c.tensor_parallel_size == 4
        assert c.data_parallel_size == 2
        assert c.world_size == 8
        assert "tp4dp2" in c.name
