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
    save_cases = [c for c in cases if c.scenario == "checkpoint_write"]
    load_cases = [c for c in cases if c.scenario == "checkpoint_read"]
    assert len(save_cases) == 6
    assert len(load_cases) == 9
    for c in save_cases:
        assert c.name.startswith("save-")
    for c in load_cases:
        assert c.name.startswith("load-")


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
    assert len(mp_cases) == 5
    for c in mp_cases:
        if c.sweep_axis != "cross_size":
            assert c.tensor_parallel_size == 4
            assert c.data_parallel_size == 2
            assert c.world_size == 8
            assert "tp4dp2" in c.name


def test_cross_size_cases():
    cases = _cases()
    cross_cases = [c for c in cases if c.sweep_axis == "cross_size"]
    assert len(cross_cases) == 3
    for c in cross_cases:
        assert c.scenario == "checkpoint_read"
        assert c.setup_world_size != c.world_size or (
            c.setup_tensor_parallel_size != c.tensor_parallel_size
        )
