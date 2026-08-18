import pytest

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning import configs
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.configs import (
    PyTorchLightningCheckpointConfigurator,
)

CONFIG = configs.__file__


def _cases():
    return PyTorchLightningCheckpointConfigurator(CONFIG).generate_cases()


def test_case_ids_unique_and_named():
    cases = _cases()
    for c in cases:
        if c.scenario == "checkpoint_write":
            assert c.name.startswith("save-")
        else:
            assert c.name.startswith("load-")
    assert {c.scenario for c in cases} == {"checkpoint_write", "checkpoint_read"}


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


def test_cross_topology_cases():
    cases = _cases()
    # Check that cross_size variants got parsed correctly
    cross_size_cases = [
        c for c in cases if getattr(c, "sweep_axis", None) == "cross_size"
    ]
    assert len(cross_size_cases) >= 3, "Expected at least 3 cross_size variants"

    # Check specific variant: fsdp_sharded 4 -> 8
    fsdp_down_up = next(
        (
            c
            for c in cross_size_cases
            if c.strategy == "fsdp_sharded"
            and c.setup_world_size == 4
            and c.world_size == 8
        ),
        None,
    )
    assert fsdp_down_up is not None
    assert fsdp_down_up.setup_tensor_parallel_size == 1  # default

    # Check specific variant: model_parallel_sharded
    mp_case = next(
        (c for c in cross_size_cases if c.strategy == "model_parallel_sharded"), None
    )
    assert mp_case is not None
    assert mp_case.setup_world_size == 8
    assert mp_case.setup_tensor_parallel_size == 4
    assert mp_case.setup_data_parallel_size == 2
    assert mp_case.world_size == 4
    assert mp_case.tensor_parallel_size == 2
    assert mp_case.data_parallel_size == 2


def test_is_distributed_strategy():
    pytest.importorskip("lightning")
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.common import (
        is_distributed_strategy,
    )

    assert not is_distributed_strategy("single")
    assert is_distributed_strategy("ddp")
    assert is_distributed_strategy("fsdp_sharded")
    assert is_distributed_strategy("fsdp_full")
    assert is_distributed_strategy("model_parallel_full")
    assert is_distributed_strategy("model_parallel_sharded")
