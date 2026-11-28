import pytest

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator


@pytest.fixture(autouse=True)
def _pin_cpu_device(monkeypatch):
    """Keep CPU unit tests isolated on dev machines with GPUs or TPUs."""
    monkeypatch.setenv(accelerator.DEVICE_ENV, "cpu")
    accelerator.device_kind.cache_clear()
    yield
    accelerator.device_kind.cache_clear()


@pytest.fixture(scope="session")
def tiny_models(tmp_path_factory):
    """Tiny 8-layer Llama and 2-layer, 16-expert OLMoE checkpoints on disk."""
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.tests.tiny import (
        tiny_model_dir,
    )

    return {
        "llama": tiny_model_dir(tmp_path_factory.mktemp("tiny_llama"), num_layers=8),
        "olmoe": tiny_model_dir(
            tmp_path_factory.mktemp("tiny_olmoe"), kind="olmoe", num_layers=2
        ),
    }
