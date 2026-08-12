from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning import configs
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.configs import (
    PyTorchLightningCheckpointConfigurator,
)

CONFIG = configs.__file__


def _cases():
    return PyTorchLightningCheckpointConfigurator(CONFIG).generate_cases()


def test_case_ids_unique_and_named():
    cases = _cases()
    assert len(cases) == len({c.name for c in cases})
    assert all(c.name.startswith("save-") for c in cases)
    assert all(c.scenario == "checkpoint_write" for c in cases)


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
