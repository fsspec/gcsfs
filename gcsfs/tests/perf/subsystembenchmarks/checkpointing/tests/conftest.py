import pytest

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator


@pytest.fixture(autouse=True)
def _pin_cpu_device(monkeypatch):
    """Keep CPU unit tests isolated on dev machines with GPUs or TPUs."""
    monkeypatch.setenv(accelerator.DEVICE_ENV, "cpu")
    accelerator.reset_cache()
    yield
    accelerator.reset_cache()
