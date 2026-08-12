import os

import pytest

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning import configs
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.configs import (
    PyTorchLightningCheckpointConfigurator,
)

pytest.importorskip("lightning")
pytest.importorskip("torch")

pytestmark = pytest.mark.skipif(
    not os.environ.get("GCSFS_SUBSYSTEM_BUCKET_PREFIX"),
    reason="the checkpoint benchmarks create a bucket per case; CI-only (run.py exports the prefix)",
)

CASES = [
    c
    for c in PyTorchLightningCheckpointConfigurator(configs.__file__).generate_cases()
    if c.scenario == "checkpoint_read"
]


@pytest.mark.timeout(7200)
@pytest.mark.parametrize("params", CASES, ids=lambda p: p.name)
def test_checkpoint_load(benchmark, params, monitor):
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.checkpoint_case import (
        run_checkpoint_case,
    )
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.read.driver import (
        PLCheckpointReadDriver,
    )

    run_checkpoint_case(benchmark, monitor, params, PLCheckpointReadDriver())
