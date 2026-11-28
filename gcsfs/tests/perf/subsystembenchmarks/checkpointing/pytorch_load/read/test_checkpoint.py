import os

import pytest

from gcsfs.tests.perf.subsystembenchmarks._common import placement
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_load import configs
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_load.configs import (
    PyTorchLoadConfigurator,
)

pytest.importorskip("torch")

pytestmark = pytest.mark.skipif(
    not os.environ.get("GCSFS_SUBSYSTEM_BUCKET_PREFIX"),
    reason="the checkpoint benchmarks create a bucket per case; CI-only (run.py exports the prefix)",
)


@pytest.mark.timeout(7200)
@pytest.mark.parametrize(
    "params",
    placement.select(
        PyTorchLoadConfigurator(configs.__file__).generate_cases(),
        group="checkpointing/pytorch_load",
    ),
)
def test_checkpoint_load(benchmark, params, monitor):
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.checkpoint_case import (
        run_checkpoint_case,
    )
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.read.driver import (
        PyTorchCheckpointReadDriver,
    )

    run_checkpoint_case(benchmark, monitor, params, PyTorchCheckpointReadDriver())
