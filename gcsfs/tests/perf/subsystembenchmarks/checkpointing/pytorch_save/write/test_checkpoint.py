import os

import pytest

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.configs import (
    case_params,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_save import configs
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_save.configs import (
    PyTorchSaveConfigurator,
)

pytest.importorskip("torch")

pytestmark = pytest.mark.skipif(
    not os.environ.get("GCSFS_SUBSYSTEM_BUCKET_PREFIX"),
    reason="the checkpoint benchmarks create a bucket per case; CI-only (run.py exports the prefix)",
)


@pytest.mark.timeout(7200)
@pytest.mark.parametrize(
    "params", case_params(PyTorchSaveConfigurator, configs.__file__)
)
def test_checkpoint_save(benchmark, params, monitor):
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.checkpoint_case import (
        run_checkpoint_case,
    )
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.write.driver import (
        PyTorchCheckpointWriteDriver,
    )

    run_checkpoint_case(benchmark, monitor, params, PyTorchCheckpointWriteDriver())
