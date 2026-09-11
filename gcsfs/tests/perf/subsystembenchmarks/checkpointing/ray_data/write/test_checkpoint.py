import os

import pytest

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_data import configs
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_data.configs import (
    RayCheckpointConfigurator,
)

pytest.importorskip("ray")
pytest.importorskip("torch")

pytestmark = pytest.mark.skipif(
    not os.environ.get("GCSFS_SUBSYSTEM_BUCKET_PREFIX"),
    reason="the checkpoint benchmarks create a bucket per case; CI-only (run.py exports the prefix)",
)

CASES = [
    c
    for c in RayCheckpointConfigurator(configs.__file__).generate_cases()
    if c.scenario == "checkpoint_write"
]


@pytest.mark.timeout(7200)
@pytest.mark.parametrize("params", CASES, ids=lambda p: p.name)
def test_checkpoint_save(benchmark, params, monitor):
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.checkpoint_case import (
        run_checkpoint_case,
    )
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_data.write.driver import (
        RayCheckpointWriteDriver,
    )

    run_checkpoint_case(benchmark, monitor, params, RayCheckpointWriteDriver())
