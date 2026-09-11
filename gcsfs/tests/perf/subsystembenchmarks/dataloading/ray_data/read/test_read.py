import os

import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data import configs

pytest.importorskip("ray")
pytest.importorskip("torch")
pytest.importorskip("pyarrow")

pytestmark = pytest.mark.skipif(
    not os.environ.get("GCSFS_SUBSYSTEM_BUCKET_PREFIX"),
    reason="the read benchmarks create a bucket per case; CI-only (run.py exports the prefix)",
)

CASES = configs.RayDataReadConfigurator(configs.__file__).generate_cases()


@pytest.mark.parametrize("params", CASES, ids=lambda p: p.name)
def test_read(benchmark, params, monitor):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.read.driver import (
        RayDataReadDriver,
    )
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.read_case import run_read_case

    run_read_case(benchmark, monitor, params, RayDataReadDriver())
