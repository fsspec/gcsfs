import os

import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading.huggingface_datasets import (
    configs,
)

pytest.importorskip("datasets")
pytest.importorskip("torch")
pytest.importorskip("pyarrow")

pytestmark = pytest.mark.skipif(
    not os.environ.get("GCSFS_SUBSYSTEM_BUCKET_PREFIX"),
    reason="the read benchmarks create a bucket per case; CI-only (run.py exports the prefix)",
)

CASES = configs.HuggingFaceReadConfigurator(configs.__file__).generate_cases()


@pytest.mark.parametrize("params", CASES, ids=lambda p: p.name)
def test_read(benchmark, params, monitor):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.huggingface_datasets.read.driver import (
        HFReadDriver,
    )
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.read_case import run_read_case

    run_read_case(benchmark, monitor, params, HFReadDriver())
