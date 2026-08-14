import os

import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset import configs

pytest.importorskip("webdataset")
pytest.importorskip("torch")
pytest.importorskip("PIL")

pytestmark = pytest.mark.skipif(
    not os.environ.get("GCSFS_SUBSYSTEM_BUCKET_PREFIX"),
    reason="Requires GCSFS_SUBSYSTEM_BUCKET_PREFIX (CI only)",
)

CASES = configs.WebDatasetReadConfigurator(configs.__file__).generate_cases()


@pytest.mark.parametrize("params", CASES, ids=lambda p: p.name)
def test_read(benchmark, params, monitor):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.read_case import run_read_case
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset.read.driver import (
        WebDatasetReadDriver,
    )

    run_read_case(benchmark, monitor, params, WebDatasetReadDriver())
