import multiprocessing

import pytest

from gcsfs.tests.perf._common.resource_monitor import ResourceMonitor
from gcsfs.tests.perf.subsystembenchmarks._common.benchmark_publish import (  # noqa: F401
    publish_resource_metrics,
    publish_round_stats,
    pytest_benchmark_generate_json,
    pytest_benchmark_update_machine_info,
)

# ResourceMonitor runs a sampler thread before DataLoader workers start. Force the safe
# start method before any benchmark creates workers; forking the threaded pytest process can
# inherit locked interpreter/library state and deadlock.
multiprocessing.set_start_method("spawn", force=True)


@pytest.fixture
def monitor():
    return ResourceMonitor
