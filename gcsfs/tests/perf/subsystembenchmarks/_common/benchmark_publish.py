"""Benchmark publishing helpers + pytest-benchmark hooks shared by workload suites.

Kept in a plain module (not a conftest) so non-test code can import it without depending on
pytest's conftest loading. The subsystembenchmarks conftest re-exports the hook functions so
pytest registers them.
"""

import os
import statistics

from gcsfs.tests.perf.subsystembenchmarks._common import env


def pytest_benchmark_update_machine_info(config, machine_info):
    """Stamp the general environment facts attached to benchmark output."""
    machine_info["compute_accelerator_type"] = env.detect_accelerator()
    machine_info["distributed_backend"] = env.detect_backend()
    machine_info["gpu_model"] = env.gpu_model()
    machine_info["machine_type"] = env.machine_type()
    machine_info["python_version"] = env.python_version()
    machine_info["group"] = os.environ.get("GCSFS_SUBSYSTEM_GROUP", "")


def publish_round_stats(benchmark, round_durations):
    if not round_durations:
        return
    benchmark.extra_info["runs"] = round_durations
    benchmark.extra_info["min_run"] = min(round_durations)
    benchmark.extra_info["max_run"] = max(round_durations)
    benchmark.extra_info["mean_run"] = statistics.mean(round_durations)
    benchmark.extra_info["median_run"] = statistics.median(round_durations)
    benchmark.extra_info["stddev_run"] = (
        statistics.stdev(round_durations) if len(round_durations) > 1 else 0.0
    )
    benchmark.extra_info["measurement_round_count"] = len(round_durations)


def publish_resource_metrics(benchmark, monitor):
    """Resource columns named like macrobenchmarks (process-tree/host scope, not pod scope)."""
    duration = getattr(monitor, "duration", 0.0)
    benchmark.extra_info.update(
        {
            # max_cpu is percent normalized by vCPUs; convert it back to cores.
            "cpu_usage_peak_cores": (monitor.max_cpu * monitor.vcpus / 100.0),
            "memory_usage_peak_bytes": int(monitor.max_mem),
            "network_received_mean_bytes_per_sec": (
                monitor.net_recv / duration if duration > 0 else 0.0
            ),
            "network_sent_mean_bytes_per_sec": (
                monitor.net_sent / duration if duration > 0 else 0.0
            ),
            "host_vcpu_count": monitor.vcpus,
        }
    )


def pytest_benchmark_generate_json(config, benchmarks, machine_info, commit_info):
    """Map self-computed round stats onto pytest-benchmark's exported stats fields."""
    for bench in benchmarks:
        if "runs" in bench.get("extra_info", {}):
            bench.stats.data = bench.extra_info["runs"]
            bench.stats.min = bench.extra_info["min_run"]
            bench.stats.max = bench.extra_info["max_run"]
            bench.stats.mean = bench.extra_info["mean_run"]
            bench.stats.median = bench.extra_info["median_run"]
            bench.stats.stddev = bench.extra_info["stddev_run"]
            bench.stats.rounds = bench.extra_info["measurement_round_count"]
            for k in (
                "runs",
                "min_run",
                "max_run",
                "mean_run",
                "median_run",
                "stddev_run",
            ):
                del bench.extra_info[k]
