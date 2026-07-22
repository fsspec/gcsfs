import types

from gcsfs.tests.perf.subsystembenchmarks._common import benchmark_publish


def test_publish_resource_metrics_uses_macrobenchmark_metric_names():
    benchmark = types.SimpleNamespace(extra_info={})
    monitor = types.SimpleNamespace(
        max_cpu=50.0,
        max_mem=1024.0,
        net_recv=400.0,
        net_sent=200.0,
        duration=2.0,
        vcpus=8,
    )

    benchmark_publish.publish_resource_metrics(benchmark, monitor)

    assert benchmark.extra_info == {
        "cpu_usage_peak_cores": 4.0,
        "memory_usage_peak_bytes": 1024,
        "network_received_mean_bytes_per_sec": 200.0,
        "network_sent_mean_bytes_per_sec": 100.0,
        "host_vcpu_count": 8,
    }
