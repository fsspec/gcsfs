import unittest.mock as mock

import pytest

from gcsfs.tests.perf.microbenchmarks import runner


class MockParams:
    def __init__(self, threads=1, processes=1, rounds=1):
        self.threads = threads
        self.processes = processes
        self.rounds = rounds
        self.files = 1
        self.file_size_bytes = 100
        self.chunk_size_bytes = 10
        self.block_size_bytes = 10
        self.pattern = "seq"
        self.bucket_name = "test-bucket"
        self.bucket_type = "regional"
        self.name = "test-benchmark"


@pytest.fixture
def mock_benchmark():
    benchmark = mock.Mock()
    benchmark.extra_info = {}
    return benchmark


@pytest.fixture
def mock_monitor():
    monitor = mock.Mock()
    monitor.__enter__ = mock.Mock(return_value=monitor)
    monitor.__exit__ = mock.Mock(return_value=None)
    monitor.max_cpu = 10.0
    monitor.max_mem = 100.0
    monitor.throughput_s = 50.0
    monitor.vcpus = 2
    return mock.Mock(return_value=monitor)


def test_filter_test_cases():
    case1 = MockParams(threads=1, processes=1)
    case2 = MockParams(threads=2, processes=1)
    case3 = MockParams(threads=1, processes=2)

    all_cases = [case1, case2, case3]
    st, mt, mp = runner.filter_test_cases(all_cases)

    assert st == [case1]
    assert mt == [case2]
    assert mp == [case3]


def test_run_single_threaded(mock_benchmark, mock_monitor):
    params = MockParams(rounds=5)
    func = mock.Mock()
    args = (1, 2, 3)

    runner.run_single_threaded(mock_benchmark, mock_monitor, params, func, args, "read")

    assert mock_benchmark.extra_info["threads"] == 1
    assert mock_benchmark.extra_info["processes"] == 1
    assert mock_benchmark.group == "read"

    mock_monitor.assert_called_once()
    mock_benchmark.pedantic.assert_called_once_with(func, rounds=5, args=args)

    assert mock_benchmark.extra_info["cpu_max_global"] == "10.00"


def test_run_multi_threaded(mock_benchmark, mock_monitor):
    params = MockParams(threads=2, rounds=3)
    worker_func = mock.Mock()
    args_list = [(1,), (2,)]

    runner.run_multi_threaded(
        mock_benchmark, mock_monitor, params, worker_func, args_list, "write"
    )

    assert mock_benchmark.extra_info["threads"] == 2
    assert mock_benchmark.group == "write"

    mock_monitor.assert_called_once()
    mock_benchmark.pedantic.assert_called_once()

    # Extract workload and run it to verify it calls worker_func
    args, kwargs = mock_benchmark.pedantic.call_args
    workload = args[0]
    assert callable(workload)

    workload()
    assert worker_func.call_count == 2


@mock.patch("gcsfs.tests.perf.microbenchmarks.runner.multiprocessing")
def test_run_multi_process(mock_mp, mock_benchmark, mock_monitor):
    params = MockParams(processes=2, rounds=2)
    extended_gcs_factory = mock.Mock(return_value="gcs_instance")
    worker_target = mock.Mock()
    args_builder = mock.Mock(return_value=("arg1",))

    # Mock multiprocessing context and process
    mock_ctx = mock.Mock()
    mock_mp.get_context.return_value = mock_ctx
    mock_process = mock.Mock()
    mock_ctx.Process.return_value = mock_process

    # Mock shared array
    mock_array = mock.Mock()
    # Simulate durations for 2 processes
    mock_array.__getitem__ = mock.Mock(return_value=[1.0, 2.0])
    mock_ctx.Array.return_value = mock_array

    runner.run_multi_process(
        mock_benchmark,
        mock_monitor,
        params,
        extended_gcs_factory,
        worker_target,
        args_builder,
        "listing",
    )

    assert mock_benchmark.extra_info["processes"] == 2
    assert mock_benchmark.group == "listing"

    mock_mp.get_context.assert_called_with("spawn")
    assert extended_gcs_factory.call_count == 2

    # Check process creation and start
    assert mock_ctx.Process.call_count == 4  # 2 processes * 2 rounds
    assert mock_process.start.call_count == 4
    assert mock_process.join.call_count == 4

    # Check timings in extra_info
    assert mock_benchmark.extra_info["timings"] == [2.0, 2.0]
    assert mock_benchmark.extra_info["min_time"] == 2.0
