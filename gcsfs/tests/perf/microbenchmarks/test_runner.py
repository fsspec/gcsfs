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


def test_run_multi_threaded_fixed_duration(mock_benchmark, mock_monitor):
    params = MockParams(threads=2, rounds=3)
    params.runtime = 30
    # 2 threads and 3 rounds means worker_func is called 6 times in total (2 calls per round).
    # Since worker_func returns sequential values:
    # - Round 1: calls return 10 and 20 (Sum = 30)
    # - Round 2: calls return 30 and 40 (Sum = 70)
    # - Round 3: calls return 50 and 60 (Sum = 110)
    worker_func = mock.Mock(side_effect=[10, 20, 30, 40, 50, 60])
    args_list = [(1,), (2,)]

    runner.run_multi_threaded_fixed_duration(
        mock_benchmark, mock_monitor, params, worker_func, args_list, "read"
    )

    assert mock_benchmark.extra_info["threads"] == 2
    assert mock_benchmark.group == "read"
    # Verify that the sum of returns matches the expected values for each of the 3 rounds
    assert mock_benchmark.extra_info["runs"] == [30, 70, 110]
    assert mock_benchmark.extra_info["min_run"] == 30
    assert mock_benchmark.extra_info["max_run"] == 110

    mock_monitor.assert_called_once()
    assert worker_func.call_count == 6
    mock_benchmark.pedantic.assert_called_once_with(
        mock.ANY, rounds=1, iterations=1, warmup_rounds=0
    )


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
    mock_process.exitcode = 0
    mock_ctx.Process.return_value = mock_process

    # Mock shared array

    class MockArray:
        def __setitem__(self, idx, val):
            pass

        def __getitem__(self, idx):
            if isinstance(idx, slice):
                return [1.0, 2.0]
            return 1.0

    mock_array = MockArray()
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

    mock_mp.get_context.assert_called_with("forkserver")
    assert extended_gcs_factory.call_count == 2

    # Check process creation and start
    assert mock_ctx.Process.call_count == 4  # 2 processes * 2 rounds
    assert mock_process.start.call_count == 4
    assert mock_process.join.call_count == 4

    # Check runs in extra_info
    assert mock_benchmark.extra_info["runs"] == [2.0, 2.0]
    assert mock_benchmark.extra_info["min_run"] == 2.0


@mock.patch("gcsfs.tests.perf.microbenchmarks.runner.multiprocessing")
def test_run_multi_process_child_fails(mock_mp, mock_benchmark, mock_monitor):
    params = MockParams(processes=2, rounds=1)
    extended_gcs_factory = mock.Mock(return_value="gcs_instance")
    worker_target = mock.Mock()
    args_builder = mock.Mock(return_value=("arg1",))

    mock_ctx = mock.Mock()
    mock_mp.get_context.return_value = mock_ctx

    mock_process1 = mock.Mock()
    mock_process1.exitcode = 1
    mock_process1.is_alive.return_value = False

    mock_process2 = mock.Mock()
    mock_process2.exitcode = None
    mock_process2.is_alive.return_value = True

    mock_ctx.Process.side_effect = [mock_process1, mock_process2]

    class MockArray:
        def __setitem__(self, idx, val):
            pass

        def __getitem__(self, idx):
            if isinstance(idx, slice):
                return [1.0, 2.0]
            return 1.0

    mock_array = MockArray()
    mock_ctx.Array.return_value = mock_array

    with pytest.raises(RuntimeError, match="Worker process 0 exited with code 1"):
        runner.run_multi_process(
            mock_benchmark,
            mock_monitor,
            params,
            extended_gcs_factory,
            worker_target,
            args_builder,
            "listing",
        )

    mock_process1.join.assert_called_once()
    mock_process1.terminate.assert_not_called()
    mock_process2.terminate.assert_called_once()
    mock_process2.join.assert_called_once()


@mock.patch("gcsfs.tests.perf.microbenchmarks.runner.multiprocessing")
def test_run_multi_process_resets_shared_data(mock_mp, mock_benchmark, mock_monitor):
    params = MockParams(processes=2, rounds=2)
    extended_gcs_factory = mock.Mock(return_value="gcs_instance")
    worker_target = mock.Mock()
    args_builder = mock.Mock(return_value=("arg1",))

    mock_ctx = mock.Mock()
    mock_mp.get_context.return_value = mock_ctx
    mock_process = mock.Mock()
    mock_process.exitcode = 0
    mock_ctx.Process.return_value = mock_process

    # Use a real list or dict to simulate the array so we can assert it was reset
    shared_data = {}

    class MockArray:
        def __setitem__(self, idx, val):
            shared_data[idx] = val

        def __getitem__(self, idx):
            if isinstance(idx, slice):
                return [shared_data.get(0, 0.0), shared_data.get(1, 0.0)]
            return shared_data[idx]

    mock_array = MockArray()
    mock_ctx.Array.return_value = mock_array

    # Set some initial non-zero data
    shared_data[0] = 10.0
    shared_data[1] = 20.0

    runner.run_multi_process(
        mock_benchmark,
        mock_monitor,
        params,
        extended_gcs_factory,
        worker_target,
        args_builder,
        "listing",
    )

    # If it was reset to 0 at the start of each round, and the worker target is a mock
    # that doesn't actually run (since we mocked Process), the final results appended
    # will be 0.0, because the array was reset and never populated by the non-running mock process.
    assert mock_benchmark.extra_info["runs"] == [0.0, 0.0]
