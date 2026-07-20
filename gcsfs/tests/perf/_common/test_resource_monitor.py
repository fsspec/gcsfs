import multiprocessing
import time

from gcsfs.tests.perf._common.resource_monitor import ResourceMonitor


def test_default_interval_captures_short_lived_workers():
    assert ResourceMonitor().interval == 0.1


def _burn_cpu(duration):
    deadline = time.perf_counter() + duration
    value = 1
    while time.perf_counter() < deadline:
        value = (value * 3 + 1) % 1_000_003


def test_resource_monitor_observes_a_short_spawned_worker():
    ctx = multiprocessing.get_context("spawn")
    with ResourceMonitor() as monitor:
        child = ctx.Process(target=_burn_cpu, args=(0.5,))
        child.start()
        # Poll until the child process is tracked or timeout is reached
        timeout = 2.0
        start_poll = time.perf_counter()
        while time.perf_counter() - start_poll < timeout:
            if any(pid == child.pid for pid, _created in monitor._procs):
                break
            time.sleep(0.05)
        else:
            raise AssertionError(
                "Child process was not tracked by ResourceMonitor within timeout"
            )
        child.join()

    assert child.exitcode == 0
    assert monitor.max_cpu > 0.0


def test_resource_monitor_duration():
    """
    Test that the resource monitor accurately measures duration of the monitored block,
    without including thread shutdown latency.
    """
    with ResourceMonitor(interval=1.0) as m:
        time.sleep(0.05)

    assert m.duration < 0.2
    assert m.duration >= 0.05
