import time

from gcsfs.tests.perf.microbenchmarks.resource_monitor import ResourceMonitor


def test_resource_monitor_duration():
    """
    Test that the resource monitor accurately measures duration of the monitored block,
    without including thread shutdown latency.
    """
    # Use a long interval so that time.sleep inside the monitor thread would have
    # added significant latency if wait() wasn't used or stop() was called before duration measurement.
    with ResourceMonitor(interval=1.0) as m:
        time.sleep(0.05)

    # We slept for 0.05s. Due to Python overhead, let's bound it at 0.2s.
    # Prior to the fix, this would be ~1.05s because stop() waited for the time.sleep(1.0) to finish
    # BEFORE duration was recorded.
    assert m.duration < 0.2
    assert m.duration >= 0.05
