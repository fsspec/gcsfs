import os
import threading
import time
from types import SimpleNamespace
from unittest import mock

import psutil

from gcsfs.tests.perf._common.resource_monitor import ResourceMonitor


def test_resource_monitor_tracks_explicit_pid_tree(monkeypatch):
    seen_pids = []
    real_process = psutil.Process

    class _SpyProcess(real_process):
        def __init__(self, pid=None):
            seen_pids.append(pid)
            super().__init__(pid)

    monkeypatch.setattr(psutil, "Process", _SpyProcess)

    my_pid = os.getpid()
    with ResourceMonitor(interval=0.02, pid=my_pid) as monitor:
        time.sleep(0.08)

    assert seen_pids and seen_pids[0] == my_pid
    assert monitor.max_mem > 0


def test_resource_monitor_handles_exited_children_and_missing_root_pid(monkeypatch):
    target_pid = 42424
    child_pid = 42425
    sample_count = 0
    done = threading.Event()

    child_proc = mock.MagicMock()
    child_proc.pid = child_pid
    child_proc.create_time.return_value = 2.0
    child_proc.memory_info.side_effect = [
        SimpleNamespace(rss=2000),
        psutil.NoSuchProcess(child_pid),
    ]
    child_proc.cpu_percent.return_value = 40.0

    root_proc = mock.MagicMock()
    root_proc.pid = target_pid
    root_proc.create_time.return_value = 1.0
    root_proc.is_running.return_value = True

    def root_children(recursive=True):
        nonlocal sample_count
        sample_count += 1
        if sample_count == 1:
            return [child_proc]
        if sample_count == 2:
            return [child_proc]
        done.set()
        raise psutil.NoSuchProcess(target_pid)

    root_proc.children.side_effect = root_children
    root_proc.memory_info.return_value = SimpleNamespace(rss=1000)
    root_proc.cpu_percent.side_effect = [None, 20.0, 20.0]

    monkeypatch.setattr(psutil, "Process", lambda pid=None: root_proc)

    monitor = ResourceMonitor(interval=0.01, pid=target_pid)
    monitor.vcpus = 2
    monitor.start()
    assert done.wait(timeout=2.0), "monitor did not reach root exit"
    monitor._thread.join(timeout=2.0)
    assert not monitor._thread.is_alive()
    monitor.stop()

    # Sample 1 recorded root (1000) + child (2000) = 3000 bytes and (20 + 40) / 2 = 30% CPU.
    assert monitor.max_mem == 3000
    assert monitor.max_cpu == 30.0


def test_resource_monitor_handles_missing_root_on_start(monkeypatch):
    def missing_process(pid=None):
        raise psutil.NoSuchProcess(pid)

    monkeypatch.setattr(psutil, "Process", missing_process)

    monitor = ResourceMonitor(interval=0.01, pid=999999)
    monitor.start()
    monitor._thread.join(timeout=2.0)
    assert not monitor._thread.is_alive()
    monitor.stop()
    assert monitor.max_mem == 0.0
    assert monitor.max_cpu == 0.0


def test_resource_monitor_handles_access_denied_on_root_and_children(monkeypatch):
    root_proc = mock.MagicMock()
    root_proc.pid = 111
    root_proc.create_time.return_value = 1.0
    root_proc.is_running.return_value = True
    root_proc.children.side_effect = psutil.AccessDenied(111)
    root_proc.memory_info.return_value = SimpleNamespace(rss=500)
    root_proc.cpu_percent.side_effect = [None, 10.0, psutil.AccessDenied(111)]

    monkeypatch.setattr(psutil, "Process", lambda pid=None: root_proc)

    monitor = ResourceMonitor(interval=0.01, pid=111)
    monitor.vcpus = 1
    monitor.start()
    monitor._thread.join(timeout=2.0)
    assert not monitor._thread.is_alive()
    monitor.stop()

    assert monitor.max_mem == 500
    assert monitor.max_cpu == 10.0
