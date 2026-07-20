import threading
import time

import psutil


class ResourceMonitor:
    def __init__(self, interval=0.1):
        self.interval = interval

        self.vcpus = psutil.cpu_count() or 1
        self.max_cpu = 0.0
        self.max_mem = 0.0

        # Network and Time tracking
        self.start_time = 0.0
        self.duration = 0.0
        self.start_net = None
        self.net_sent = 0.0
        self.net_recv = 0.0

        self._procs = {}
        self._stop_event = threading.Event()
        self._thread = None

    def __enter__(self):
        self.start_net = psutil.net_io_counters()
        self.start_time = time.perf_counter()
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.duration = time.perf_counter() - self.start_time
        end_net = psutil.net_io_counters()

        self.net_sent = end_net.bytes_sent - self.start_net.bytes_sent
        self.net_recv = end_net.bytes_recv - self.start_net.bytes_recv
        self.stop()

    def _tracked(self, proc):
        live_procs = {}
        try:
            all_procs = [proc] + proc.children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            all_procs = [proc]
        for p in all_procs:
            try:
                proc_key = (p.pid, p.create_time())
                live_procs[proc_key] = self._procs.get(proc_key, p)
            except psutil.NoSuchProcess:
                continue
        self._procs = live_procs
        return list(self._procs.values())

    def _monitor(self):
        current_process = psutil.Process()
        current_process.cpu_percent(interval=None)

        while not self._stop_event.is_set():
            try:
                # CPU and Memory tracking for current process tree
                total_cpu = 0.0
                current_mem = 0.0

                for proc_obj in self._tracked(current_process):
                    try:
                        current_mem += proc_obj.memory_info().rss
                        total_cpu += proc_obj.cpu_percent(interval=None)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue

                # Normalize CPU by number of vcpus
                global_cpu = total_cpu / self.vcpus

                if global_cpu > self.max_cpu:
                    self.max_cpu = global_cpu
                if current_mem > self.max_mem:
                    self.max_mem = current_mem
            except psutil.NoSuchProcess:
                pass

            self._stop_event.wait(self.interval)

    def start(self):
        self._thread = threading.Thread(target=self._monitor, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join()

    @property
    def throughput_s(self):
        """Calculates combined network throughput."""
        if self.duration <= 0:
            return 0.0
        return (self.net_sent + self.net_recv) / self.duration
