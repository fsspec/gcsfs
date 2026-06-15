import threading
import time

import psutil


class ResourceMonitor:
    def __init__(self, interval=1.0):
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

    def _monitor(self):
        current_process = psutil.Process()
        known_procs = {
            (current_process.pid, current_process.create_time()): current_process
        }
        current_process.cpu_percent(interval=None)

        while not self._stop_event.is_set():
            try:
                # CPU and Memory tracking for current process tree
                all_procs = [current_process] + current_process.children(recursive=True)

                total_cpu = 0.0
                current_mem = 0.0
                alive_keys = set()

                for p in all_procs:
                    try:
                        current_mem += p.memory_info().rss

                        # CPU: Track processes across loops to properly calculate cpu_percent
                        proc_key = (p.pid, p.create_time())
                        alive_keys.add(proc_key)

                        if proc_key not in known_procs:
                            known_procs[proc_key] = p
                            # Prime newly discovered process. As per psutil docs:
                            # "the first time this is called it will return a meaningless
                            # 0.0 value which you are supposed to ignore."
                            p.cpu_percent(interval=None)
                        else:
                            total_cpu += known_procs[proc_key].cpu_percent(
                                interval=None
                            )
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue

                # Remove dead processes from tracking dictionary
                known_procs = {
                    k: proc for k, proc in known_procs.items() if k in alive_keys
                }

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
