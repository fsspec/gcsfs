"""Telemetry detectors for extracting caller context tokens."""

from __future__ import annotations

import asyncio
import concurrent.futures
import os
import sys
import threading
import weakref
from typing import Dict, Optional

import aiohttp
import fsspec.asyn

from gcsfs.telemetry.context import Dimension

ENV_GCE = "gce"
ENV_GKE = "gke"
ENV_OTHER = "oth"

_TRUTHY_VALUES = ("1", "true", "yes")
_GCP_PRODUCT_NAMES = ("Google", "Google Compute Engine")
_DMI_PRODUCT_NAME_PATH = "/sys/class/dmi/id/product_name"
_K8S_SERVICE_ACCOUNT_PATH = "/var/run/secrets/kubernetes.io/serviceaccount"
_METADATA_HOST_ENV_VARS = ("GCE_METADATA_HOST", "GCE_METADATA_ROOT", "GCE_METADATA_IP")
_DEFAULT_METADATA_HOST = "169.254.169.254"
_CLUSTER_NAME_PATH = "/computeMetadata/v1/instance/attributes/cluster-name"
_METADATA_FLAVOR_HEADER = "Metadata-Flavor"
_METADATA_FLAVOR_VALUE = "Google"
_PROBE_TIMEOUT_SECONDS = 0.5
_PROBE_RETRY_DELAYS_SECONDS = (0.5, 1.0, 2.0, 4.0, 8.0)
_SERVERLESS_ENV_VARS = (
    "K_SERVICE",
    "K_CONFIGURATION",
    "K_REVISION",
    "CLOUD_RUN_JOB",
    "CLOUD_RUN_EXECUTION",
    "CLOUD_RUN_WORKER_POOL",
    "FUNCTION_TARGET",
    "FUNCTION_NAME",
    "GAE_SERVICE",
    "GAE_VERSION",
    "GAE_INSTANCE",
    "GAE_ENV",
)
# Only errors time can fix; others hit _probe_coro's local fallback at once.
_PROBE_RETRYABLE_ERRORS = (aiohttp.ClientConnectionError, asyncio.TimeoutError)


def _env_flag_enabled(name: str) -> bool:
    return os.environ.get(name, "").lower() in _TRUTHY_VALUES


def _read_linux_product_name() -> Optional[str]:
    try:
        with open(_DMI_PRODUCT_NAME_PATH, encoding="utf-8", errors="replace") as f:
            return f.read()
    except OSError:
        return None


_backoff_sleep = asyncio.sleep


def _new_probe_session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        trust_env=False, timeout=aiohttp.ClientTimeout(total=_PROBE_TIMEOUT_SECONDS)
    )


_live_detectors: weakref.WeakSet[EnvironmentDetector] = weakref.WeakSet()


def _reset_detectors_after_fork() -> None:
    for detector in list(_live_detectors):
        detector._reset_after_fork()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_detectors_after_fork)


class BaseDetector:
    """Base interface for extracting telemetry tokens."""

    name: Dimension | str = ""

    def detect(self) -> Optional[str]:
        """
        Execute detection logic and return a bare telemetry token value.

        Returns
        -------
        Optional[str]: Bare token value (e.g. 'pandas') or None if not detected.
        """
        raise NotImplementedError


class FrameworkDetector(BaseDetector):
    """
    Detects the top-most initiating framework name on the call stack.

    Traverses frames from innermost (gcsfs) to outermost (closest to __main__),
    retaining the outermost match to attribute high-level orchestrators (e.g., Dask, Ray)
    over lower-level serialization layers (PyArrow).
    """

    name = Dimension.FRAMEWORK

    # Canonical mapping of top-level package name -> standard brand token
    KNOWN_FRAMEWORKS: Dict[str, str] = {
        # Data & Analytics
        "pandas": "pandas",
        "duckdb": "duckdb",
        "pyspark": "spark",
        "dask": "dask",
        "ray": "ray",
        "pyarrow": "pyarrow",
        "xarray": "xarray",
        "fastparquet": "fastparquet",
        "polars": "polars",
        # ML & Deep Learning
        "torch": "torch",
        "torchdata": "torchdata",
        "lightning": "lightning",
        "pytorch_lightning": "lightning",
        "datasets": "datasets",
        "transformers": "transformers",
        "orbax": "orbax",
        "tensorstore": "tensorstore",
        "tensorflow": "tensorflow",
        "jax": "jax",
    }

    def __init__(self, max_depth: int = 64):
        self.max_depth = max_depth

    def detect(self) -> Optional[str]:
        try:
            start_frame = sys._getframe()
        except (ValueError, AttributeError, RuntimeError, PermissionError):
            return None

        try:
            return self._walk_stack(start_frame)
        finally:
            del start_frame

    def _walk_stack(self, start_frame) -> Optional[str]:
        """
        Traverses the frame chain in a single pass from innermost (gcsfs) to outermost (root).
        Returns the outermost recognized framework encountered.
        """
        frame = start_frame
        depth = 0
        detected_framework = None

        try:
            while frame is not None and depth < self.max_depth:
                f_globals = getattr(frame, "f_globals", None) or {}
                mod_name = f_globals.get("__name__", "")
                if mod_name:
                    top_pkg = mod_name.partition(".")[0]
                    if top_pkg in self.KNOWN_FRAMEWORKS:
                        detected_framework = self.KNOWN_FRAMEWORKS[top_pkg]
                frame = frame.f_back
                depth += 1
        finally:
            del frame
            del start_frame

        return detected_framework


class EnvironmentDetector(BaseDetector):
    """
    Detects the compute substrate the process runs on.

    Returns 'gce' on Google Compute Engine VMs, 'gke' on Google Kubernetes
    Engine pods, 'oth' everywhere else (including GCP serverless runtimes), and
    None while unknown. ``detect()`` never blocks: when the metadata server must be
    consulted, it schedules a background probe on an event loop and returns None
    until the probe resolves.
    """

    name = Dimension.ENVIRONMENT

    def __init__(self):
        self._lock = threading.Lock()
        self._cached_env: Optional[str] = None
        self._probe_exhausted = False
        self._probe_in_flight = False
        self._probe_task: Optional[asyncio.Task] = None
        self._probe_future: Optional[concurrent.futures.Future] = None
        self._probe_generation = 0
        self._gcp_bios: Optional[bool] = None
        _live_detectors.add(self)

    def detect(self) -> Optional[str]:
        cached_env = self._cached_env
        if cached_env is not None:
            return cached_env

        if self._has_serverless_env() and not self._is_kubernetes():
            return self._cache_verdict(ENV_OTHER)

        if self._probe_exhausted:
            return None

        if _env_flag_enabled("NO_GCE_CHECK"):
            return self._resolve_from_local_signals()

        if not self._is_gcp_bios() and not self._has_metadata_override():
            return self._cache_verdict(ENV_OTHER)

        self._schedule_probe()
        return None

    def _reset_after_fork(self) -> None:
        self._lock = threading.Lock()
        self._probe_in_flight = False
        self._probe_task = None
        self._probe_future = None
        self._probe_generation += 1

    @staticmethod
    def _has_serverless_env() -> bool:
        return any(os.environ.get(name) for name in _SERVERLESS_ENV_VARS)

    @staticmethod
    def _is_kubernetes() -> bool:
        return bool(os.environ.get("KUBERNETES_SERVICE_HOST")) or os.path.exists(
            _K8S_SERVICE_ACCOUNT_PATH
        )

    @staticmethod
    def _has_metadata_override() -> bool:
        return any(os.environ.get(name) for name in _METADATA_HOST_ENV_VARS)

    @staticmethod
    def _metadata_host() -> str:
        for name in _METADATA_HOST_ENV_VARS:
            host = os.environ.get(name)
            if host:
                return host
        return _DEFAULT_METADATA_HOST

    def _is_gcp_bios(self) -> bool:
        if self._gcp_bios is None:
            self._gcp_bios = self._read_gcp_bios()
        return self._gcp_bios

    @staticmethod
    def _read_gcp_bios() -> bool:
        if not sys.platform.startswith("linux"):
            return False
        product_name = _read_linux_product_name()
        return product_name is not None and product_name.strip() in _GCP_PRODUCT_NAMES

    def _local_fallback_verdict(self) -> Optional[str]:
        if not self._is_gcp_bios():
            return ENV_OTHER
        if not self._is_kubernetes():
            return ENV_GCE
        return None

    def _cache_verdict(self, verdict: str) -> str:
        with self._lock:
            self._cached_env = verdict
        return verdict

    def _resolve_from_local_signals(self) -> Optional[str]:
        verdict = self._local_fallback_verdict()
        with self._lock:
            if verdict is None:
                self._probe_exhausted = True
            else:
                self._cached_env = verdict
        return verdict

    def _probe_handle_is_stale(self) -> bool:
        if self._probe_task is not None:
            task_loop = self._probe_task.get_loop()
            return (
                self._probe_task.done()
                or task_loop.is_closed()
                or not task_loop.is_running()
            )
        if self._probe_future is not None:
            return self._probe_future.done()
        return True

    def _schedule_probe(self) -> None:
        with self._lock:
            if self._cached_env is not None or self._probe_exhausted:
                return
            if self._probe_in_flight and not self._probe_handle_is_stale():
                return

            self._probe_generation += 1
            probe = self._probe_coro(self._probe_generation)
            try:
                running_loop = asyncio.get_running_loop()
            except RuntimeError:
                running_loop = None

            fsspec_loop = fsspec.asyn.loop[0]
            if running_loop is not None and (
                fsspec_loop is None or running_loop is fsspec_loop
            ):
                self._probe_task = running_loop.create_task(probe)
                self._probe_future = None
            else:
                self._probe_future = asyncio.run_coroutine_threadsafe(
                    probe, fsspec.asyn.get_loop()
                )
                self._probe_task = None
            self._probe_in_flight = True

    async def _probe_coro(self, generation: int) -> None:
        verdict: Optional[str] = None
        resolved = False
        closed_by_loop_teardown = False
        try:
            verdict = await self._query_metadata_server()
            if verdict is None:
                verdict = self._local_fallback_verdict()
            resolved = True
        except GeneratorExit:
            # GC can close() us under self._lock; locking here would deadlock.
            closed_by_loop_teardown = True
            raise
        except Exception:
            verdict = self._local_fallback_verdict()
            resolved = True
        finally:
            if not closed_by_loop_teardown:
                with self._lock:
                    if generation == self._probe_generation:
                        if resolved and verdict is not None:
                            self._cached_env = verdict
                        elif resolved:
                            self._probe_exhausted = True
                        self._probe_in_flight = False
                        self._probe_task = None
                        self._probe_future = None

    async def _query_metadata_server(self) -> Optional[str]:
        url = f"http://{self._metadata_host()}{_CLUSTER_NAME_PATH}"
        for delay in _PROBE_RETRY_DELAYS_SECONDS:
            verdict = await self._probe_metadata_server_once(url)
            if verdict is not None:
                return verdict
            await _backoff_sleep(delay)
        return await self._probe_metadata_server_once(url)

    @staticmethod
    async def _probe_metadata_server_once(url: str) -> Optional[str]:
        headers = {_METADATA_FLAVOR_HEADER: _METADATA_FLAVOR_VALUE}
        try:
            async with _new_probe_session() as session:
                async with session.get(url, headers=headers) as response:
                    from_google_server = (
                        response.headers.get(_METADATA_FLAVOR_HEADER)
                        == _METADATA_FLAVOR_VALUE
                    )
                    if not from_google_server:
                        return None
                    if response.status == 200:
                        cluster_name = await response.text()
                        return ENV_GKE if cluster_name.strip() else None
                    if response.status == 404:
                        return ENV_GCE
                    return None
        except _PROBE_RETRYABLE_ERRORS:
            return None
