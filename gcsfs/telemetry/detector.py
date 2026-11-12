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
# GKE Sandbox (gVisor) exposes this sysfs entry from the host, unlike sys_vendor.
_DMI_PRODUCT_NAME_PATH = "/sys/class/dmi/id/product_name"
_WINDOWS_HARDWARE_CONFIG_KEY = r"SYSTEM\HardwareConfig\Current"
_K8S_SERVICE_ACCOUNT_PATH = "/var/run/secrets/kubernetes.io/serviceaccount"
_METADATA_HOST_ENV_VARS = ("GCE_METADATA_HOST", "GCE_METADATA_ROOT", "GCE_METADATA_IP")
_DEFAULT_METADATA_HOST = "169.254.169.254"
_CLUSTER_NAME_PATH = "/computeMetadata/v1/instance/attributes/cluster-name"
_METADATA_FLAVOR_HEADER = "Metadata-Flavor"
_METADATA_FLAVOR_VALUE = "Google"
_PROBE_TIMEOUT_SECONDS = 0.5
_PROBE_RETRY_DELAYS_SECONDS = (0.5, 1.0, 2.0, 4.0, 8.0)
_SERVERLESS_ENV_VAR_PAIRS = (
    ("K_SERVICE", "K_REVISION"),
    ("CLOUD_RUN_JOB", "CLOUD_RUN_EXECUTION"),
    ("CLOUD_RUN_WORKER_POOL", "CLOUD_RUN_REVISION"),
    ("GAE_SERVICE", "GAE_INSTANCE"),
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


def _read_windows_product_name() -> Optional[str]:
    import winreg

    try:
        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE, _WINDOWS_HARDWARE_CONFIG_KEY
        ) as key:
            product_name, _ = winreg.QueryValueEx(key, "SystemProductName")
    except OSError:
        return None
    return product_name


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
    call_scoped: bool = True

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
    consulted, it schedules a background probe on the ``fsspec`` IO loop and
    returns None until the probe resolves.
    """

    name = Dimension.ENVIRONMENT
    call_scoped = False

    def __init__(self):
        self._lock = threading.Lock()
        self._cached_env: Optional[str] = None
        self._probe_in_flight = False
        self._probe_future: Optional[concurrent.futures.Future] = None
        self._probe_generation = 0
        self._gcp_bios: Optional[bool] = None
        self._k8s: Optional[bool] = None
        self._serverless: Optional[bool] = None
        self._no_gce_check: Optional[bool] = None
        self._metadata_override: Optional[bool] = None
        _live_detectors.add(self)

    def detect(self) -> Optional[str]:
        cached_env = self._cached_env
        if cached_env is not None:
            return cached_env
        if self._probe_in_flight:
            return None
        return self._detect_slow()

    def _detect_slow(self) -> Optional[str]:
        cached_env = self._cached_env
        if cached_env is not None:
            return cached_env

        if self._has_serverless_env() and not self._is_kubernetes():
            return self._cache_verdict(ENV_OTHER)

        if self._is_no_gce_check():
            return self._cache_verdict(self._local_fallback_verdict())

        if not self._is_gcp_bios() and not self._has_metadata_override():
            return self._cache_verdict(ENV_OTHER)

        # Resolve filesystem cluster presence on the caller before background work.
        self._is_kubernetes()
        if self._schedule_probe():
            return None
        return self._cached_env

    def _reset_after_fork(self) -> None:
        self._lock = threading.Lock()
        self._probe_in_flight = False
        self._probe_future = None
        self._probe_generation += 1

    def _has_serverless_env(self) -> bool:
        if self._serverless is None:
            self._serverless = any(
                bool(os.environ.get(primary)) and bool(os.environ.get(secondary))
                for primary, secondary in _SERVERLESS_ENV_VAR_PAIRS
            )
        return self._serverless

    def _is_kubernetes(self) -> bool:
        if self._k8s is None:
            self._k8s = bool(os.environ.get("KUBERNETES_SERVICE_HOST")) or (
                os.path.exists(_K8S_SERVICE_ACCOUNT_PATH)
            )
        return self._k8s

    def _is_no_gce_check(self) -> bool:
        if self._no_gce_check is None:
            self._no_gce_check = _env_flag_enabled("NO_GCE_CHECK")
        return self._no_gce_check

    def _has_metadata_override(self) -> bool:
        if self._metadata_override is None:
            self._metadata_override = any(
                os.environ.get(name) for name in _METADATA_HOST_ENV_VARS
            )
        return self._metadata_override

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
        if sys.platform.startswith("linux"):
            product_name = _read_linux_product_name()
        elif sys.platform == "win32":
            product_name = _read_windows_product_name()
        else:
            return False
        return (
            isinstance(product_name, str) and product_name.strip() in _GCP_PRODUCT_NAMES
        )

    def _local_fallback_verdict(self) -> str:
        if not self._is_gcp_bios():
            return ENV_OTHER
        if self._is_kubernetes():
            return ENV_GKE
        return ENV_GCE

    def _cache_verdict(self, verdict: str) -> str:
        with self._lock:
            self._cached_env = verdict
        return verdict

    def _schedule_probe(self) -> bool:
        with self._lock:
            if self._cached_env is not None or self._probe_in_flight:
                return False
            self._probe_generation += 1
            generation = self._probe_generation
            self._probe_in_flight = True
            self._probe_future = None

        probe = self._probe_coro(generation)
        try:
            loop = fsspec.asyn.get_loop()
            future = asyncio.run_coroutine_threadsafe(probe, loop)
        except BaseException:
            probe.close()
            with self._lock:
                if self._probe_generation == generation:
                    self._probe_in_flight = False
            raise

        with self._lock:
            if self._probe_generation == generation and self._probe_in_flight:
                self._probe_future = future

        def _clear_if_cancelled(_fut: concurrent.futures.Future) -> None:
            if generation == self._probe_generation and self._probe_in_flight:
                with self._lock:
                    if generation == self._probe_generation:
                        self._probe_in_flight = False
                        self._probe_future = None

        future.add_done_callback(_clear_if_cancelled)
        return True

    async def _probe_coro(self, generation: int) -> None:
        verdict: Optional[str] = None
        resolved = False
        try:
            verdict = await self._query_metadata_server()
            if verdict is None:
                verdict = self._local_fallback_verdict()
            resolved = True
        except Exception:
            verdict = self._local_fallback_verdict()
            resolved = True
        finally:
            if generation == self._probe_generation:
                with self._lock:
                    if generation == self._probe_generation:
                        if resolved:
                            self._cached_env = verdict
                        self._probe_in_flight = False
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
                        return ENV_GKE if cluster_name.strip() else ENV_GCE
                    if response.status == 404:
                        return ENV_GCE
                    return None
        except _PROBE_RETRYABLE_ERRORS:
            return None
