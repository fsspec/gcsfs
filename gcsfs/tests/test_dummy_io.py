import asyncio
import os
import time
from typing import Optional
from unittest import mock

import pytest
from google.cloud.storage.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
)

from gcsfs.core import GCSFile, GCSFileSystem
from gcsfs.extended_gcsfs import BucketType, ExtendedGcsFileSystem
from gcsfs.zb_hns_utils import MRDPool
from gcsfs.zonal_file import ZonalFile


class DummyDelaySimulator:
    """Simulates network latency (TTFB in ms) and streaming bandwidth transfer delay (MB/s)."""

    def __init__(
        self,
        ttfb_ms: Optional[float] = None,
        bandwidth_mb_per_sec: Optional[float] = None,
    ):
        if ttfb_ms is None:
            env_ttfb = os.getenv("GCSFS_DUMMY_IO_TTFB_MS", "0")
            try:
                self.ttfb_ms = float(env_ttfb)
            except ValueError:
                self.ttfb_ms = 0.0
        else:
            self.ttfb_ms = float(ttfb_ms)

        if bandwidth_mb_per_sec is None:
            env_bw = os.getenv("GCSFS_DUMMY_IO_BANDWIDTH_MB_PER_SEC", "")
            try:
                self.bandwidth_mb_per_sec = float(env_bw) if env_bw else None
            except ValueError:
                self.bandwidth_mb_per_sec = None
        else:
            self.bandwidth_mb_per_sec = (
                float(bandwidth_mb_per_sec) if bandwidth_mb_per_sec else None
            )

    def calculate_delay(self, size: int = 0, is_first_chunk: bool = False) -> float:
        delay = 0.0
        if is_first_chunk and self.ttfb_ms > 0:
            delay += self.ttfb_ms / 1000.0
        if self.bandwidth_mb_per_sec and self.bandwidth_mb_per_sec > 0 and size > 0:
            delay += size / (self.bandwidth_mb_per_sec * 1024 * 1024)
        return delay

    async def async_delay(self, size: int = 0, is_first_chunk: bool = False):
        delay = self.calculate_delay(size, is_first_chunk)
        if delay > 0:
            await asyncio.sleep(delay)


class DummyMRD(AsyncMultiRangeDownloader):
    """Mock AsyncMultiRangeDownloader yielding zeroed data without network data transfer."""

    def __init__(
        self,
        object_name: str,
        persisted_size: int,
        delay_simulator: Optional[DummyDelaySimulator] = None,
    ):
        self.object_name = object_name
        self.persisted_size = persisted_size
        self._delay_simulator = delay_simulator

    async def download_ranges(self, ranges):
        for idx, (offset, length, buf) in enumerate(ranges):
            if self._delay_simulator:
                await self._delay_simulator.async_delay(
                    size=length, is_first_chunk=(idx == 0)
                )
            buf.write(bytes(length))

    async def close(self):
        pass


class DummyMRDPool(MRDPool):
    """Subclasses production MRDPool to create DummyMRD without gRPC connections."""

    async def _create_mrd(self):
        delay_sim = getattr(self.gcsfs, "delay_simulator", None)
        if delay_sim:
            await delay_sim.async_delay(is_first_chunk=True)
        info = self.details or await self.gcsfs._info(
            f"{self.bucket_name}/{self.object_name}", generation=self.generation
        )
        size = info.get("size", 0) if info else 0
        return DummyMRD(self.object_name, size, delay_simulator=delay_sim)


class DummyGcsFileSystem(ExtendedGcsFileSystem):
    """
    Subclass of ExtendedGcsFileSystem that bypasses network data transfer on the read path.
    """

    def __init__(
        self,
        *args,
        dummy_io_ttfb_ms: Optional[float] = None,
        dummy_io_bandwidth_mb_per_sec: Optional[float] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.delay_simulator = DummyDelaySimulator(
            ttfb_ms=dummy_io_ttfb_ms,
            bandwidth_mb_per_sec=dummy_io_bandwidth_mb_per_sec,
        )

    async def _cat_file_sequential(self, path, start=None, end=None, **kwargs):
        """Simulate sequential cat file without network data transfer."""
        offset, length = await self._process_limits_to_offset_and_length(
            path, start, end, file_size=kwargs.get("file_size")
        )
        if length <= 0:
            return b""

        await self.delay_simulator.async_delay(size=length, is_first_chunk=True)
        return bytes(length)


# --- Tests ---


@pytest.mark.asyncio
async def test_dummy_delay_simulator(monkeypatch):
    """Verify TTFB and bandwidth delay calculations and execution."""
    monkeypatch.setenv("GCSFS_DUMMY_IO_TTFB_MS", "50")
    monkeypatch.setenv("GCSFS_DUMMY_IO_BANDWIDTH_MB_PER_SEC", "100")
    sim = DummyDelaySimulator()
    assert sim.ttfb_ms == 50.0
    assert sim.bandwidth_mb_per_sec == 100.0

    # 50ms TTFB + 10MB / 100MB/s (100ms) = 150ms
    delay = sim.calculate_delay(size=10 * 1024 * 1024, is_first_chunk=True)
    assert pytest.approx(delay, rel=1e-3) == 0.15

    # Negative/zero size or non-first chunk TTFB behavior
    assert sim.calculate_delay(size=0, is_first_chunk=False) == 0.0
    assert sim.calculate_delay(size=10 * 1024 * 1024, is_first_chunk=False) == 0.1

    # Async delay execution
    start = time.perf_counter()
    await sim.async_delay(size=1024, is_first_chunk=False)
    assert time.perf_counter() - start >= 0.0


def test_dummy_delay_simulator_invalid_env(monkeypatch):
    """Verify fallback when environment variables contain invalid float values."""
    monkeypatch.setenv("GCSFS_DUMMY_IO_TTFB_MS", "invalid_ttfb")
    monkeypatch.setenv("GCSFS_DUMMY_IO_BANDWIDTH_MB_PER_SEC", "invalid_bw")
    sim = DummyDelaySimulator()
    assert sim.ttfb_ms == 0.0
    assert sim.bandwidth_mb_per_sec is None


@pytest.mark.asyncio
async def test_dummy_mrd_close():
    """Verify DummyMRD close method executes cleanly."""
    mrd = DummyMRD("test_obj", 100)
    await mrd.close()


def test_dummy_zonal_read():
    """Verify DummyMRD read path through production MRDPoolCache, and ZonalFile."""
    fs = DummyGcsFileSystem()
    dummy_info = {"size": 10000, "name": "my-zonal-bucket/file.dat", "type": "file"}
    with mock.patch("gcsfs.zb_hns_utils.MRDPool", DummyMRDPool):
        with mock.patch.object(
            fs, "_sync_lookup_bucket_type", return_value=BucketType.ZONAL_HIERARCHICAL
        ):
            with mock.patch.object(
                GCSFileSystem,
                "_info",
                new_callable=mock.AsyncMock,
                return_value=dummy_info,
            ):
                with mock.patch.object(fs, "info", return_value=dummy_info):
                    with mock.patch.object(
                        fs,
                        "_is_zonal_bucket",
                        new_callable=mock.AsyncMock,
                        return_value=True,
                    ):
                        with fs.open("my-zonal-bucket/file.dat", "rb") as f:
                            assert isinstance(f, ZonalFile)
                            data = f.read(2048)
                            assert len(data) == 2048
                            assert data == bytes(2048)


def test_standard_read():
    """Verify standard dummy sequential read path through GCSFile and _cat_file_sequential."""
    fs = DummyGcsFileSystem()
    dummy_info = {"size": 5000, "name": "my-bucket/file.dat", "type": "file"}
    with mock.patch.object(
        fs, "_sync_lookup_bucket_type", return_value=BucketType.NON_HIERARCHICAL
    ):
        with mock.patch.object(
            GCSFileSystem, "_info", new_callable=mock.AsyncMock, return_value=dummy_info
        ):
            with mock.patch.object(fs, "info", return_value=dummy_info):
                with fs.open("my-bucket/file.dat", "rb") as f:
                    assert isinstance(f, GCSFile)
                    data = f.read(1000)
                    assert len(data) == 1000
                    assert data == bytes(1000)
