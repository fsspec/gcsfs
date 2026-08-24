import asyncio
import logging
import os
import time
from typing import Optional

from google.cloud.storage.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
)

from gcsfs.extended_gcsfs import ExtendedGcsFileSystem
from gcsfs.zb_hns_utils import (
    MRDPool,
    MRDPoolCache,
    _close_mrds,
    _drain_queue,
)

logger = logging.getLogger("gcsfs.dummy_io")


class DummyDelaySimulator:
    """Simulates network latency (TTFB in ms) and streaming bandwidth transfer delay."""

    def __init__(
        self,
        ttfb_ms: Optional[float] = None,
        bandwidth_mbps: Optional[float] = None,
    ):
        if ttfb_ms is None:
            env_ttfb = os.getenv("GCSFS_DUMMY_IO_TTFB_MS", "0")
            try:
                self.ttfb_ms = float(env_ttfb)
            except ValueError:
                self.ttfb_ms = 0.0
        else:
            self.ttfb_ms = float(ttfb_ms)

        if bandwidth_mbps is None:
            env_bw = os.getenv("GCSFS_DUMMY_IO_BANDWIDTH_MBPS", "")
            try:
                self.bandwidth_mbps = float(env_bw) if env_bw else None
            except ValueError:
                self.bandwidth_mbps = None
        else:
            self.bandwidth_mbps = float(bandwidth_mbps) if bandwidth_mbps else None

    def calculate_delay(self, size: int = 0, is_first_chunk: bool = False) -> float:
        delay = 0.0
        if is_first_chunk and self.ttfb_ms > 0:
            delay += self.ttfb_ms / 1000.0
        if self.bandwidth_mbps and self.bandwidth_mbps > 0 and size > 0:
            delay += size / (self.bandwidth_mbps * 1024 * 1024)
        return delay

    async def async_delay(self, size: int = 0, is_first_chunk: bool = False):
        delay = self.calculate_delay(size, is_first_chunk)
        if delay > 0:
            await asyncio.sleep(delay)

    def sync_delay(self, size: int = 0, is_first_chunk: bool = False):
        delay = self.calculate_delay(size, is_first_chunk)
        if delay > 0:
            time.sleep(delay)


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
        info = self.details or await self.gcsfs._info(
            f"{self.bucket_name}/{self.object_name}", generation=self.generation
        )
        size = info.get("size", 0) if info else 0
        return DummyMRD(
            self.object_name, size, delay_simulator=self.gcsfs.delay_simulator
        )


class DummyMRDPoolCache(MRDPoolCache):
    """Subclasses production MRDPoolCache to construct DummyMRDPool instances."""

    async def get(self, bucket_name, object_name, generation=None, pool_size=1):
        if self._closed:
            raise RuntimeError("MRDPoolCache is closed.")
        fs = self._gcsfs()
        if fs is None:
            raise RuntimeError("ExtendedGcsFileSystem has been garbage collected.")

        info = await fs._info(f"{bucket_name}/{object_name}", generation=generation)
        if generation is None:
            generation = info.get("generation")
        key = (bucket_name, object_name, generation)
        finalized = info.get("timeFinalized") is not None

        self._incref(key)
        mrd_pool = DummyMRDPool(
            fs,
            bucket_name,
            object_name,
            generation,
            finalized,
            pool_size,
            cache=self,
        )
        if info is not None:
            mrd_pool.details = info

        try:
            await mrd_pool.initialize()
        except BaseException:
            await mrd_pool.close()
            mrds_to_close = []
            if key not in self._refcounts:
                self._evictable_keys.pop(key, None)
                mrds_to_close = _drain_queue(self._mrd_queues.pop(key, None))
            await _close_mrds(mrds_to_close, raise_exception=False)
            raise

        return mrd_pool


class DummyGcsFileSystem(ExtendedGcsFileSystem):
    """
    Subclass of ExtendedGcsFileSystem that bypasses network data transfer only on the read path.
    """

    def __init__(
        self,
        *args,
        dummy_io_ttfb_ms: Optional[float] = None,
        dummy_io_bandwidth_mbps: Optional[float] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.delay_simulator = DummyDelaySimulator(
            ttfb_ms=dummy_io_ttfb_ms,
            bandwidth_mbps=dummy_io_bandwidth_mbps,
        )
        self._mrd_pool_cache = DummyMRDPoolCache(
            self,
            max_idle_pools=kwargs.get("max_mrd_pool_cache_idle_pools", 16),
            max_queue_size=kwargs.get("max_mrd_pool_cache_queue_size", 8),
        )

    async def _cat_file_sequential(self, path, start=None, end=None, **kwargs):
        """Simulate sequential cat file without network data transfer."""
        if start is not None and end is not None and start >= end >= 0:
            return b""

        if start is not None and end is not None and start >= 0 and end >= start:
            length = end - start
        else:
            offset, length = await self._process_limits_to_offset_and_length(
                path, start, end, file_size=kwargs.get("file_size")
            )

        if length <= 0:
            return b""

        await self.delay_simulator.async_delay(size=length, is_first_chunk=True)
        return bytes(length)
