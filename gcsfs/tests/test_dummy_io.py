import subprocess
import sys
import time
from unittest import mock

import pytest

from gcsfs.core import GCSFile, GCSFileSystem
from gcsfs.dummy_gcsfs import DummyDelaySimulator, DummyGcsFileSystem
from gcsfs.extended_gcsfs import BucketType
from gcsfs.zonal_file import ZonalFile


def test_dummy_io_env_toggle_import():
    """Verify that ENABLE_GCSFS_DUMMY_IO=true cleanly exposes DummyGcsFileSystem as GCSFileSystem."""
    code = (
        "import os\n"
        "os.environ['ENABLE_GCSFS_DUMMY_IO'] = 'true'\n"
        "import gcsfs\n"
        "from gcsfs.dummy_gcsfs import DummyGcsFileSystem\n"
        "assert gcsfs.GCSFileSystem is DummyGcsFileSystem\n"
    )
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr


@pytest.mark.asyncio
async def test_dummy_delay_simulator(monkeypatch):
    """Verify TTFB and bandwidth delay calculations and execution."""
    monkeypatch.setenv("GCSFS_DUMMY_IO_TTFB_MS", "50")
    monkeypatch.setenv("GCSFS_DUMMY_IO_BANDWIDTH_MBPS", "100")
    sim = DummyDelaySimulator()
    assert sim.ttfb_ms == 50.0
    assert sim.bandwidth_mbps == 100.0

    # 50ms TTFB + 10MB / 100MB/s (100ms) = 150ms
    delay = sim.calculate_delay(size=10 * 1024 * 1024, is_first_chunk=True)
    assert pytest.approx(delay, rel=1e-3) == 0.15

    # Async delay execution
    start = time.perf_counter()
    await sim.async_delay(size=1024, is_first_chunk=False)
    assert time.perf_counter() - start >= 0.0


def test_dummy_zonal_read():
    """Verify DummyMRD read path through production MRDPool, MRDPoolCache, and ZonalFile."""
    fs = DummyGcsFileSystem()
    dummy_info = {"size": 10000, "name": "my-zonal-bucket/file.dat", "type": "file"}
    with mock.patch.object(
        fs, "_sync_lookup_bucket_type", return_value=BucketType.ZONAL_HIERARCHICAL
    ):
        with mock.patch.object(
            GCSFileSystem, "_info", new_callable=mock.AsyncMock, return_value=dummy_info
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


def test_dummy_sequential_read():
    """Verify standard (non-zonal) dummy read path through GCSFile and _cat_file_sequential."""
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
