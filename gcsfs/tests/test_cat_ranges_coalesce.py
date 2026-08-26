"""Unit tests for cat_ranges adjacent range coalescing in GCSFileSystem and ExtendedGcsFileSystem."""

import io
from unittest import mock
import pytest

from gcsfs.core import GCSFileSystem, _coalesce_ranges
from gcsfs.extended_gcsfs import ExtendedGcsFileSystem


# =====================================================================
# 1. Unit tests for _coalesce_ranges helper
# =====================================================================

def test_coalesce_ranges_single_item():
    items = [(0, 10, 0)]
    merged = _coalesce_ranges(items, max_gap=5)
    assert len(merged) == 1
    m_s, m_e, slices = merged[0]
    assert m_s == 0
    assert m_e == 10
    assert slices == [(0, 0, 10)]


def test_coalesce_ranges_contiguous():
    # Gap is 0: [0, 10) and [10, 20)
    items = [(0, 10, 0), (10, 20, 1)]
    merged = _coalesce_ranges(items, max_gap=0)
    assert len(merged) == 1
    m_s, m_e, slices = merged[0]
    assert m_s == 0
    assert m_e == 20
    assert slices == [(0, 0, 10), (1, 10, 20)]


def test_coalesce_ranges_within_max_gap():
    # Gap is 5: [0, 10) and [15, 25) with max_gap=5 -> coalesced into [0, 25)
    items = [(0, 10, 0), (15, 25, 1)]
    merged = _coalesce_ranges(items, max_gap=5)
    assert len(merged) == 1
    m_s, m_e, slices = merged[0]
    assert m_s == 0
    assert m_e == 25
    assert slices == [(0, 0, 10), (1, 15, 25)]


def test_coalesce_ranges_exceeding_max_gap():
    # Gap is 6: [0, 10) and [16, 25) with max_gap=5 -> NOT coalesced
    items = [(0, 10, 0), (16, 25, 1)]
    merged = _coalesce_ranges(items, max_gap=5)
    assert len(merged) == 2
    assert merged[0] == (0, 10, [(0, 0, 10)])
    assert merged[1] == (16, 25, [(1, 0, 9)])


def test_coalesce_ranges_overlapping():
    # Overlapping: [0, 15) and [5, 20)
    items = [(0, 15, 0), (5, 20, 1)]
    merged = _coalesce_ranges(items, max_gap=0)
    assert len(merged) == 1
    m_s, m_e, slices = merged[0]
    assert m_s == 0
    assert m_e == 20
    assert slices == [(0, 0, 15), (1, 5, 20)]


def test_coalesce_ranges_unordered_inputs():
    # Unordered indices: index 2 at start 0, index 0 at start 20, index 1 at start 10
    items = [(20, 30, 0), (0, 10, 2), (10, 20, 1)]
    merged = _coalesce_ranges(items, max_gap=0)
    assert len(merged) == 1
    m_s, m_e, slices = merged[0]
    assert m_s == 0
    assert m_e == 30
    # Original indices are retained in slices
    assert slices == [(2, 0, 10), (1, 10, 20), (0, 20, 30)]


def test_coalesce_ranges_with_file_size_unbounded():
    # end=None with known file_size=100
    items = [(0, 50, 0), (60, None, 1)]
    merged = _coalesce_ranges(items, max_gap=10, file_size=100)
    assert len(merged) == 1
    assert merged[0] == (0, 100, [(0, 0, 50), (1, 60, 100)])


def test_coalesce_ranges_without_file_size_unbounded():
    # end=None without known file_size cannot coalesce past unbounded range
    items = [(0, None, 0), (10, 20, 1)]
    merged = _coalesce_ranges(items, max_gap=10, file_size=None)
    assert len(merged) == 2
    assert merged[0] == (0, None, [(0, 0, None)])
    assert merged[1] == (10, 20, [(1, 0, 10)])


def test_coalesce_ranges_multiple_clusters():
    # [0, 5) & [6, 11) -> cluster 1 [0, 11)
    # [20, 21) & [22, 30) -> cluster 2 [20, 30)
    items = [(0, 5, 0), (6, 11, 1), (20, 21, 2), (22, 30, 3)]
    merged = _coalesce_ranges(items, max_gap=5)
    assert len(merged) == 2
    assert merged[0] == (0, 11, [(0, 0, 5), (1, 6, 11)])
    assert merged[1] == (20, 30, [(2, 0, 1), (3, 2, 10)])


# =====================================================================
# 2. Unit tests for GCSFileSystem._cat_ranges
# =====================================================================

@pytest.mark.asyncio
async def test_gcsfs_cat_ranges_validation():
    fs = GCSFileSystem(token="anon")

    # Invalid paths type
    with pytest.raises(TypeError, match="paths must be a list"):
        await fs._cat_ranges("bucket/file.txt", starts=[0], ends=[10])

    # Length mismatch
    with pytest.raises(ValueError, match="same length"):
        await fs._cat_ranges(["bucket/file.txt"], starts=[0, 10], ends=[5])

    # Empty paths
    assert await fs._cat_ranges([], starts=[], ends=[]) == []


@pytest.mark.asyncio
async def test_gcsfs_cat_ranges_scalar_broadcast():
    fs = GCSFileSystem(token="anon")
    data = b"0123456789"

    async def mock_cat_file(path, start=None, end=None, **kwargs):
        s = start or 0
        e = end if end is not None else len(data)
        return data[s:e]

    with mock.patch.object(fs, "_cat_file", side_effect=mock_cat_file) as mock_cat:
        # Scalar starts=2 and ends=7 broadcasted across 2 paths
        res = await fs._cat_ranges(["b/f1", "b/f2"], starts=2, ends=7, max_gap=None)
        assert len(res) == 2
        assert bytes(res[0]) == b"23456"
        assert bytes(res[1]) == b"23456"
        assert mock_cat.call_count == 2


@pytest.mark.asyncio
async def test_gcsfs_cat_ranges_coalesced_single_file():
    fs = GCSFileSystem(token="anon")
    full_content = b"0123456789abcdefghijklmnopqrstuvwxyz"

    async def mock_cat_file(path, start=None, end=None, **kwargs):
        s = start or 0
        e = end if end is not None else len(full_content)
        return full_content[s:e]

    with mock.patch.object(fs, "_cat_file", side_effect=mock_cat_file) as mock_cat:
        paths = ["bucket/file.txt", "bucket/file.txt", "bucket/file.txt"]
        starts = [0, 10, 20]
        ends = [5, 15, 25]

        # max_gap=5 merges [0, 5), [10, 15), [20, 25) into a single read [0, 25)
        res = await fs._cat_ranges(paths, starts, ends, max_gap=5)

        assert len(res) == 3
        assert bytes(res[0]) == b"01234"
        assert bytes(res[1]) == b"abcde"
        assert bytes(res[2]) == b"klmno"
        # Only 1 merged _cat_file call for the single coalesced range [0, 25)
        assert mock_cat.call_count == 1
        mock_cat.assert_called_once_with("bucket/file.txt", start=0, end=25)


@pytest.mark.asyncio
async def test_gcsfs_cat_ranges_coalesced_multi_files_and_gaps():
    fs = GCSFileSystem(token="anon")
    file1_content = b"0123456789abcdefghijklmnopqrstuvwxyz"
    file2_content = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

    async def mock_cat_file(path, start=None, end=None, **kwargs):
        data = file1_content if path == "b/f1" else file2_content
        s = start or 0
        e = end if end is not None else len(data)
        return data[s:e]

    with mock.patch.object(fs, "_cat_file", side_effect=mock_cat_file) as mock_cat:
        # Interleaved requests between f1 and f2
        paths = ["b/f1", "b/f2", "b/f1", "b/f2", "b/f1"]
        starts = [0,      0,      6,      5,      30]
        ends =   [5,      4,      10,     9,      35]

        # max_gap=5:
        # b/f1 ranges: [0, 5) & [6, 10) merged into [0, 10); [30, 35) separate
        # b/f2 ranges: [0, 4) & [5, 9) merged into [0, 9)
        res = await fs._cat_ranges(paths, starts, ends, max_gap=5)

        assert len(res) == 5
        assert bytes(res[0]) == b"01234"       # f1 [0, 5)
        assert bytes(res[1]) == b"ABCD"        # f2 [0, 4)
        assert bytes(res[2]) == b"6789"        # f1 [6, 10)
        assert bytes(res[3]) == b"FGHI"        # f2 [5, 9)
        assert bytes(res[4]) == b"uvwxy"       # f1 [30, 35)

        # 3 merged calls: f1 [0, 10), f1 [30, 35), f2 [0, 9)
        assert mock_cat.call_count == 3


@pytest.mark.asyncio
async def test_gcsfs_cat_ranges_error_handling():
    fs = GCSFileSystem(token="anon")

    async def mock_cat_file(path, start=None, end=None, **kwargs):
        if path == "b/error.txt":
            raise FileNotFoundError("Object not found")
        return b"0123456789"

    with mock.patch.object(fs, "_cat_file", side_effect=mock_cat_file):
        paths = ["b/ok.txt", "b/error.txt", "b/ok.txt"]
        starts = [0, 0, 5]
        ends = [5, 5, 10]

        # on_error="return" places exception at index 1
        res = await fs._cat_ranges(paths, starts, ends, max_gap=5, on_error="return")
        assert len(res) == 3
        assert bytes(res[0]) == b"01234"
        assert isinstance(res[1], FileNotFoundError)
        assert bytes(res[2]) == b"56789"

        # on_error="raise" raises immediately
        with pytest.raises(FileNotFoundError):
            await fs._cat_ranges(paths, starts, ends, max_gap=5, on_error="raise")


# =====================================================================
# 3. Unit tests for ExtendedGcsFileSystem._cat_ranges (Zonal MRD & routing)
# =====================================================================

@pytest.mark.asyncio
async def test_extended_gcsfs_cat_ranges_non_zonal_delegation():
    fs = ExtendedGcsFileSystem(token="anon")

    with mock.patch.object(fs, "_is_zonal_bucket", return_value=False):
        with mock.patch("gcsfs.core.GCSFileSystem._cat_ranges", return_value=[b"chunk1", b"chunk2"]) as mock_super:
            res = await fs._cat_ranges(["reg-bucket/f1", "reg-bucket/f2"], starts=[0, 10], ends=[5, 15], max_gap=5)
            assert res == [b"chunk1", b"chunk2"]
            assert mock_super.call_count == 1


@pytest.mark.asyncio
async def test_extended_gcsfs_cat_ranges_zonal_coalescing():
    fs = ExtendedGcsFileSystem(token="anon")
    zonal_data = b"0123456789abcdefghijklmnopqrstuvwxyz"

    # Mock MRD client
    class MockMRD:
        async def download_ranges(self, mrd_spec):
            # mrd_spec is a list of (start, length, BytesIO_buffer)
            for s, length, buf in mrd_spec:
                buf.write(zonal_data[s : s + length])

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_mrd_instance = MockMRD()

    # Mock pool cache get
    mock_pool = mock.AsyncMock()
    mock_pool_cache = mock.AsyncMock()
    mock_pool_cache.get = mock.AsyncMock(return_value=mock_pool)
    fs._mrd_pool_cache = mock_pool_cache

    with mock.patch.object(fs, "_is_zonal_bucket", return_value=True):
        with mock.patch("gcsfs.extended_gcsfs._get_mrd_size", return_value=len(zonal_data)):
            with mock.patch("gcsfs.extended_gcsfs._get_mrd_from_pool_or_mrd", return_value=mock_mrd_instance):
                paths = ["zonal-bucket/model.distcp", "zonal-bucket/model.distcp", "zonal-bucket/model.distcp"]
                starts = [0, 10, 20]
                ends = [5, 15, 25]

                # max_gap=5 coalesces into a single range [0, 25)
                res = await fs._cat_ranges(paths, starts, ends, max_gap=5)

                assert len(res) == 3
                assert bytes(res[0]) == b"01234"
                assert bytes(res[1]) == b"abcde"
                assert bytes(res[2]) == b"klmno"


@pytest.mark.asyncio
async def test_extended_gcsfs_cat_ranges_zonal_error_handling():
    fs = ExtendedGcsFileSystem(token="anon")

    class FailingMRD:
        async def download_ranges(self, mrd_spec):
            raise RuntimeError("Zonal MRD Connection Error")

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_pool = mock.AsyncMock()
    mock_pool_cache = mock.AsyncMock()
    mock_pool_cache.get = mock.AsyncMock(return_value=mock_pool)
    fs._mrd_pool_cache = mock_pool_cache

    with mock.patch.object(fs, "_is_zonal_bucket", return_value=True):
        with mock.patch("gcsfs.extended_gcsfs._get_mrd_size", return_value=100):
            with mock.patch("gcsfs.extended_gcsfs._get_mrd_from_pool_or_mrd", return_value=FailingMRD()):
                paths = ["zonal-bucket/f1", "zonal-bucket/f1"]
                starts = [0, 10]
                ends = [5, 15]

                # on_error="return"
                res = await fs._cat_ranges(paths, starts, ends, max_gap=5, on_error="return")
                assert len(res) == 2
                assert isinstance(res[0], RuntimeError)
                assert isinstance(res[1], RuntimeError)

                # on_error="raise"
                with pytest.raises(RuntimeError, match="Zonal MRD Connection Error"):
                    await fs._cat_ranges(paths, starts, ends, max_gap=5, on_error="raise")
