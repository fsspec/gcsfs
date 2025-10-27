import io
from unittest import mock

import pytest
from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import AsyncMultiRangeDownloader

from gcsfs.gcsfs_adapter import BucketType
from gcsfs.tests.conftest import files
from gcsfs.tests.settings import TEST_BUCKET

file = "test/accounts.1.json"
file_path = f"{TEST_BUCKET}/{file}"
data = files[file]
lines = io.BytesIO(data).readlines()
file_size = len(data)

read_block_params = [
    # Read specific chunk
    pytest.param(3, 10, None, data[3:3 + 10], id="offset=3, length=10"),
    # Read from beginning up to length
    pytest.param(0, 5, None, data[0:5], id="offset=0, length=5"),
    # Read from offset to end (simulate large length)
    pytest.param(15, 5000, None, data[15:], id="offset=15, length=large"),
    # Read beyond end of file (should return empty bytes)
    pytest.param(file_size + 10, 5, None, b"", id="offset>size, length=5"),
    # Read exactly at the end (zero length effective)
    pytest.param(file_size, 10, None, b"", id="offset=size, length=10"),
    # Read with delimiter
    pytest.param(1, 35, b'\n', lines[1], id="offset=1, length=35, delimiter=newline"),
    pytest.param(0, 30, b'\n', lines[0], id="offset=0, length=35, delimiter=newline"),
    pytest.param(0, 35, b'\n', lines[0] + lines[1], id="offset=0, length=35, delimiter=newline"),
]


@pytest.mark.parametrize("offset, length, delimiter, expected_data", read_block_params)
def test_read_block_zonal_path(gcs_adapter, offset, length, delimiter, expected_data):
    path = file_path

    async def mock_download_ranges_side_effect(read_requests, **kwargs):
        if read_requests and len(read_requests) == 1:
            param_offset, param_length, buffer_arg = read_requests[0]
            if hasattr(buffer_arg, 'write'):
                buffer_arg.write(data[param_offset:param_offset + param_length])
        return [mock.Mock(error=None)]

    patch_target_sync_layout = "gcsfs.gcsfs_adapter.GCSFileSystemAdapter._sync_get_storage_layout"
    mock_sync_get_layout = mock.Mock(return_value=BucketType.ZONAL_HIERARCHICAL)

    patch_target_create_mrd = "gcsfs.gcsfs_adapter.zb_hns_utils.create_mrd"
    mock_downloader = mock.Mock(spec=AsyncMultiRangeDownloader)
    mock_downloader.download_ranges = mock.AsyncMock(side_effect=mock_download_ranges_side_effect)
    mock_create_mrd = mock.AsyncMock(return_value=mock_downloader)

    with mock.patch(patch_target_sync_layout, mock_sync_get_layout), \
            mock.patch(patch_target_create_mrd, mock_create_mrd):
        result = gcs_adapter.read_block(path, offset, length, delimiter)

        assert result == expected_data
        mock_sync_get_layout.assert_called_once_with(TEST_BUCKET)
        if expected_data:
            mock_downloader.download_ranges.assert_called_with([(offset, mock.ANY, mock.ANY)])
        else:
            mock_downloader.download_ranges.assert_not_called()
