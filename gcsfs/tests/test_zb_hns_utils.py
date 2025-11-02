from io import BytesIO
from unittest import mock

import pytest

from gcsfs import zb_hns_utils


@pytest.mark.asyncio
async def test_create_mrd():
    """
    Tests that create_mrd calls the underlying AsyncMultiRangeDownloader.create_mrd
    with the correct arguments and returns its result.
    """
    mock_grpc_client = mock.Mock()
    bucket_name = "test-bucket"
    object_name = "test-object"
    generation = "12345"
    mock_mrd_instance = mock.AsyncMock()

    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncMultiRangeDownloader.create_mrd",
        new_callable=mock.AsyncMock,
        return_value=mock_mrd_instance,
    ) as mock_create:
        result = await zb_hns_utils.create_mrd(
            mock_grpc_client, bucket_name, object_name, generation
        )

        mock_create.assert_called_once_with(
            mock_grpc_client, bucket_name, object_name, generation
        )
        assert result is mock_mrd_instance


@pytest.mark.asyncio
async def test_download_range():
    """
    Tests that download_range calls mrd.download_ranges with the correct
    parameters and returns the data written to the buffer.
    """
    offset = 10
    length = 20
    mock_mrd = mock.AsyncMock()
    expected_data = b"test data from download"

    # Simulate the download_ranges method writing data to the buffer
    async def mock_download_ranges(ranges):
        _offset, _length, buffer = ranges[0]
        buffer.write(expected_data)

    mock_mrd.download_ranges.side_effect = mock_download_ranges

    result = await zb_hns_utils.download_range(offset, length, mock_mrd)

    mock_mrd.download_ranges.assert_called_once_with([(offset, length, mock.ANY)])
    assert result == expected_data
