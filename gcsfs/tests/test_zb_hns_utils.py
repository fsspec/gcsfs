from unittest import mock

import pytest

from gcsfs import zb_hns_utils

mock_grpc_client = mock.Mock()
bucket_name = "test-bucket"
object_name = "test-object"
generation = "12345"


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ranges, expected_call_count",
    [
        ([(0, 5), (10, 3)], 1),  # Basic case
        ([(0, 4), (5, 0), (10, 3)], 1),  # Mixed empty (should filter middle)
        ([(0, 0), (10, 0)], 0),  # All empty (should not call MRD)
        ([], 0),  # Empty list
    ],
    ids=["basic", "mixed_empty", "all_empty", "empty_list"],
)
async def test_download_ranges_unified(ranges, expected_call_count):
    """Unified test for download_ranges success scenarios."""
    mock_mrd = mock.AsyncMock()

    # Writes distinct data like b"0-5" to verify mapping
    async def side_effect(req_ranges):
        for offset, length, buf in req_ranges:
            buf.write(f"{offset}-{length}".encode())

    mock_mrd.download_ranges.side_effect = side_effect

    # Execute
    results = await zb_hns_utils.download_ranges(ranges, mock_mrd)

    # 1. Verify Results
    # Expect empty bytes for 0-length, otherwise expect encoded "{offset}-{length}"
    expected_results = [f"{off}-{ln}".encode() if ln > 0 else b"" for off, ln in ranges]
    assert results == expected_results

    # 2. Verify MRD Interaction
    assert mock_mrd.download_ranges.call_count == expected_call_count

    if expected_call_count > 0:
        # Verify it only received non-zero length ranges
        actual_args = mock_mrd.download_ranges.call_args[0][0]
        non_empty_ranges = [r for r in ranges if r[1] > 0]

        assert len(actual_args) == len(non_empty_ranges)
        for (act_off, act_len, act_buf), (exp_off, exp_len) in zip(
            actual_args, non_empty_ranges
        ):
            assert act_off == exp_off
            assert act_len == exp_len
            assert hasattr(act_buf, "write")


@pytest.mark.asyncio
async def test_download_ranges_exception():
    """Test exception propagation (Keep separate as it changes control flow)."""
    mock_mrd = mock.AsyncMock()
    mock_mrd.download_ranges.side_effect = ValueError("Fail")

    with pytest.raises(ValueError, match="Fail"):
        await zb_hns_utils.download_ranges([(0, 5)], mock_mrd)


@pytest.mark.asyncio
async def test_init_aaow():
    """
    Tests that init_aaow calls the underlying AsyncAppendableObjectWriter.open
    method and returns its result.
    """
    mock_writer_instance = mock.AsyncMock()
    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncAppendableObjectWriter",
        new_callable=mock.Mock,
        return_value=mock_writer_instance,
    ) as mock_writer_class:
        result = await zb_hns_utils.init_aaow(
            mock_grpc_client, bucket_name, object_name, generation
        )

        mock_writer_class.assert_called_once_with(
            client=mock_grpc_client,
            bucket_name=bucket_name,
            object_name=object_name,
            generation=generation,
            writer_options={},
        )
        mock_writer_instance.open.assert_awaited_once()
        assert result is mock_writer_instance


@pytest.mark.asyncio
async def test_init_aaow_with_flush_interval_bytes():
    """
    Tests that init_aaow correctly passes the flush_interval_bytes
    parameter to the AsyncAppendableObjectWriter.
    """
    mock_writer_instance = mock.AsyncMock()
    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncAppendableObjectWriter",
        new_callable=mock.Mock,
        return_value=mock_writer_instance,
    ) as mock_writer_class:
        result = await zb_hns_utils.init_aaow(
            mock_grpc_client,
            bucket_name,
            object_name,
            generation,
            flush_interval_bytes=1024,
        )

        mock_writer_class.assert_called_once_with(
            client=mock_grpc_client,
            bucket_name=bucket_name,
            object_name=object_name,
            generation=generation,
            writer_options={"FLUSH_INTERVAL_BYTES": 1024},
        )
        mock_writer_instance.open.assert_awaited_once()
        assert result is mock_writer_instance
