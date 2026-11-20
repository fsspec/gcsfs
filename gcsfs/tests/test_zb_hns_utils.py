import array
import asyncio
import collections
import concurrent.futures
import contextlib
import dataclasses
import importlib
import logging
import os
import sys
import threading
from typing import Optional
from unittest import mock

import pytest
from google.api_core.exceptions import FailedPrecondition, NotFound
from google.cloud.storage.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
)

from gcsfs import zb_hns_utils
from gcsfs.retry import GenerationMismatchError
from gcsfs.zb_hns_utils import DirectMemmoveBuffer, MRDPool, MRDPoolCache, _close_mrds

mock_grpc_client = mock.Mock()
bucket_name = "test-bucket"
object_name = "test-object"
generation = "12345"


@pytest.mark.asyncio
async def test_shared_mrd_closed_only_by_last_holder():
    class FakeMRD:
        def __init__(self):
            self.persisted_size = 0
            self.close_count = 0

        async def close(self):
            self.close_count += 1

    pool = MRDPool(mock.Mock(), "b", "o", 1, True, 1, cache=None)
    pool._create_mrd = mock.AsyncMock(side_effect=lambda: FakeMRD())

    await pool.initialize()

    cm_a = pool.get_mrd()
    mrd_a = await cm_a.__aenter__()  # exclusive
    cm_b = pool.get_mrd()
    mrd_b = await cm_b.__aenter__()  # round-robin share
    assert mrd_a is mrd_b  # same MRD shared

    await pool.close()
    assert mrd_a.close_count == 0  # still in use -> not closed

    await cm_a.__aexit__(None, None, None)
    assert mrd_a.close_count == 0  # B still holds it

    await cm_b.__aexit__(None, None, None)
    assert mrd_a.close_count == 1  # last holder closes it exactly once


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


@dataclasses.dataclass
class _FakePreconditions:
    if_generation_match: Optional[int] = None


# The real class once google-cloud-storage ships it, so CI builds it too.
_Preconditions = zb_hns_utils.Preconditions or _FakePreconditions


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "if_generation_match, expected_kwargs",
    [
        (0, {"generation": 0}),
        (
            7,
            {
                "generation": None,
                "preconditions": _Preconditions(if_generation_match=7),
            },
        ),
    ],
)
async def test_init_aaow_if_generation_match(if_generation_match, expected_kwargs):
    mock_writer_instance = mock.AsyncMock()
    with (
        mock.patch.object(zb_hns_utils, "Preconditions", _Preconditions),
        mock.patch(
            "gcsfs.zb_hns_utils.AsyncAppendableObjectWriter",
            new_callable=mock.Mock,
            return_value=mock_writer_instance,
        ) as mock_writer_class,
    ):
        await zb_hns_utils.init_aaow(
            mock_grpc_client,
            bucket_name,
            object_name,
            if_generation_match=if_generation_match,
        )
        mock_writer_class.assert_called_once_with(
            client=mock_grpc_client,
            bucket_name=bucket_name,
            object_name=object_name,
            writer_options={},
            **expected_kwargs,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_class, expected_kwargs",
    [("RAPID", {"storage_class": "RAPID"}), (None, {})],
)
async def test_init_aaow_storage_class(storage_class, expected_kwargs):
    """storage_class is forwarded to the writer only when it is set."""
    mock_writer_instance = mock.AsyncMock()
    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncAppendableObjectWriter",
        new_callable=mock.Mock,
        return_value=mock_writer_instance,
    ) as mock_writer_class:
        await zb_hns_utils.init_aaow(
            mock_grpc_client, "bucket", "object", storage_class=storage_class
        )
        mock_writer_class.assert_called_once_with(
            client=mock_grpc_client,
            bucket_name="bucket",
            object_name="object",
            generation=None,
            writer_options={},
            **expected_kwargs,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("generation", [0, 123])
@pytest.mark.parametrize("if_generation_match", [0, 7])
async def test_init_aaow_rejects_generation_with_if_generation_match(
    generation, if_generation_match
):
    # A set generation selects an existing object (or, for 0, already means
    # create-only), so the combination must not silently drop the precondition.
    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncAppendableObjectWriter", new_callable=mock.Mock
    ) as mock_writer_class:
        with pytest.raises(ValueError, match="if_generation_match"):
            await zb_hns_utils.init_aaow(
                mock_grpc_client,
                bucket_name,
                object_name,
                generation=generation,
                if_generation_match=if_generation_match,
            )
        mock_writer_class.assert_not_called()


@pytest.mark.asyncio
async def test_init_aaow_if_generation_match_unsupported_sdk():
    with (
        mock.patch.object(zb_hns_utils, "Preconditions", None),
        mock.patch(
            "gcsfs.zb_hns_utils.AsyncAppendableObjectWriter",
            new_callable=mock.Mock,
        ) as mock_writer_class,
    ):
        with pytest.raises(NotImplementedError):
            await zb_hns_utils.init_aaow(
                mock_grpc_client, bucket_name, object_name, if_generation_match=7
            )
        mock_writer_class.assert_not_called()


@pytest.mark.asyncio
async def test_init_aaow_if_generation_match_zero_without_preconditions():
    mock_writer_instance = mock.AsyncMock()
    with (
        mock.patch.object(zb_hns_utils, "Preconditions", None),
        mock.patch(
            "gcsfs.zb_hns_utils.AsyncAppendableObjectWriter",
            new_callable=mock.Mock,
            return_value=mock_writer_instance,
        ) as mock_writer_class,
    ):
        await zb_hns_utils.init_aaow(
            mock_grpc_client, bucket_name, object_name, if_generation_match=0
        )
        mock_writer_class.assert_called_once_with(
            client=mock_grpc_client,
            bucket_name=bucket_name,
            object_name=object_name,
            generation=0,
            writer_options={},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "if_generation_match, expected",
    [(0, FileExistsError), (7, GenerationMismatchError), (None, FailedPrecondition)],
)
async def test_init_aaow_failed_precondition(if_generation_match, expected):
    mock_writer_instance = mock.AsyncMock()
    mock_writer_instance.open.side_effect = FailedPrecondition("exists")
    with (
        mock.patch.object(zb_hns_utils, "Preconditions", _Preconditions),
        mock.patch(
            "gcsfs.zb_hns_utils.AsyncAppendableObjectWriter",
            new_callable=mock.Mock,
            return_value=mock_writer_instance,
        ),
    ):
        with pytest.raises(expected) as excinfo:
            await zb_hns_utils.init_aaow(
                mock_grpc_client,
                bucket_name,
                object_name,
                if_generation_match=if_generation_match,
            )
    if if_generation_match is None:
        # No precondition was requested, so the SDK error is not translated.
        assert excinfo.value is mock_writer_instance.open.side_effect
    elif if_generation_match == 7:
        assert excinfo.value.generation == 7
        assert excinfo.value.__cause__ is mock_writer_instance.open.side_effect


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


@pytest.mark.asyncio
async def test_init_mrd_success():
    """Tests successful initialization of MRD."""
    mock_mrd_instance = mock.Mock()
    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncMultiRangeDownloader.create_mrd",
        new_callable=mock.AsyncMock,
        return_value=mock_mrd_instance,
    ) as mock_create_mrd:
        result = await zb_hns_utils.init_mrd(
            mock_grpc_client, bucket_name, object_name, generation
        )

        mock_create_mrd.assert_awaited_once_with(
            mock_grpc_client, bucket_name, object_name, generation
        )
        assert result is mock_mrd_instance


@pytest.mark.asyncio
async def test_init_mrd_with_cache_type():
    """Tests that init_mrd passes cache_type as metadata."""
    mock_mrd_instance = mock.Mock()
    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncMultiRangeDownloader.create_mrd",
        new_callable=mock.AsyncMock,
        return_value=mock_mrd_instance,
    ) as mock_create_mrd:
        result = await zb_hns_utils.init_mrd(
            mock_grpc_client,
            bucket_name,
            object_name,
            generation,
            cache_type="readahead",
            cache_source="explicit",
        )

        mock_create_mrd.assert_awaited_once_with(
            mock_grpc_client,
            bucket_name,
            object_name,
            generation,
            metadata=[("x-goog-api-client", "cache_type/readahead:e")],
        )
        assert result is mock_mrd_instance


@pytest.mark.asyncio
async def test_init_mrd_not_found():
    """Tests that init_mrd raises FileNotFoundError when object is not found."""

    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncMultiRangeDownloader.create_mrd",
        new_callable=mock.AsyncMock,
    ) as mock_create_mrd:
        mock_create_mrd.side_effect = NotFound("Object not found")

        with pytest.raises(FileNotFoundError) as excinfo:
            await zb_hns_utils.init_mrd(
                mock_grpc_client, bucket_name, object_name, generation
            )

        assert f"{bucket_name}/{object_name}" in str(excinfo.value)


@pytest.mark.asyncio
async def test_close_aaow(caplog):
    """Tests the closing scenarios for AsyncAppendableObjectWriter."""
    # 1. Handles None gracefully
    await zb_hns_utils.close_aaow(None)

    # 2. Closes successfully
    mock_aaow = mock.AsyncMock()
    await zb_hns_utils.close_aaow(mock_aaow, finalize_on_close=True)
    mock_aaow.close.assert_awaited_once_with(finalize_on_close=True)

    # 3. Catches exceptions and logs a warning
    mock_aaow.reset_mock()
    mock_aaow.bucket_name = "test-bucket"
    mock_aaow.object_name = "test-object"
    mock_aaow.close.side_effect = Exception("Close failed")

    with caplog.at_level(logging.WARNING, logger="gcsfs"):
        await zb_hns_utils.close_aaow(mock_aaow, finalize_on_close=False)

    mock_aaow.close.assert_awaited_once_with(finalize_on_close=False)
    assert (
        "Error closing AsyncAppendableObjectWriter for test-bucket/test-object: Close failed"
        in caplog.text
    )

    # 4. Logs and re-raises a failed finalize, so the write is not reported as successful
    mock_aaow.reset_mock()
    mock_aaow.close.side_effect = NotFound("Object deleted before finalize")
    caplog.clear()

    with caplog.at_level(logging.WARNING, logger="gcsfs"):
        with pytest.raises(NotFound, match="Object deleted before finalize"):
            await zb_hns_utils.close_aaow(mock_aaow, finalize_on_close=True)

    mock_aaow.close.assert_awaited_once_with(finalize_on_close=True)
    assert (
        "Error closing AsyncAppendableObjectWriter for test-bucket/test-object"
        in caplog.text
    )


@pytest.mark.asyncio
async def test_close_mrd(caplog):
    """Tests all graceful closing scenarios for AsyncMultiRangeDownloader."""
    # 1. Handles None gracefully
    await zb_hns_utils.close_mrd(None)

    # 2. Closes successfully
    mock_mrd = mock.AsyncMock()
    await zb_hns_utils.close_mrd(mock_mrd)
    mock_mrd.close.assert_awaited_once()

    # 3. Catches exceptions and logs a warning
    mock_mrd.reset_mock()
    mock_mrd.bucket_name = "test-bucket"
    mock_mrd.object_name = "test-object"
    mock_mrd.close.side_effect = Exception("Close failed")

    with caplog.at_level(logging.WARNING, logger="gcsfs"):
        await zb_hns_utils.close_mrd(mock_mrd)

    mock_mrd.close.assert_awaited_once()
    assert (
        "Error closing AsyncMultiRangeDownloader for test-bucket/test-object: Close failed"
        in caplog.text
    )


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
async def test_download_ranges_validation_limit():
    """
    Tests that download_ranges raises a ValueError if the number of ranges
    exceeds 1000.
    """
    mock_mrd = mock.AsyncMock()
    ranges = [(i, 10) for i in range(1001)]

    with pytest.raises(
        ValueError,
        match="Invalid input - number of ranges cannot be more than 1000",
    ):
        await zb_hns_utils.download_ranges(ranges, mock_mrd)


@pytest.mark.asyncio
async def test_mrd_pool_close():
    gcsfs_mock = mock.Mock()
    gcsfs_mock._get_grpc_client = mock.AsyncMock()

    mrd_instance_mock = mock.AsyncMock()

    with mock.patch(
        "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
        return_value=mrd_instance_mock,
    ):
        pool = MRDPool(gcsfs_mock, "bucket", "obj", "123", True, 1)
        await pool.initialize()

        await pool.close()
        mrd_instance_mock.close.assert_awaited_once()
        assert len(pool._all_mrds) == 0


@pytest.fixture
def mock_gcsfs():
    gcsfs_mock = mock.Mock()
    gcsfs_mock._get_grpc_client = mock.AsyncMock()
    gcsfs_mock._info = mock.AsyncMock(
        return_value={"generation": "123", "timeFinalized": "2026-06-17T00:00:00Z"}
    )
    return gcsfs_mock


@pytest.mark.asyncio
@mock.patch(
    "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
    new_callable=mock.AsyncMock,
)
async def test_mrd_pool_scaling(create_mrd_mock, mock_gcsfs):
    def mock_mrd_factory(*args, **kwargs):
        m = mock.AsyncMock()
        m.persisted_size = 1024
        return m

    create_mrd_mock.side_effect = mock_mrd_factory

    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 2)

    await pool.initialize()
    assert pool.persisted_size == 1024
    assert pool._active_count == 1
    create_mrd_mock.assert_awaited_once()

    async with pool.get_mrd() as mrd1:
        # Since mrd1 is in use, getting another one should spawn a new MRD
        async with pool.get_mrd() as mrd2:
            assert pool._active_count == 2
            assert create_mrd_mock.call_count == 2
            assert mrd1 is not mrd2

    # Both should have been returned to the free queue
    assert pool._free_mrds.qsize() == 2


@pytest.mark.asyncio
@mock.patch(
    "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
    new_callable=mock.AsyncMock,
)
async def test_mrd_pool_double_initialize(create_mrd_mock, mock_gcsfs):
    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 2)

    await pool.initialize()
    await pool.initialize()  # Second call should be a no-op

    assert pool._active_count == 1
    create_mrd_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_mrd_pool_initialize_after_close(mock_gcsfs):
    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 1)
    await pool.close()

    with pytest.raises(RuntimeError, match="Cannot initialize a closed MRDPool"):
        await pool.initialize()


@pytest.mark.asyncio
@mock.patch(
    "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
    new_callable=mock.AsyncMock,
)
async def test_mrd_pool_get_mrd_creation_error(create_mrd_mock, mock_gcsfs):
    # First creation succeeds during initialization
    valid_mrd = mock.AsyncMock()

    # Second creation fails when pool tries to scale
    create_mrd_mock.side_effect = [valid_mrd, Exception("Network Error")]

    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 2)
    await pool.initialize()

    # Consume the initialized MRD
    async def consume_and_error():
        async with pool.get_mrd() as _:
            # Try to get a second one, which forces a spawn that will fail
            with pytest.raises(Exception, match="Network Error"):
                async with pool.get_mrd() as _:
                    pass

    await consume_and_error()

    # Active count should remain 1 because the second creation failed and rolled back
    assert pool._active_count == 1


@pytest.mark.asyncio
@mock.patch(
    "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
    new_callable=mock.AsyncMock,
)
async def test_mrd_pool_close_with_exceptions(create_mrd_mock, mock_gcsfs):
    bad_mrd_instance = mock.AsyncMock()
    bad_mrd_instance.close.side_effect = RuntimeError("Close failed")
    create_mrd_mock.return_value = bad_mrd_instance

    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 1)
    await pool.initialize()

    with pytest.raises(RuntimeError, match="Close failed"):
        await pool.close()

    bad_mrd_instance.close.assert_awaited_once()
    assert len(pool._all_mrds) == 0


@pytest.mark.asyncio
async def test_mrd_pool_queue_filled_during_lock_wait(mock_gcsfs):
    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 1)
    mrd_mock = mock.AsyncMock()

    # Simulate _create_mrd so we correctly populate _all_mrds
    async def fake_create_mrd():
        return mrd_mock

    with mock.patch.object(pool, "_create_mrd", side_effect=fake_create_mrd):
        await pool.initialize()

        side_effects = [True] + [False] * 10
        # When `_free_mrds.empty` evaluates to True on the first pass,
        # it checks `_all_mrds`. If it's non-empty, it does a round-robin
        # instead of blocking on `_free_mrds.get()`. So, the mock doesn't
        # really test waiting on lock anymore because round-robin returns
        # immediately. Let's force `_all_mrds` to be empty temporarily to test
        # the blocking wait behavior if sharing is not possible (e.g., initial
        # scale up but the only MRD was closed/removed, which shouldn't happen
        # in practice but covers the logic branch).
        with mock.patch.object(pool, "_all_mrds", []):
            with mock.patch.object(pool._free_mrds, "empty", side_effect=side_effects):
                # We need a task to put the mrd into the queue while we're waiting
                async def put_mrd_later():
                    await asyncio.sleep(0.01)
                    await pool._free_mrds.put(mrd_mock)

                asyncio.create_task(put_mrd_later())

                async with pool.get_mrd() as mrd:
                    assert mrd == mrd_mock

        # We should not have spawned a new MRD beyond the first
        assert pool._active_count == 1


@pytest.mark.asyncio
async def test_mrd_pool_round_robin_multi_request(mock_gcsfs):
    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 2)
    mrd1 = mock.AsyncMock()
    mrd2 = mock.AsyncMock()

    mrd_mocks = [mrd1, mrd2]

    # Ensure our mock actually appends to _all_mrds so the round-robin
    # logic sees that there are available active MRDs to share.
    async def fake_create_mrd():
        mrd = mrd_mocks.pop(0)
        return mrd

    with mock.patch.object(pool, "_create_mrd", side_effect=fake_create_mrd):
        await pool.initialize()

        # Keep both MRDs checked out to force the pool to its maximum size
        # and keep the free queue empty.
        async with pool.get_mrd() as active_mrd1:
            async with pool.get_mrd() as active_mrd2:
                assert active_mrd1 == mrd1
                assert active_mrd2 == mrd2
                assert pool._free_mrds.empty()
                assert pool._active_count == 2
                assert pool._rr_index == 0

                # Requesting a 3rd MRD should trigger the round-robin logic
                async with pool.get_mrd() as shared_mrd1:
                    assert shared_mrd1 == mrd1
                    assert pool._rr_index == 1

                # Requesting a 4th MRD should continue the round-robin
                async with pool.get_mrd() as shared_mrd2:
                    assert shared_mrd2 == mrd2
                    assert pool._rr_index == 0

                # Requesting a 5th MRD should wrap around back to the first
                async with pool.get_mrd() as shared_mrd3:
                    assert shared_mrd3 == mrd1
                    assert pool._rr_index == 1


def _gated_mrd_factory(num_gates):
    """Returns (create, created, gates): each create() call makes a new mock
    MRD and returns it once its gate is set."""
    created = []
    gates = [asyncio.Event() for _ in range(num_gates)]

    async def create():
        idx = len(created)
        mrd = mock.AsyncMock(name=f"mrd{idx}")
        created.append(mrd)
        await gates[idx].wait()
        return mrd

    return create, created, gates


@pytest.mark.asyncio
async def test_mrd_pool_opens_streams_in_parallel_and_spreads_waiters(mock_gcsfs):
    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 4)
    pool._create_mrd, created, gates = _gated_mrd_factory(4)
    holders = collections.Counter()
    done = asyncio.Event()

    async def worker():
        async with pool.get_mrd() as mrd:
            holders[created.index(mrd)] += 1
            await done.wait()

    tasks = [asyncio.create_task(worker()) for _ in range(8)]
    await asyncio.sleep(0.01)
    # All four streams open at once, outside the lock.
    assert pool._creating_count == 4
    assert len(created) == 4

    # The four extra callers must not all pile onto the first stream to open.
    gates[0].set()
    await asyncio.sleep(0.01)
    for gate in gates[1:]:
        gate.set()
    await asyncio.sleep(0.01)
    done.set()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)

    assert dict(holders) == {0: 2, 1: 2, 2: 2, 3: 2}
    assert pool._creating_count == 0
    assert pool._active_count == 4


@pytest.mark.asyncio
async def test_mrd_pool_waiters_wake_once_after_last_stream_opens(mock_gcsfs):
    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 4)
    pool._create_mrd, created, gates = _gated_mrd_factory(4)
    done = asyncio.Event()
    wait_calls = 0
    orig_wait = pool._cond.wait

    async def counting_wait():
        nonlocal wait_calls
        wait_calls += 1
        return await orig_wait()

    pool._cond.wait = counting_wait

    async def worker():
        async with pool.get_mrd():
            await done.wait()

    tasks = [asyncio.create_task(worker()) for _ in range(8)]
    await asyncio.sleep(0.01)
    assert wait_calls == 4

    # Waiters stay asleep while other streams are still opening.
    for gate in gates[:3]:
        gate.set()
        await asyncio.sleep(0.01)
    assert wait_calls == 4

    gates[3].set()
    await asyncio.sleep(0.01)
    done.set()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)
    assert wait_calls == 4


@pytest.mark.asyncio
async def test_mrd_pool_close_during_create(mock_gcsfs):
    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 1)
    pool._create_mrd, created, gates = _gated_mrd_factory(1)
    creator = asyncio.create_task(pool.get_mrd().__aenter__())
    waiter = asyncio.create_task(pool.get_mrd().__aenter__())
    await asyncio.sleep(0.01)
    assert pool._creating_count == 1
    assert not waiter.done()

    await pool.close()
    # close() wakes the caller waiting for the stream being opened.
    with pytest.raises(RuntimeError, match="MRDPool is closed"):
        await asyncio.wait_for(waiter, timeout=1)

    # The stream that finishes opening after close() is closed, not leaked.
    gates[0].set()
    with pytest.raises(RuntimeError, match="MRDPool is closed"):
        await creator
    created[0].close.assert_awaited_once()
    assert pool._all_mrds == []
    assert pool._creating_count == 0


@pytest.mark.asyncio
async def test_mrd_pool_create_failure_wakes_waiter(mock_gcsfs):
    # Uninitialized pool_size=1: caller 1 fails to open the stream while
    # caller 2 waits; caller 2 must take over the slot instead of hanging.
    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 1)
    calls = 0
    good_mrd = mock.AsyncMock()

    async def flaky_create():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        if calls == 1:
            raise ValueError("transient open failure")
        return good_mrd

    pool._create_mrd = flaky_create

    async def use_mrd():
        async with pool.get_mrd() as mrd:
            return mrd

    r1, r2 = await asyncio.wait_for(
        asyncio.gather(use_mrd(), use_mrd(), return_exceptions=True), timeout=1
    )
    assert isinstance(r1, ValueError)
    assert r2 is good_mrd
    assert pool._active_count == 1
    assert pool._creating_count == 0


@pytest.mark.asyncio
async def test_mrd_pool_cancel_after_create_requeues_mrd(mock_gcsfs):
    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 2)
    pool._create_mrd, created, gates = _gated_mrd_factory(1)
    task = asyncio.create_task(pool.get_mrd().__aenter__())
    await asyncio.sleep(0.01)

    # Another coroutine holds the lock when the stream finishes opening, and
    # the caller is cancelled while it is still waiting for the pool.
    await pool._lock.acquire()
    gates[0].set()
    await asyncio.sleep(0.01)
    task.cancel()
    pool._lock.release()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)

    new_mrd = created[0]
    assert pool._creating_count == 0
    assert pool._active_count == 1
    assert pool._all_mrds == [new_mrd]
    assert pool._free_mrds.qsize() == 1

    await pool.close()
    new_mrd.close.assert_awaited_once()


@mock.patch("gcsfs.zb_hns_utils.ctypes.memmove")
def test_direct_memmove_buffer_error_handling(mock_memmove):
    # Use a size > 128KB to trigger the executor background path
    size = 130 * 1024 + 10
    data1 = b"a" * (130 * 1024)
    data2 = b"b" * 10

    # Simulate an access violation or similar error during memory copy
    mock_memmove.side_effect = MemoryError("Segfault simulated")

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    # First write triggers the background error (slow path)
    future = view.write(data1)

    # Wait for the background thread to actually fail
    with pytest.raises(MemoryError):
        future.result()

    # Subsequent writes should raise the stored error immediately
    with pytest.raises(MemoryError, match="Segfault simulated"):
        view.write(data2)

    # Close should also raise the stored error.
    with pytest.raises(MemoryError, match="Segfault simulated"):
        buf.close()

    executor.shutdown()


def test_direct_memmove_buffer():
    data1 = b"hello"
    data2 = b"world"
    size = len(data1) + len(data2)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    future1 = view.write(data1)
    future2 = view.write(data2)

    future1.result()
    future2.result()

    view.close()
    buf.close()

    result_bytes = buf.get_value()
    assert result_bytes == b"helloworld"

    executor.shutdown()


def test_direct_memmove_buffer_accepts_memoryview_slices():
    # The storage SDK's zero-copy read path hands chunks over as memoryview
    # slices of the gRPC message; halves above THRESHOLD_BYTES_FOR_SCHEDULING
    # go through the executor, so this covers the scheduled memmove path.
    data = bytes(range(256)) * 2048
    size = len(data)
    half = size // 2
    assert half > DirectMemmoveBuffer.THRESHOLD_BYTES_FOR_SCHEDULING
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    view.write(memoryview(data)[:half]).result()
    view.write(memoryview(data)[half:]).result()

    view.close()
    buf.close()
    assert buf.get_value() == data
    executor.shutdown()


def test_direct_memmove_buffer_copies_whole_buffer_memoryview():
    # A bytes payload covering the whole buffer is kept by reference. A small
    # memoryview covering it is copied into new bytes during write(), which
    # then take the same path, so the caller can resize or overwrite the source.
    data = bytes(range(256)) * 4
    source = bytearray(data)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(len(data), executor, max_pending=2)
    view = buf.get_view(0, len(data))

    view.write(memoryview(source)).result()
    source[:] = b"\x00" * len(source)
    source.clear()

    view.close()
    buf.close()
    assert buf._start_address is None
    result = buf.get_value()
    assert isinstance(result, bytes)
    assert result == data
    executor.shutdown()


def test_direct_memmove_buffer_memoryview_after_module_reexecution():
    # gcsfs/tests/test_init.py drops every gcsfs module from sys.modules and
    # imports the package again, then restores the originals. The module body
    # runs twice in one process; each run binds its own private PyDLL
    # functions, so the second run must not break the first module's calls.
    import gcsfs

    saved_module = sys.modules.pop("gcsfs.zb_hns_utils")
    saved_attr = gcsfs.zb_hns_utils
    try:
        importlib.import_module("gcsfs.zb_hns_utils")
    finally:
        sys.modules["gcsfs.zb_hns_utils"] = saved_module
        gcsfs.zb_hns_utils = saved_attr

    data = bytes(range(256)) * 4
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(len(data), executor, max_pending=2)
    view = buf.get_view(0, len(data))

    view.write(memoryview(data)).result()

    view.close()
    buf.close()
    assert buf.get_value() == data
    executor.shutdown()


@pytest.mark.parametrize(
    "bad",
    ["text", 123, memoryview(b"abcdef")[::2], memoryview(b"abcdef")[0:0:2]],
    ids=["str", "int", "non_contiguous_memoryview", "empty_non_contiguous_memoryview"],
)
def test_partial_view_write_rejects_non_contiguous_or_non_buffer(bad):
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(4, executor, max_pending=2)
    view = buf.get_view(0, 4)

    with pytest.raises(ValueError, match="Expected a contiguous bytes-like object"):
        view.write(bad)

    executor.shutdown()


@pytest.mark.parametrize(
    "make",
    [bytearray, lambda data: array.array("I", data)],
    ids=["bytearray", "array_uint32"],
)
def test_partial_view_write_accepts_any_contiguous_buffer(make):
    data = bytes(range(256)) * 4
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(len(data), executor, max_pending=2)
    view = buf.get_view(0, len(data))

    view.write(make(data)).result()

    view.close()
    buf.close()
    assert buf.get_value() == data
    executor.shutdown()


@pytest.mark.parametrize(
    "make_view",
    [
        lambda data: memoryview(data).cast("I"),
        lambda data: memoryview(data).cast("B", [4, len(data) // 4]),
    ],
    ids=["cast_uint32", "two_dimensional"],
)
def test_partial_view_write_sizes_memoryview_in_bytes(make_view):
    # len() of these views is a fraction of their byte size; the whole payload
    # must still be copied and accounted for.
    data = bytes(range(256)) * 4
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(len(data), executor, max_pending=2)
    view = buf.get_view(0, len(data))

    view.write(make_view(data)).result()

    view.close()
    buf.close()
    assert buf.get_value() == data
    executor.shutdown()


def test_partial_view_write_handles_bytes_subclass_and_empty_2d_view():
    class _LyingBytes(bytes):
        def __len__(self):
            return 64

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        # Whole-buffer write of a bytes subclass returns plain bytes sized by
        # its true buffer export rather than Python __len__.
        whole = DirectMemmoveBuffer(4, executor, max_pending=2)
        wv = whole.get_view(0, 4)
        wv.write(_LyingBytes(b"abcd")).result()
        wv.close()
        whole.close()
        res = whole.get_value()
        assert type(res) is bytes and res == b"abcd"

        # Multi-view write + empty 2D C-contiguous view does not over-read or fail.
        split = DirectMemmoveBuffer(8, executor, max_pending=2)
        v1 = split.get_view(0, 4)
        v2 = split.get_view(4, 4)
        empty_2d = memoryview(b"abcd").cast("B", [1, 4])[:0]
        v1.write(empty_2d).result()
        v1.write(_LyingBytes(b"efgh")).result()
        v2.write(b"ijkl").result()
        v1.close()
        v2.close()
        split.close()
        assert split.get_value() == b"efghijkl"
    finally:
        executor.shutdown()


def test_partial_view_write_error_does_not_pin_buffer():
    # PartialView.write casts non-bytes inputs to an internal byte memoryview;
    # if write() fails, the exception traceback must not keep that export open
    # while the caller holds the exception (e.g. cat_ranges on_error="return"),
    # nor release a memoryview owned by the caller.
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        buf = DirectMemmoveBuffer(4, executor, max_pending=2)
        view = buf.get_view(0, 4)

        overflow_src = bytearray(b"abcdef")
        caller_mv = memoryview(overflow_src)
        with pytest.raises(BufferError) as overflow_exc:
            view.write(caller_mv)
        assert "Max capacity is 4 bytes" in str(overflow_exc.value)
        assert caller_mv.tobytes() == b"abcdef"
        caller_mv.release()
        overflow_src.clear()

        buf.close()
        closed_src = bytearray(b"abcd")
        with pytest.raises(ValueError) as closed_exc:
            view.write(closed_src)
        assert "closed buffer" in str(closed_exc.value)
        closed_src.clear()
    finally:
        executor.shutdown()


@pytest.mark.parametrize("copy_fails", [False, True], ids=["copied", "copy_fails"])
def test_direct_memmove_buffer_releases_source_before_future_completes(copy_fails):
    # A scheduled copy works on PartialView.write's own byte view of the chunk.
    # The worker's arguments, or the stored error of a failed copy, would keep
    # that view alive past the write future, so the source could not be resized
    # by a caller reacting to the future.
    size = DirectMemmoveBuffer.THRESHOLD_BYTES_FOR_SCHEDULING + 1
    source = bytearray(b"a" * size)
    resize_errors = []

    def resize_source(_future):
        try:
            source.append(0)
            resize_errors.append(None)
        except BufferError as e:
            resize_errors.append(e)

    failing_memmove = (
        mock.patch.object(zb_hns_utils.ctypes, "memmove", side_effect=OSError("boom"))
        if copy_fails
        else contextlib.nullcontext()
    )
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    gate = threading.Event()
    executor.submit(gate.wait)  # hold the worker until the callback is attached
    buf = DirectMemmoveBuffer(size, executor)
    with failing_memmove:
        future = buf.get_view(0, size).write(source)
        future.add_done_callback(resize_source)
        gate.set()
        error = future.exception()
        executor.shutdown()

    assert isinstance(error, OSError) == copy_fails
    assert resize_errors == [None]


@pytest.mark.parametrize(
    "size",
    [1024, DirectMemmoveBuffer.THRESHOLD_BYTES_FOR_SCHEDULING + 1],
    ids=["inline", "scheduled"],
)
def test_direct_memmove_buffer_leaves_callers_memoryview_usable(size):
    # google-cloud-storage reads a chunk again after write() returns (the
    # rolling CRC32C of whole-object reads), so the sink may release its own
    # view of the chunk but never the caller's. The buffer is one byte larger
    # than the chunk, so the chunk is copied through the sink's own view.
    data = bytes(range(256)) * (size // 256 + 1)
    chunk = memoryview(data)[:size]
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size + 1, executor)
    view = buf.get_view(0, size + 1)

    view.write(chunk).result()
    assert chunk.tobytes() == data[:size]
    view.write(b"!").result()

    view.close()
    buf.close()
    assert buf.get_value() == data[:size] + b"!"
    executor.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("existing, expected", [(None, "1"), ("0", "0")])
async def test_init_mrd_enables_zero_copy_bidi_read(monkeypatch, existing, expected):
    # The opt-in happens before create_mrd opens the stream, and an explicit
    # setting in the environment wins.
    name = zb_hns_utils.ZERO_COPY_BIDI_READ_ENV_VAR
    if existing is None:
        monkeypatch.delenv(name, raising=False)
    else:
        monkeypatch.setenv(name, existing)

    seen_in_create_mrd = []

    async def _fake_create_mrd(*args, **kwargs):
        seen_in_create_mrd.append(os.environ.get(name))
        return mock.Mock()

    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncMultiRangeDownloader.create_mrd",
        side_effect=_fake_create_mrd,
    ):
        await zb_hns_utils.init_mrd(mock_grpc_client, bucket_name, object_name)

    assert seen_in_create_mrd == [expected]
    assert os.environ.get(name) == expected


def test_import_does_not_enable_zero_copy_bidi_read(monkeypatch):
    # Importing gcsfs must not change the SDK's behaviour for processes that
    # never open a gRPC MRD read stream (Rapid or Rapid Cache Ultra).
    import gcsfs

    name = zb_hns_utils.ZERO_COPY_BIDI_READ_ENV_VAR
    monkeypatch.delenv(name, raising=False)

    saved_module = sys.modules.pop("gcsfs.zb_hns_utils")
    saved_attr = gcsfs.zb_hns_utils
    try:
        importlib.import_module("gcsfs.zb_hns_utils")
    finally:
        sys.modules["gcsfs.zb_hns_utils"] = saved_module
        gcsfs.zb_hns_utils = saved_attr

    assert name not in os.environ


class _MemoryviewDownloader:
    """download_ranges() of a google-cloud-storage with accepts_memoryview=."""

    bucket_name = "bucket"
    object_name = "obj"

    def __init__(self):
        self.accepts_memoryview_args = []

    async def download_ranges(self, read_ranges, accepts_memoryview=False):
        self.accepts_memoryview_args.append(accepts_memoryview)
        for _, length, buffer in read_ranges:
            buffer.write(memoryview(b"x" * length))


class _LegacyDownloader:
    """download_ranges() of a google-cloud-storage without the keyword."""

    bucket_name = "bucket"
    object_name = "obj"

    def __init__(self):
        self.calls = 0

    async def download_ranges(self, read_ranges):
        self.calls += 1
        for _, length, buffer in read_ranges:
            buffer.write(b"x" * length)


def test_download_ranges_kwargs_follow_the_downloader_signature():
    first = zb_hns_utils._download_ranges_kwargs(_MemoryviewDownloader)
    second = zb_hns_utils._download_ranges_kwargs(_MemoryviewDownloader)
    assert first == {"accepts_memoryview": True}
    assert second == {"accepts_memoryview": True}
    assert first is not second
    assert zb_hns_utils._download_ranges_kwargs(_LegacyDownloader) == {}
    # Mock downloaders in tests carry download_ranges on the instance only.
    assert zb_hns_utils._download_ranges_kwargs(type(mock.MagicMock())) == {}

    class _UnhashableCallable:
        __hash__ = None

        async def __call__(self, read_ranges):
            pass

    class _UnhashableDownloader:
        download_ranges = _UnhashableCallable()

    assert zb_hns_utils._download_ranges_kwargs(_UnhashableDownloader) == {}


@pytest.mark.asyncio
async def test_download_range_requests_memoryview_chunks_when_supported():
    mrd = _MemoryviewDownloader()
    assert await zb_hns_utils.download_range(0, 4, mrd) == b"xxxx"
    assert mrd.accepts_memoryview_args == [True]


@pytest.mark.asyncio
async def test_download_range_passes_no_keyword_to_older_sdks():
    mrd = _LegacyDownloader()
    assert await zb_hns_utils.download_range(0, 4, mrd) == b"xxxx"
    assert mrd.calls == 1


@pytest.mark.asyncio
async def test_download_ranges_requests_memoryview_chunks_when_supported():
    mrd = _MemoryviewDownloader()
    assert await zb_hns_utils.download_ranges([(0, 2), (5, 3)], mrd) == [
        b"xx",
        b"xxx",
    ]
    assert mrd.accepts_memoryview_args == [True]


@pytest.mark.asyncio
async def test_extended_gcsfs_mrd_downloads_forward_accepts_memoryview():
    from gcsfs.extended_gcsfs import ExtendedGcsFileSystem

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    fs = ExtendedGcsFileSystem.__new__(ExtendedGcsFileSystem)
    fs._memmove_executor = executor
    fs.MIN_CHUNK_SIZE_FOR_CONCURRENCY = (
        ExtendedGcsFileSystem.MIN_CHUNK_SIZE_FOR_CONCURRENCY
    )

    def _pool_for(mrd):
        pool = mock.MagicMock(spec=MRDPool)

        @contextlib.asynccontextmanager
        async def _get_mrd():
            yield mrd

        pool.get_mrd.side_effect = _get_mrd
        return pool

    try:
        mv_mrd = _MemoryviewDownloader()
        mv_pool = _pool_for(mv_mrd)
        assert await fs._concurrent_mrd_fetch(0, 6, 1, mv_pool) == b"xxxxxx"
        assert await fs._download_range_group(mv_pool, [(0, 0, 2), (1, 4, 3)]) == [
            b"xx",
            b"xxx",
        ]
        assert mv_mrd.accepts_memoryview_args == [True, True]

        legacy_mrd = _LegacyDownloader()
        legacy_pool = _pool_for(legacy_mrd)
        assert await fs._concurrent_mrd_fetch(0, 5, 1, legacy_pool) == b"xxxxx"
        assert await fs._download_range_group(legacy_pool, [(0, 0, 4)]) == [b"xxxx"]
        assert legacy_mrd.calls == 2
    finally:
        executor.shutdown()


def test_direct_memmove_buffer_copies_real_zero_copy_parser_chunks(monkeypatch):
    # The chunk shape google-cloud-storage's zero-copy parser produces: a
    # read-only view aliasing the whole received message. The copy must be
    # correct and independent of the message, which the view keeps alive.
    parser = pytest.importorskip("google.cloud.storage.asyncio._zero_copy_bidi_read")
    from google.cloud import _storage_v2

    assert zb_hns_utils.ZERO_COPY_BIDI_READ_ENV_VAR == parser._ENV_VAR
    assert zb_hns_utils._download_ranges_kwargs(AsyncMultiRangeDownloader) == {
        "accepts_memoryview": True
    }

    monkeypatch.setattr(parser, "_memoryview_supported", True)
    payload = bytes(range(256)) * 1024
    message = _storage_v2.BidiReadObjectResponse.serialize(
        _storage_v2.BidiReadObjectResponse(
            object_data_ranges=[
                _storage_v2.ObjectRangeData(
                    checksummed_data=_storage_v2.ChecksummedData(content=payload),
                    read_range=_storage_v2.ReadRange(
                        read_offset=0, read_length=len(payload), read_id=1
                    ),
                    range_end=True,
                )
            ]
        )
    )
    response = parser.deserialize(message)
    content = response.object_data_ranges[0].checksummed_data.content
    assert isinstance(content, memoryview) and content.readonly
    assert content.obj is message and len(content) == len(payload)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(len(payload), executor, max_pending=2)
    view = buf.get_view(0, len(payload))
    view.write(content).result()
    view.close()
    buf.close()
    result = buf.get_value()
    del response, content, message
    assert result == payload
    executor.shutdown()


def test_direct_memmove_buffer_overflow():
    """Tests that writing past the view boundaries raises a BufferError."""
    size = 10
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    # Fill the buffer exactly to capacity
    view.write(b"1234567890")

    # Attempting to write even 1 more byte should trigger the overflow protection
    with pytest.raises(BufferError, match="Attempted to write"):
        view.write(b"1")

    view.close()
    buf.close()
    executor.shutdown()


def test_direct_memmove_buffer_underflow():
    """Tests that closing an incompletely filled view/buffer raises a BufferError."""
    size = 10
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    # Write fewer bytes than the expected capacity
    view.write(b"12345")

    # Closing the view should detect that current_offset (5) < expected size (10)
    with pytest.raises(BufferError, match="Buffer contains uninitialized data"):
        view.close()

    # Calling get_value after an incompletely filled buffer should also error
    buf.close()
    with pytest.raises(BufferError, match="Buffer incomplete"):
        buf.get_value()

    executor.shutdown()


@mock.patch("gcsfs.zb_hns_utils.ctypes.memmove")
def test_direct_memmove_buffer_submit_failure(mock_memmove):
    """
    Tests that if executor.submit fails synchronously (e.g., executor is closed),
    the internal locks, semaphores, and events are properly reset, and close()
    does not hang.
    """
    # 1. Chunk > 128KB to force executor scheduling (skip the synchronous fast path)
    chunk_size = 130 * 1024

    # 2. Expected size > chunk_size to skip the Zero-Copy optimization
    expected_size = 140 * 1024

    data = b"a" * chunk_size

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(expected_size, executor, max_pending=2)
    view = buf.get_view(0, expected_size)

    # Mock the submit method to simulate a closed executor throwing a RuntimeError
    with mock.patch.object(
        executor, "submit", side_effect=RuntimeError("Executor closed")
    ):
        # The write operation should raise the simulated RuntimeError
        with pytest.raises(RuntimeError, match="Executor closed"):
            view.write(data)

    # Verify that the internal tracking state was correctly rolled back
    assert buf._pending_count == 0
    assert buf._done_event.is_set()

    # Calling close() should NOT hang. It should immediately raise the stored error.
    with pytest.raises(RuntimeError, match="Executor closed"):
        buf.close()

    executor.shutdown()


def test_direct_memmove_buffer_zero_copy():
    """Tests that a perfect aligned single payload avoids memory allocation completely."""
    data = b"exact_size_payload"
    size = len(data)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    # Writing a single payload identical to the expected size
    future = view.write(data)
    future.result()

    view.close()
    buf.close()

    # Should be the EXACT same string object returned without copying
    result = buf.get_value()
    assert result is data

    executor.shutdown()


def test_direct_memmove_buffer_overlapping_views():
    """Tests that getting overlapping views raises a ValueError."""
    size = 100
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)

    # Get a view for the first half
    _ = buf.get_view(0, 50)

    # Attempting to get an overlapping view should fail
    with pytest.raises(ValueError, match="Overlapping view requested"):
        _ = buf.get_view(25, 50)

    # Getting a view for the second half should succeed
    _ = buf.get_view(50, 50)

    buf.close()
    executor.shutdown()


@pytest.mark.asyncio
async def test_close_mrds():
    mrd1 = mock.AsyncMock()
    mrd2 = mock.AsyncMock()
    mrds = [mrd1, mrd2]

    await _close_mrds(mrds)

    mrd1.close.assert_awaited_once()
    mrd2.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_mrds_empty():
    await _close_mrds([])  # must not raise


@pytest.mark.asyncio
async def test_close_mrds_propagates_exception():
    bad_mrd = mock.AsyncMock()
    bad_mrd.close.side_effect = RuntimeError("boom")
    good_mrd = mock.AsyncMock()
    mrds = [bad_mrd, good_mrd]

    with pytest.raises(RuntimeError, match="boom"):
        await _close_mrds(mrds, raise_exception=True)

    good_mrd.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_mrds_logs_warning(caplog):
    bad_mrd = mock.AsyncMock()
    bad_mrd.close.side_effect = RuntimeError("boom")
    mrds = [bad_mrd]

    with caplog.at_level(logging.WARNING, logger="gcsfs"):
        await _close_mrds(mrds)

    assert "Error closing MRD: boom" in caplog.text


@pytest.fixture
def mock_cache():
    """A mock cache with an internal idle MRD queue."""
    queue = collections.deque()
    cache = mock.AsyncMock()
    cache.get_idle_mrd = mock.Mock(
        side_effect=lambda key: (queue.popleft() if queue else None)
    )
    cache.queue = queue
    return cache


@pytest.mark.asyncio
async def test_mrd_pool_get_mrd_from_local_free_queue(mock_cache):
    mock_mrd = mock.AsyncMock()
    mock_cache.queue.append(mock_mrd)

    mrd_pool = MRDPool(mock.Mock(), "bucket", "obj", "123", True, 2, cache=mock_cache)
    async with mrd_pool.get_mrd() as mrd:
        assert mrd is mock_mrd
        assert mrd_pool._active_count == 1
        assert mrd_pool._all_mrds == [mock_mrd]
        # The MRD has left the queue's free queue while in use
        assert len(mock_cache.queue) == 0
    # After release it stays in the mrd_pool's local free queue, not the shared queue
    assert mrd_pool._free_mrds.qsize() == 1
    assert len(mock_cache.queue) == 0


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_get_mrd_creates_when_empty(
    init_mrd_mock, mock_cache, mock_gcsfs
):
    new_mrd = mock.AsyncMock()
    init_mrd_mock.return_value = new_mrd

    mrd_pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 2, cache=mock_cache)
    async with mrd_pool.get_mrd() as mrd:
        assert mrd is new_mrd

    init_mrd_mock.assert_awaited_once()
    assert mrd_pool._all_mrds == [new_mrd]
    assert mrd_pool._active_count == 1


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_pool_size_cap(init_mrd_mock, mock_cache, mock_gcsfs):
    init_mrd_mock.side_effect = lambda *a, **kw: mock.AsyncMock()

    mrd_pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 2, cache=mock_cache)
    # Hold two slots open concurrently
    async with mrd_pool.get_mrd(), mrd_pool.get_mrd():
        assert mrd_pool._active_count == 2

        # A third concurrent get_mrd must NOT create a third MRD
        # Because we changed `mrd_supports_multi_request` to always True,
        # it will round-robin and share an existing MRD immediately
        async def third():
            async with mrd_pool.get_mrd():
                pass

        third_task = asyncio.create_task(third())
        await asyncio.wait_for(third_task, timeout=1.0)
        assert init_mrd_mock.await_count == 2  # never grew past 2


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_create_failure_decrements_active(
    init_mrd_mock, mock_cache, mock_gcsfs
):
    init_mrd_mock.side_effect = RuntimeError("init failed")
    mrd_pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", True, 2, cache=mock_cache)

    with pytest.raises(RuntimeError, match="init failed"):
        async with mrd_pool.get_mrd():
            pass

    assert mrd_pool._active_count == 0
    assert mrd_pool._all_mrds == []


@pytest.mark.asyncio
async def test_mrd_pool_close_donates_and_repools(mock_cache):
    captured = []

    async def mock_release(key, mrds):
        captured.append((key, list(mrds)))
        for mrd in mrds:
            mock_cache.queue.append(mrd)

    mock_cache.release = mock.AsyncMock(side_effect=mock_release)

    mock_mrd_a = mock.AsyncMock()
    mock_mrd_b = mock.AsyncMock()
    mrd_pool = MRDPool(mock.Mock(), "bucket", "obj", "123", True, 2, cache=mock_cache)
    mrd_pool._free_mrds.put_nowait(mock_mrd_a)
    mrd_pool._free_mrds.put_nowait(mock_mrd_b)
    mrd_pool._all_mrds.extend([mock_mrd_a, mock_mrd_b])
    mrd_pool._initialized = True

    await mrd_pool.close()

    assert mrd_pool._closed is True
    assert mrd_pool._free_mrds.qsize() == 0
    assert len(mock_cache.queue) == 2
    assert captured == [(mrd_pool._key, [mock_mrd_a, mock_mrd_b])]


@pytest.mark.asyncio
async def test_mrd_pool_close_idempotent(mock_cache):
    captured = []

    async def mock_release(key, mrds):
        captured.append((key, mrds))

    mock_cache.release = mock.AsyncMock(side_effect=mock_release)

    mrd_pool = MRDPool(mock.Mock(), "bucket", "obj", "123", True, 2, cache=mock_cache)
    await mrd_pool.close()
    await mrd_pool.close()  # second call is a no-op

    assert captured == [(mrd_pool._key, [])]


@pytest.mark.asyncio
async def test_mrd_pool_get_mrd_after_close_raises(mock_cache):
    mock_cache.release = mock.AsyncMock()

    mrd_pool = MRDPool(mock.Mock(), "bucket", "obj", "123", True, 2, cache=mock_cache)
    await mrd_pool.close()

    with pytest.raises(RuntimeError, match="MRDPool is closed"):
        async with mrd_pool.get_mrd():
            pass


@pytest.mark.asyncio
async def test_mrd_pool_close_concurrently(mock_cache):
    pool = MRDPool(mock.Mock(), "bucket", "obj", "123", True, 2, cache=mock_cache)

    # Mock release to take some time to yield control
    async def slow_release(key, mrds):
        await asyncio.sleep(0.1)

    mock_cache.release.side_effect = slow_release

    # Call close concurrently
    tasks = [asyncio.create_task(pool.close()) for _ in range(5)]
    await asyncio.gather(*tasks)

    assert mock_cache.release.call_count == 1


@pytest.mark.asyncio
async def test_mrd_pool_close_exception_handling(mock_cache):
    pool = MRDPool(mock.Mock(), "bucket", "obj", "123", True, 2, cache=mock_cache)
    mrd = mock.AsyncMock()
    pool._all_mrds.append(mrd)
    pool._free_mrds.put_nowait(mrd)
    pool._initialized = True

    mock_cache.release.side_effect = RuntimeError("release failed")

    with pytest.raises(RuntimeError, match="release failed"):
        await pool.close()

    assert pool._closed is True
    assert len(pool._all_mrds) == 0


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_get_creates_mrd_queue(init_mrd_mock, mock_gcsfs):
    mock_mrd = mock.AsyncMock()
    mock_mrd.persisted_size = 8
    init_mrd_mock.return_value = mock_mrd

    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)
    mrd_pool = await cache.get("bucket", "obj", "123", pool_size=2)

    assert mrd_pool.persisted_size == 8
    assert mrd_pool.pool_size == 2
    assert mrd_pool._cache is cache
    assert cache._refcounts[("bucket", "obj", "123", None)] == 1


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_get_cache_type(init_mrd_mock, mock_gcsfs):
    mock_mrd = mock.AsyncMock()
    mock_mrd.persisted_size = 8
    init_mrd_mock.return_value = mock_mrd

    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)
    mrd_pool = await cache.get(
        "bucket",
        "obj",
        "123",
        pool_size=2,
        cache_type="readahead",
        cache_source="explicit",
    )

    assert mrd_pool.cache_type == "readahead"
    assert mrd_pool.cache_source == "explicit"

    await mrd_pool.initialize()

    init_mrd_mock.assert_awaited_once_with(
        mock_gcsfs.grpc_client,
        "bucket",
        "obj",
        "123",
        cache_type="readahead",
        cache_source="explicit",
    )


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_get_shares_mrd_queue(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.return_value = mock.AsyncMock(persisted_size=0)

    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)
    a = await cache.get("bucket", "obj", "123", pool_size=2)
    a_queue = a._cache._mrd_queues[a._key]
    b = await cache.get("bucket", "obj", "123", pool_size=4)

    assert a_queue is b._cache._mrd_queues[b._key]
    assert cache._refcounts[("bucket", "obj", "123", None)] == 2
    assert init_mrd_mock.await_count == 2


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_get_distinct_keys(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.return_value = mock.AsyncMock(persisted_size=0)
    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)

    await cache.get("bucket", "obj-a", "1", pool_size=1)
    await cache.get("bucket", "obj-b", "1", pool_size=1)

    assert len(cache._mrd_queues) == 2
    assert init_mrd_mock.await_count == 2


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_get_init_failure_drops_entry(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.side_effect = RuntimeError("init boom")
    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)

    with pytest.raises(RuntimeError, match="init boom"):
        await cache.get("bucket", "obj", "1", pool_size=1)

    assert ("bucket", "obj", "1", None) not in cache._mrd_queues

    # A retry succeeds
    init_mrd_mock.side_effect = None
    init_mrd_mock.return_value = mock.AsyncMock(persisted_size=0)
    await cache.get("bucket", "obj", "1", pool_size=1)
    assert cache._refcounts[("bucket", "obj", "1", None)] == 1


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_get_init_failure_with_max_idle_zero(
    init_mrd_mock, mock_gcsfs
):
    init_mrd_mock.side_effect = RuntimeError("init boom")
    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=0)

    with pytest.raises(RuntimeError, match="init boom"):
        await cache.get("bucket", "obj", "1", pool_size=1)

    assert ("bucket", "obj", "1", None) not in cache._mrd_queues


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_release_refcount(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.return_value = mock.AsyncMock(persisted_size=0)
    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)

    a = await cache.get("bucket", "obj", "1", pool_size=1)
    b = await cache.get("bucket", "obj", "1", pool_size=1)

    await a.close()
    assert cache._refcounts[("bucket", "obj", "1", None)] == 1
    assert ("bucket", "obj", "1", None) not in cache._evictable_keys

    await b.close()
    assert ("bucket", "obj", "1", None) not in cache._refcounts
    assert ("bucket", "obj", "1", None) in cache._evictable_keys


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_lru_eviction(init_mrd_mock, mock_gcsfs):
    mock_mrds = []

    async def mock_create_mrd(*_a, **_kw):
        m = mock.AsyncMock(persisted_size=0)
        mock_mrds.append(m)
        return m

    init_mrd_mock.side_effect = mock_create_mrd

    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=2)

    # Open + close 3 distinct objects sequentially
    mrd_pools = []
    for i in range(3):
        mrd_pools.append(await cache.get("bucket", f"obj-{i}", "1", pool_size=1))
    for mrd_pool in mrd_pools:
        await mrd_pool.close()

    # Only the most recent 2 should remain
    assert ("bucket", "obj-0", "1", None) not in cache._mrd_queues
    assert ("bucket", "obj-1", "1", None) in cache._mrd_queues
    assert ("bucket", "obj-2", "1", None) in cache._mrd_queues
    assert list(cache._evictable_keys.keys()) == [
        ("bucket", "obj-1", "1", None),
        ("bucket", "obj-2", "1", None),
    ]
    # The evicted MRD queue's MRDs were torn down
    mock_mrds[0].close.assert_awaited_once()
    mock_mrds[1].close.assert_not_awaited()
    mock_mrds[2].close.assert_not_awaited()


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_pinned_never_evicted(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.side_effect = lambda *a, **kw: mock.AsyncMock(persisted_size=0)

    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=0)
    mrd_pools = [
        await cache.get("bucket", f"obj-{i}", "1", pool_size=1) for i in range(3)
    ]

    # All 3 still resident even with max_idle=0 since they're in use
    assert len(cache._mrd_queues) == 3
    assert len(cache._evictable_keys) == 0

    for mrd_pool in mrd_pools:
        await mrd_pool.close()

    # Now they should all be evicted (each release pushes idle past cap=0)
    assert cache._mrd_queues == {}


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_release_reuses_mrd_on_get(init_mrd_mock, mock_gcsfs):
    mock_mrds = []

    async def mock_create_mrd(*_a, **_kw):
        m = mock.AsyncMock(persisted_size=0)
        mock_mrds.append(m)
        return m

    init_mrd_mock.side_effect = mock_create_mrd

    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=4)
    a = await cache.get("bucket", "obj", "1", pool_size=1)
    a_mrd = a._all_mrds[0]
    await a.close()

    b = await cache.get("bucket", "obj", "1", pool_size=2)

    async with b.get_mrd() as m1:
        assert (
            m1 is a_mrd
        )  # Reused from cache (originally from a) during b.initialize()

        async with b.get_mrd() as m2:
            assert (
                m2 is mock_mrds[1]
            )  # Created on demand since cache is empty and pool size is 2

    assert init_mrd_mock.await_count == 2


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_close_tears_down_all(init_mrd_mock, mock_gcsfs):
    mock_mrds = []

    async def mock_create_mrd(*_a, **_kw):
        m = mock.AsyncMock(persisted_size=0)
        mock_mrds.append(m)
        return m

    init_mrd_mock.side_effect = mock_create_mrd

    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)
    a = await cache.get("bucket", "obj-a", "1", pool_size=1)
    assert a is not None
    mrd_pool_b = await cache.get("bucket", "obj-b", "1", pool_size=1)
    await mrd_pool_b.close()  # one idle, one pinned

    await cache.close()

    assert cache._mrd_queues == {}
    assert cache._evictable_keys == collections.OrderedDict()
    assert cache._closed is True

    await a.close()

    for m in mock_mrds:
        m.close.assert_awaited_once()

    # Subsequent get raises
    with pytest.raises(RuntimeError, match="MRDPoolCache is closed"):
        await cache.get("bucket", "obj-c", "1", pool_size=1)

    # Subsequent close is a no-op
    await cache.close()


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_close_no_op_when_already_closed(
    init_mrd_mock, mock_gcsfs
):
    cache = MRDPoolCache(mock_gcsfs)
    await cache.close()
    await cache.close()  # idempotent
    assert cache._closed is True


@pytest.mark.asyncio
async def test_mrd_pool_cache_get_idle_mrd_closed(mock_gcsfs):
    cache = MRDPoolCache(mock_gcsfs)
    await cache.close()
    assert cache.get_idle_mrd(("bucket", "obj", "1", None)) is None


def test_mrd_pool_cache_get_idle_mrd_not_found(mock_gcsfs):
    cache = MRDPoolCache(mock_gcsfs)
    assert cache.get_idle_mrd(("bucket", "obj", "1", None)) is None


@pytest.mark.asyncio
async def test_mrd_pool_cache_get_fs_gc(mock_gcsfs):
    cache = MRDPoolCache(mock_gcsfs)
    cache._gcsfs = lambda: None  # Simulate GC
    with pytest.raises(
        RuntimeError, match="ExtendedGcsFileSystem has been garbage collected"
    ):
        await cache.get("bucket", "obj", "1", pool_size=1)


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_max_queue_size_limit(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.side_effect = lambda *a, **kw: mock.AsyncMock(persisted_size=0)
    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=2, max_queue_size=2)

    # Create pools for same key
    pool_a = await cache.get("bucket", "obj", "1", pool_size=2)
    pool_b = await cache.get("bucket", "obj", "1", pool_size=2)

    # Let's get the MRDs from the pools so they scale up
    async with pool_a.get_mrd() as m1, pool_a.get_mrd() as m2:
        async with pool_b.get_mrd() as m3, pool_b.get_mrd() as m4:
            # We have 4 MRDs checked out across the pools for the same key
            mrd_instances = [m1, m2, m3, m4]

    # Now let's close both pools, which releases their MRDs back to the cache
    await pool_a.close()
    await pool_b.close()

    # The queue size should be exactly max_queue_size (2)
    queue = cache._mrd_queues[("bucket", "obj", "1", None)]
    assert len(queue) == 2

    # Two of the MRDs should have been closed
    closed_count = sum(1 for mrd in mrd_instances if mrd.close.call_count > 0)
    assert closed_count == 2


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_max_queue_size_zero(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.return_value = mock.AsyncMock(persisted_size=0)
    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=2, max_queue_size=0)

    pool = await cache.get("bucket", "obj", "1", pool_size=2)
    async with pool.get_mrd() as m1:
        mrd_instance = m1

    await pool.close()

    queue = cache._mrd_queues[("bucket", "obj", "1", None)]
    assert len(queue) == 0
    mrd_instance.close.assert_awaited_once()


def test_direct_memmove_buffer_zero_byte_write_after_zero_copy():
    """
    Tests that a zero-byte write (like a gRPC range completion chunk)
    does not crash the buffer, especially after the zero-copy fast path
    has left self._start_address as None.
    """
    data = b"exact_size_payload"
    size = len(data)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    # 1. Trigger the zero-copy fast path
    future1 = view.write(data)
    future1.result()

    # 2. Trigger the empty chunk write
    # This should return a completed future and NOT raise a BufferError
    future2 = view.write(b"")
    future2.result()

    view.close()
    buf.close()

    # Verify the payload was still handled via zero-copy successfully
    result = buf.get_value()
    assert result is data

    executor.shutdown()


def test_direct_memmove_buffer_zero_byte_write_closed_state():
    """
    Tests that zero-byte writes still respect the closed state of the buffer.
    """
    size = 10
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    # Force the buffer closed
    buf.close()

    # Even a zero-byte write should fail if the buffer is no longer accepting I/O
    with pytest.raises(ValueError, match="I/O operation on closed buffer."):
        view.write(b"")

    executor.shutdown()


def test_direct_memmove_buffer_zero_byte_write_error_state():
    """
    Tests that zero-byte writes still raise background errors if the buffer
    is in a failed state.
    """
    size = 10
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    # Manually inject a background error
    buf._error = RuntimeError("Simulated background failure")

    # A zero-byte write should surface the pending error
    with pytest.raises(RuntimeError, match="Simulated background failure"):
        view.write(b"")

    # Clean up the error so we can safely close the test
    buf._error = None
    buf._stop_accepting_writes = True
    executor.shutdown()


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_finalized_reuses_cached_mrd_on_init(
    init_mrd_mock, mock_cache, mock_gcsfs
):
    # Cached MRD
    cached_mrd = mock.AsyncMock()
    cached_mrd.persisted_size = 100
    mock_cache.queue.append(cached_mrd)

    # Create finalized pool (finalized=True)
    mrd_pool = MRDPool(
        mock_gcsfs,
        "bucket",
        "obj",
        "123",
        finalized=True,
        pool_size=1,
        cache=mock_cache,
    )

    # Initialize. Should reuse cached MRD.
    await mrd_pool.initialize()
    assert mrd_pool.persisted_size == 100
    init_mrd_mock.assert_not_called()  # Should not create new MRD
    assert len(mock_cache.queue) == 0  # Cached MRD was reused


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_unfinalized_reuses_cached_mrd_after_init(
    init_mrd_mock, mock_cache, mock_gcsfs
):
    # Cached MRD, opened when the object already had its current size
    cached_mrd = mock.AsyncMock()
    cached_mrd.persisted_size = 200
    mock_cache.queue.append(cached_mrd)

    # New MRD for initialization
    new_mrd = mock.AsyncMock()
    new_mrd.persisted_size = 200
    init_mrd_mock.return_value = new_mrd

    # Create unfinalized pool (finalized=False)
    mrd_pool = MRDPool(
        mock_gcsfs,
        "bucket",
        "obj",
        "123",
        finalized=False,
        pool_size=2,
        cache=mock_cache,
    )

    # 1. Initialize. Should create a new MRD (not reuse cached one) to get up-to-date size.
    await mrd_pool.initialize()
    assert mrd_pool.persisted_size == 200
    init_mrd_mock.assert_awaited_once()  # Created new MRD
    assert len(mock_cache.queue) == 1  # Cached MRD still in cache

    # 2. Get MRDs. First should be new_mrd, second should be cached_mrd (scale up).
    async with mrd_pool.get_mrd() as mrd1:
        assert mrd1 is new_mrd
        async with mrd_pool.get_mrd() as mrd2:
            assert mrd2 is cached_mrd
            assert len(mock_cache.queue) == 0  # Cached MRD was reused

    # init_mrd should not have been called again
    assert init_mrd_mock.await_count == 1


def _mrd_with_size(persisted_size):
    mrd = mock.AsyncMock()
    mrd.persisted_size = persisted_size
    return mrd


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_unfinalized_scale_up_skips_stale_idle_mrd(
    init_mrd_mock, mock_cache, mock_gcsfs
):
    # Appends keep the generation, so idle MRDs opened before an append share
    # the pool's cache key but carry an older persisted_size.
    stale_mrd = _mrd_with_size(100)
    current_mrd = _mrd_with_size(200)
    mock_cache.queue.extend([stale_mrd, current_mrd])
    init_mrd = _mrd_with_size(200)
    created_mrd = _mrd_with_size(200)
    init_mrd_mock.side_effect = [init_mrd, created_mrd]

    pool = MRDPool(
        mock_gcsfs,
        "bucket",
        "obj",
        "123",
        finalized=False,
        pool_size=3,
        cache=mock_cache,
    )
    await pool.initialize()
    assert pool.persisted_size == 200

    async with pool.get_mrd() as mrd1, pool.get_mrd() as mrd2:
        async with pool.get_mrd() as mrd3:
            assert mrd1 is init_mrd
            # Scale-up skips the stale idle MRD and takes the current one...
            assert mrd2 is current_mrd
            # ...then opens a new stream once the cache has nothing usable.
            assert mrd3 is created_mrd
    assert stale_mrd not in pool._all_mrds
    stale_mrd.close.assert_not_awaited()

    await pool.close()
    stale_mrd.close.assert_awaited_once()
    released = mock_cache.release.call_args.args[1]
    assert sorted(map(id, released)) == sorted(
        map(id, [init_mrd, current_mrd, created_mrd])
    )


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_finalized_scale_up_skips_stale_idle_mrd(
    init_mrd_mock, mock_cache, mock_gcsfs
):
    # Finalizing after an append keeps the generation, so idle MRDs opened
    # before the append are still cached under the finalized pool's key.
    stale_mrd = _mrd_with_size(100)
    mock_cache.queue.append(stale_mrd)
    created_mrd = _mrd_with_size(200)
    init_mrd_mock.return_value = created_mrd
    pool = MRDPool(
        mock_gcsfs,
        "bucket",
        "obj",
        "123",
        finalized=True,
        pool_size=1,
        cache=mock_cache,
    )
    pool.persisted_size = 200

    async with pool.get_mrd() as mrd:
        assert mrd is created_mrd
    assert pool._stale_mrds == [stale_mrd]


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_get_finalized_after_append_skips_stale_idle_mrd(
    init_mrd_mock, mock_gcsfs
):
    # A reader leaves an idle MRD opened at 100 bytes; the object then grows to
    # 150 bytes and is finalized under the same generation (same cache key).
    old_mrd = _mrd_with_size(100)
    new_mrd = _mrd_with_size(150)
    init_mrd_mock.side_effect = [old_mrd, new_mrd]
    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)

    mock_gcsfs._info.return_value = {"generation": "123", "size": 100}
    pool = await cache.get("bucket", "obj", "123", pool_size=1)
    await pool.close()

    mock_gcsfs._info.return_value = {
        "generation": "123",
        "size": 150,
        "timeFinalized": "2026-11-04T00:00:00Z",
    }
    pool = await cache.get("bucket", "obj", "123", pool_size=1)

    assert pool.persisted_size == 150
    async with pool.get_mrd() as mrd:
        assert mrd is new_mrd
    await pool.close()
    old_mrd.close.assert_awaited_once()


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_get_finalized_keeps_details_size_when_mrd_persisted_size_is_none(
    init_mrd_mock, mock_gcsfs
):
    # Redirected BidiReadObject opens that retry with a read_handle omit object
    # metadata in the server response, leaving mrd.persisted_size as None.
    redirected_mrd = _mrd_with_size(None)
    init_mrd_mock.return_value = redirected_mrd
    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)

    mock_gcsfs._info.return_value = {
        "generation": "123",
        "size": 134,
        "timeFinalized": "2026-11-06T00:00:00Z",
    }
    pool = await cache.get("bucket", "obj", "123", pool_size=1)
    assert pool.persisted_size == 134
    await pool.close()

    # Unfinalized objects still report None when the MRD has no persisted_size.
    mock_gcsfs._info.return_value = {"generation": "124", "size": 0}
    unfinalized_pool = await cache.get("bucket", "obj2", "124", pool_size=1)
    assert unfinalized_pool.persisted_size is None
    await unfinalized_pool.close()


@mock.patch("gcsfs.zb_hns_utils.HAS_CPYTHON_API", False)
def test_direct_memmove_buffer_pypy_fallback():
    """
    Tests that when HAS_CPYTHON_API is False (e.g., on PyPy), the buffer correctly
    falls back to using bytearray.
    """
    data1 = b"pypy_"
    source2 = bytearray(b"fallback")
    size = len(data1) + len(source2)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    # Use max_pending=2 and split the write to prevent the zero-copy fast path
    # from skipping the memory allocation phase.
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    future1 = view.write(data1)
    future2 = view.write(memoryview(source2))

    future1.result()
    future2.result()
    source2[:] = b"\x00" * len(source2)
    source2.clear()

    view.close()
    buf.close()

    result_bytes = buf.get_value()

    # 1. Verify the data is completely intact
    assert result_bytes == b"pypy_fallback"

    # 2. Verify it is standard Python bytes
    assert isinstance(result_bytes, bytes)

    # 3. Verify the fallback path was ACTUALLY taken by inspecting the internal buffer.
    assert isinstance(buf._result_bytes, bytearray)

    executor.shutdown()


@pytest.mark.asyncio
async def test_mrd_methods_raise_unrelated_type_error():
    """Tests that unrelated TypeErrors are properly re-raised."""
    mock_grpc_client = mock.Mock()

    # Test init_mrd re-raise
    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncMultiRangeDownloader.create_mrd",
        side_effect=TypeError("some other error"),
    ):
        with pytest.raises(TypeError, match="some other error"):
            await zb_hns_utils.init_mrd(
                mock_grpc_client, "b", "o", cache_type="readahead"
            )
