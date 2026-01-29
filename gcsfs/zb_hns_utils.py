import logging
from io import BytesIO

from google.cloud.storage._experimental.asyncio.async_appendable_object_writer import (
    _DEFAULT_FLUSH_INTERVAL_BYTES,
    AsyncAppendableObjectWriter,
)

logger = logging.getLogger("gcsfs")


async def download_range(offset, length, mrd):
    """
    Downloads a byte range from the file asynchronously.
    """
    # If length = 0, mrd returns till end of file, so handle that case here
    if length == 0:
        return b""
    buffer = BytesIO()
    await mrd.download_ranges([(offset, length, buffer)])
    data = buffer.getvalue()
    logger.info(
        f"Requested {length} bytes from offset {offset}, downloaded {len(data)} bytes"
    )
    return data


async def download_ranges(ranges, mrd):
    """
    Downloads multiple byte ranges from the file asynchronously in a single batch.
    
    Args:
        ranges: List of (offset, length) tuples to download
        mrd: AsyncMultiRangeDownloader instance
        
    Returns:
        List of bytes objects, one for each range
    """
    # Create buffers for each range
    range_tuples = []
    for offset, length in ranges:
        buffer = BytesIO()
        range_tuples.append((offset, length, buffer))
    
    # Download all ranges at once
    await mrd.download_ranges(range_tuples)
    
    # Extract data from buffers
    results = [buffer.getvalue() for _, _, buffer in range_tuples]
    
    total_requested = sum(length for _, length in ranges)
    total_downloaded = sum(len(r) for r in results)
    logger.info(
        f"Downloaded {len(results)} ranges: requested {total_requested} bytes, "
        f"downloaded {total_downloaded} bytes"
    )
    return results


async def init_aaow(
    grpc_client, bucket_name, object_name, generation=None, flush_interval_bytes=None
):
    """
    Creates and opens the AsyncAppendableObjectWriter.
    """
    writer_options = {}
    # Only pass flush_interval_bytes if the user explicitly provided a
    # non-default flush interval.
    if flush_interval_bytes and flush_interval_bytes != _DEFAULT_FLUSH_INTERVAL_BYTES:
        writer_options["FLUSH_INTERVAL_BYTES"] = flush_interval_bytes
    writer = AsyncAppendableObjectWriter(
        client=grpc_client,
        bucket_name=bucket_name,
        object_name=object_name,
        generation=generation,
        writer_options=writer_options,
    )
    await writer.open()
    return writer
