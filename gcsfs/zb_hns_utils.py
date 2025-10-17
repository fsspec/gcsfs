from io import BytesIO

from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import AsyncMultiRangeDownloader


async def create_mrd(grpc_client, bucket_name, object_name, generation=None):
    """
    Creates the AsyncMultiRangeDownloader.
    """
    mrd = await AsyncMultiRangeDownloader.create_mrd(
        grpc_client, bucket_name, object_name, generation
    )
    return mrd


async def download_range(offset, length, mrd):
    """
    Downloads a byte range from the file asynchronously.
    """
    buffer = BytesIO()
    await mrd.download_ranges([(offset, length, buffer)])
    return buffer.getvalue()
