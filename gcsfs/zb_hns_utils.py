from io import BytesIO

from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import \
    AsyncMultiRangeDownloader


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
    # If length = 0, mrd returns till end of file, so handle that case here
    if length == 0:
        return b""
    buffer = BytesIO()
    await mrd.download_ranges([(offset, length, buffer)])
    return buffer.getvalue()
