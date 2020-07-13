import pytest

from gcsfs.core import GCSFileSystem
import vcr.stubs.aiohttp_stubs as aios


@pytest.yield_fixture
def token_restore():
    cache = GCSFileSystem.tokens
    try:
        GCSFileSystem.tokens = {}
        yield
    finally:
        GCSFileSystem.tokens = cache
        GCSFileSystem._save_tokens()
        GCSFileSystem.clear_instance_cache()


# patch; for some reason, original wants vcr_response["url"], which is empty
def build_response(vcr_request, vcr_response, history):
    request_info = aios.RequestInfo(
        url=aios.URL(vcr_request.url),
        method=vcr_request.method,
        headers=aios.CIMultiDictProxy(aios.CIMultiDict(vcr_request.headers)),
        real_url=aios.URL(vcr_request.url),
    )
    response = aios.MockClientResponse(vcr_request.method, aios.URL(vcr_request.url), request_info=request_info)
    response.status = vcr_response["status"]["code"]
    response._body = vcr_response["body"].get("string", b"")
    response.reason = vcr_response["status"]["message"]
    response._headers = aios.CIMultiDictProxy(aios.CIMultiDict(vcr_response["headers"]))
    response._history = tuple(history)

    response.close()
    return response

aios.build_response = build_response


# patch: but the value of body back in the stream, to enable streaming reads
# https://github.com/kevin1024/vcrpy/pull/509
async def record_response(cassette, vcr_request, response):
    """Record a VCR request-response chain to the cassette."""

    try:
        byts = await response.read()
        body = {"string": byts}
        if byts:
            if response.content._buffer_offset:
                response.content._buffer[0] = (
                    response.content._buffer[0][response.content._buffer_offset :]
                )
                response.content._buffer_offset = 0
            response.content._size += len(byts)
            response.content._cursor -= len(byts)
            response.content._buffer.appendleft(byts)
            response.content._eof_counter = 0

    except aios.ClientConnectionError:
        body = {}

    vcr_response = {
        "status": {"code": response.status, "message": response.reason},
        "headers": aios._serialize_headers(response.headers),
        "body": body,  # NOQA: E999
        "url": str(response.url),
    }

    cassette.append(vcr_request, vcr_response)


aios.record_response = record_response
