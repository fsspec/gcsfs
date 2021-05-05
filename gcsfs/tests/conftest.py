import pytest

from gcsfs.core import GCSFileSystem, GoogleCredentials
from gcsfs.tests.settings import TEST_PROJECT, TEST_BUCKET
import vcr.stubs.aiohttp_stubs as aios


import fsspec.config

fsspec.config.conf.pop("gcs", None)


@pytest.fixture
def token_restore():
    cache = GoogleCredentials.tokens
    try:
        GoogleCredentials.tokens = {}
        yield
    finally:
        GoogleCredentials.tokens = cache
        GoogleCredentials._save_tokens()
        GCSFileSystem.clear_instance_cache()


# patch; for some reason, original wants vcr_response["url"], which is empty
def build_response(vcr_request, vcr_response, history):
    request_info = aios.RequestInfo(
        url=aios.URL(vcr_request.url),
        method=vcr_request.method,
        headers=aios.CIMultiDictProxy(aios.CIMultiDict(vcr_request.headers)),
        real_url=aios.URL(vcr_request.url),
    )
    response = aios.MockClientResponse(
        vcr_request.method, aios.URL(vcr_request.url), request_info=request_info
    )
    response.status = vcr_response["status"]["code"]
    response._body = vcr_response["body"].get("string", b"")
    response.reason = vcr_response["status"]["message"]
    head = {
        k: v[0] if isinstance(v, list) else v
        for k, v in vcr_response["headers"].items()
    }
    response._headers = aios.CIMultiDictProxy(aios.CIMultiDict(head))
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
                response.content._buffer[0] = response.content._buffer[0][
                    response.content._buffer_offset :
                ]
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
        "url": str(response.url)
        .replace(TEST_BUCKET, "gcsfs-testing")
        .replace(TEST_PROJECT, "test_project"),
    }

    cassette.append(vcr_request, vcr_response)


aios.record_response = record_response


def play_responses(cassette, vcr_request):
    history = []
    vcr_response = cassette.play_response(vcr_request)
    response = build_response(vcr_request, vcr_response, history)

    # If we're following redirects, continue playing until we reach
    # our final destination.
    while 300 <= response.status <= 399:
        if "Location" not in response.headers:
            # Not a redirect, an instruction not to call again
            break
        next_url = aios.URL(response.url).with_path(response.headers["location"])

        # Make a stub VCR request that we can then use to look up the recorded
        # VCR request saved to the cassette. This feels a little hacky and
        # may have edge cases based on the headers we're providing (e.g. if
        # there's a matcher that is used to filter by headers).
        vcr_request = aios.Request(
            "GET",
            str(next_url),
            None,
            aios._serialize_headers(response.request_info.headers),
        )
        vcr_request = cassette.find_requests_with_most_matches(vcr_request)[0][0]

        # Tack on the response we saw from the redirect into the history
        # list that is added on to the final response.
        history.append(response)
        vcr_response = aios.cassette.play_response(vcr_request)
        response = aios.build_response(vcr_request, vcr_response, history)

    return response


aios.play_responses = play_responses
