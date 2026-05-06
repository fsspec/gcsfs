import multiprocessing
import os
import pickle
from concurrent.futures import ProcessPoolExecutor

import pytest
import requests
from requests.exceptions import ProxyError

from gcsfs.retry import (
    DEFAULT_RETRY_CONFIG,
    HttpError,
    get_storage_control_retry_config,
    is_retriable,
    validate_response,
)
from gcsfs.tests.settings import TEST_BUCKET
from gcsfs.tests.utils import tmpfile


def test_storage_control_retry_config_from_kwargs():
    kwargs = {"retry_timeout": 15.0, "retry_initial": 3.0, "other_arg": "value"}
    cfg = {k: v for k, v in kwargs.items() if k.startswith("retry_") and v is not None}
    assert cfg["retry_timeout"] == 15.0
    assert cfg["retry_initial"] == 3.0
    assert "retry_maximum" not in cfg


@pytest.mark.parametrize(
    "kwargs, base_config, expected",
    [
        # 1. Defaults only
        ({}, None, {"timeout": DEFAULT_RETRY_CONFIG.get("timeout")}),
        # 2. FS Config override
        ({}, {"timeout": 10.0}, {"timeout": 10.0}),
        # 3. Call-site override (highest priority)
        (
            {"timeout": 5.0},
            {"timeout": 10.0},
            {"timeout": 5.0},
        ),
        # 4. Partial override (Call-site has timeout, FS has initial)
        (
            {"timeout": 7.0},
            {"initial": 2.0},
            {"timeout": 7.0, "initial": 2.0},
        ),
    ],
)
def test_get_storage_control_retry_config_resolution(kwargs, base_config, expected):
    retry = get_storage_control_retry_config(base_config, **kwargs)
    for attr, val in expected.items():
        assert getattr(retry, f"_{attr}") == val


from unittest import mock

from google.api_core import exceptions as api_exceptions


@pytest.mark.asyncio
async def test_get_storage_control_retry_config_execution():
    # Create a config with very short delays for testing
    base_cfg = {"initial": 0.001, "maximum": 0.01}
    retry = get_storage_control_retry_config(base_config=base_cfg)

    mock_func = mock.AsyncMock()
    mock_func.side_effect = [
        api_exceptions.ServiceUnavailable("Transient error 1"),
        api_exceptions.ServiceUnavailable("Transient error 2"),
        "success",
    ]

    # In the real client, the method is wrapped with the retry object
    # AsyncRetry objects are callable and take the function to wrap
    wrapped_func = retry(mock_func)

    result = await wrapped_func()

    assert result == "success"
    assert mock_func.call_count == 3


@pytest.mark.asyncio
async def test_get_storage_control_retry_config_non_retriable():
    base_cfg = {"initial": 0.001, "maximum": 0.01}
    retry = get_storage_control_retry_config(base_config=base_cfg)

    mock_func = mock.AsyncMock()
    # 404 is NOT in our transient exceptions list
    mock_func.side_effect = api_exceptions.NotFound("Not Found")

    wrapped_func = retry(mock_func)

    with pytest.raises(api_exceptions.NotFound):
        await wrapped_func()

    # Should only be called once because NotFound is not retriable
    assert mock_func.call_count == 1


def test_tempfile():
    with tmpfile() as fn:
        with open(fn, "w"):
            pass
        assert os.path.exists(fn)
    assert not os.path.exists(fn)


def test_retriable_exception():
    e = requests.exceptions.Timeout()
    assert is_retriable(e)
    e = ValueError
    assert not is_retriable(e)

    e = HttpError({"message": "", "code": 500})
    assert is_retriable(e)

    e = HttpError({"message": "", "code": "500"})
    assert is_retriable(e)

    e = HttpError({"message": "", "code": 400})
    assert not is_retriable(e)

    e = HttpError({"code": "429"})
    assert is_retriable(e)

    e = ProxyError()
    assert is_retriable(e)


def test_pickle_serialization():
    expected = HttpError({"message": "", "code": 400})

    # Serialize/Deserialize
    serialized = pickle.dumps(expected)
    actual = pickle.loads(serialized)

    is_same_type = type(expected) is type(actual)
    is_same_args = expected.args == actual.args

    assert is_same_type and is_same_args


def conditional_exception(process_id):
    # Raise only on second process (id=1)
    if process_id == 1:
        raise HttpError({"message": "", "code": 400})


def test_multiprocessing_error_handling():
    # Ensure spawn context to avoid forking issues
    ctx = multiprocessing.get_context("spawn")

    # Run on two processes
    with ProcessPoolExecutor(2, mp_context=ctx) as p:
        results = p.map(conditional_exception, range(2))

    with pytest.raises(HttpError):
        _ = [result for result in results]


def test_validate_response():
    validate_response(200, None, "/path")

    # HttpError with no JSON body
    with pytest.raises(HttpError) as e:
        validate_response(503, b"", "/path")
    assert e.value.code == 503
    assert e.value.message == ", 503"

    # HttpError with JSON body
    j = '{"error": {"code": 503, "message": "Service Unavailable"}}'
    with pytest.raises(HttpError) as e:
        validate_response(503, j, "/path")
    assert e.value.code == 503
    assert e.value.message == "Service Unavailable, 503"

    # 403
    j = '{"error": {"message": "Not ok"}}'
    with pytest.raises(IOError, match="Forbidden: /path\nNot ok"):
        validate_response(403, j, "/path")

    # 404
    with pytest.raises(FileNotFoundError):
        validate_response(404, b"", "/path")

    # 502
    with pytest.raises(ProxyError):
        validate_response(502, b"", "/path")


def test_validate_response_error_is_string():
    # HttpError with JSON body
    j = '{"error": "Too Many Requests"}'
    with pytest.raises(HttpError) as e:
        validate_response(429, j, "/path")
    assert e.value.code == 429
    assert e.value.message == "Too Many Requests, 429"


def test_validate_response_content_none():
    with pytest.raises(HttpError) as e:
        validate_response(429, None, "/path")
    assert e.value.code == 429
    assert e.value.message == ", 429"


def test_validate_response_invalid_json():
    content = "This is a raw plain-text error"
    with pytest.raises(HttpError) as e:
        validate_response(400, content, "/path")
    assert e.value.code == 400
    assert e.value.message == "This is a raw plain-text error, 400"


@pytest.mark.parametrize(
    ["file_path", "validate_get_error", "validate_list_error", "expected_error"],
    [
        (
            "/missing",
            FileNotFoundError,
            None,
            FileNotFoundError,
        ),  # Not called
        (
            "/missing",
            OSError("Forbidden"),
            FileNotFoundError,
            FileNotFoundError,
        ),
        (
            "/2014-01-01.csv",
            None,
            None,
            None,
        ),
        (
            "/2014-01-01.csv",
            OSError("Forbidden"),
            None,
            None,
        ),
    ],
    ids=[
        "missing_with_get_perms",
        "missing_with_list_perms",
        "existing_with_get_perms",
        "existing_with_list_perms",
    ],
)
def test_metadata_read_permissions(
    file_path, validate_get_error, validate_list_error, expected_error, gcs
):
    def _validate_response(self, status, content, path):
        if path.endswith(f"/o{file_path}") and validate_get_error is not None:
            raise validate_get_error
        if path.endswith("/o/") and validate_list_error is not None:
            raise validate_list_error
        validate_response(status, content, path)

    if expected_error is None:
        gcs.ls(TEST_BUCKET + file_path)
        gcs.info(TEST_BUCKET + file_path)
        assert gcs.exists(TEST_BUCKET + file_path)
    else:
        with pytest.raises(expected_error):
            gcs.ls(TEST_BUCKET + file_path)
        with pytest.raises(expected_error):
            gcs.info(TEST_BUCKET + file_path)
        assert gcs.exists(TEST_BUCKET + file_path) is False
