import datetime
import os
from unittest.mock import Mock, patch

import pytest

from gcsfs import GCSFileSystem
from gcsfs.credentials import GoogleCredentials
from gcsfs.retry import HttpError, NonRetryableError

MOCK_TOKEN_STR = "ya29.valid_raw_token_string"
MOCK_EXP_TIMESTAMP = 1764620492  # 2025-12-01 20:21:32 UTC


def test_googlecredentials_none():
    credentials = GoogleCredentials(project="myproject", token=None, access="read_only")
    headers = {}
    credentials.apply(headers)


@pytest.mark.parametrize("token", ["", "incorrect.token", "x" * 100])
def test_credentials_from_raw_token(token):
    with patch.dict(os.environ, {"FETCH_RAW_TOKEN_EXPIRY": "false"}):
        with pytest.raises(HttpError, match="Invalid Credentials"):
            fs = GCSFileSystem(project="myproject", token=token)
            fs.ls("/")


@pytest.fixture
def mock_token_info_api_response():
    """Returns a mock response object that mimics a valid Google Token Info response"""
    resp = Mock()
    resp.status_code = 200
    resp.json.return_value = {"exp": str(MOCK_EXP_TIMESTAMP)}
    return resp


def test_raw_token_credentials_init_with_raw_token_fetches_expiry(
    mock_token_info_api_response,
):
    """
    Test that initializing GoogleCredentials with a raw string token
    triggers the API lookup and sets the expiry.
    """
    future_time = int(
        (
            datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(seconds=600)
        ).timestamp()
    )
    mock_token_info_api_response.json.return_value = {"exp": str(future_time)}

    with patch(
        "gcsfs.credentials.requests.get", return_value=mock_token_info_api_response
    ) as mock_get:
        creds = GoogleCredentials(
            project="my-project", token=MOCK_TOKEN_STR, access="read_only"
        )
        mock_get.assert_called_once_with(
            "https://oauth2.googleapis.com/tokeninfo",
            params={"access_token": MOCK_TOKEN_STR},
            timeout=10,
        )

        assert creds.credentials.token == MOCK_TOKEN_STR
        assert creds.credentials.expiry is not None
        assert creds.credentials.expiry == datetime.datetime.utcfromtimestamp(
            future_time
        )


def test_raw_token_credentials_init_env_var_disables_fetch(
    mock_token_info_api_response,
):
    """Test that the FETCH_RAW_TOKEN_EXPIRY environment variable stops the network call."""
    with patch.dict(os.environ, {"FETCH_RAW_TOKEN_EXPIRY": "false"}):
        with patch(
            "gcsfs.credentials.requests.get", return_value=mock_token_info_api_response
        ) as mock_get:
            creds = GoogleCredentials(
                project="my-project", token=MOCK_TOKEN_STR, access="read_only"
            )
            mock_get.assert_not_called()
            assert creds.credentials.token == MOCK_TOKEN_STR
            assert creds.credentials.expiry is None


def test_raw_token_credentials_init_raises_on_invalid_token(
    mock_token_info_api_response,
):
    """Test that if the API returns 400 (Bad Request), the class initialization fails."""
    mock_token_info_api_response.status_code = 400
    mock_token_info_api_response.json.return_value = {"error": "invalid_token"}

    with patch(
        "gcsfs.credentials.requests.get", return_value=mock_token_info_api_response
    ):
        with pytest.raises(ValueError, match="Provided token is either not valid"):
            GoogleCredentials(
                project="my-project", token="bad_token_string", access="read_only"
            )


def test_raw_token_credentials_refresh_throws_error_after_expiry(
    mock_token_info_api_response,
):
    """Tests that raw token cred refresh throws error after expiry."""
    future_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        seconds=600
    )
    mock_token_info_api_response.json.return_value = {
        "exp": str(int(future_time.timestamp()))
    }

    with patch(
        "gcsfs.credentials.requests.get", return_value=mock_token_info_api_response
    ) as _:
        creds = GoogleCredentials(
            project="my-project", token="my_token", access="read_only"
        )

    # Refresh before expiry
    with patch("gcsfs.credentials.requests.Session") as mock_session:
        creds.maybe_refresh()
        mock_session.assert_not_called()

    creds.credentials.expiry = datetime.datetime.utcnow() - datetime.timedelta(
        minutes=10
    )

    # Refresh after expiry
    with pytest.raises(
        NonRetryableError, match="Got error while refreshing credentials"
    ):
        creds.maybe_refresh()


def test_raw_token_credentials_init_raises_on_short_lived_token(
    mock_token_info_api_response,
):
    """
    Test that if the token expires too soon (less than the safety buffer),
    we raise a ValueError immediately to warn the user.
    """
    future_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        minutes=2
    )
    mock_token_info_api_response.json.return_value = {
        "exp": str(int(future_time.timestamp()))
    }

    with patch(
        "gcsfs.credentials.requests.get", return_value=mock_token_info_api_response
    ):
        with pytest.raises(ValueError, match="less than the safety buffer"):
            GoogleCredentials(
                project="my-project", token="short_lived_token", access="read_only"
            )
