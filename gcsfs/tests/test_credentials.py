import datetime
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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


def test_no_gce_check_skips_cloud():
    """When NO_GCE_CHECK=true, the 'cloud' method should not be attempted."""
    with patch.dict(os.environ, {"NO_GCE_CHECK": "true"}):
        with patch.object(GoogleCredentials, "_connect_cloud") as mock_cloud:
            # google_default and cloud will fail; anon will succeed
            with patch.object(
                GoogleCredentials,
                "_connect_google_default",
                side_effect=ValueError("no default"),
            ):
                GoogleCredentials(project="myproject", token=None, access="read_only")
            mock_cloud.assert_not_called()


def test_connect_google_default_uses_request():
    with patch("gcsfs.credentials.gauth.default") as mock_default:
        mock_default.return_value = (Mock(), "my-project")
        _ = GoogleCredentials(project=None, token="google_default", access="read_only")

        # Check if gauth.default was called with a 'request' argument
        _, kwargs = mock_default.call_args
        assert "request" in kwargs
        from google.auth.transport.requests import Request

        assert isinstance(kwargs["request"], Request)


def test_connect_cloud_success():
    with patch("google.auth.compute_engine.Credentials") as mock_creds_class:
        mock_creds = Mock()
        mock_creds_class.return_value = mock_creds

        cred = GoogleCredentials(
            project="my-project", access="read_only", token="cloud", on_google=True
        )

        assert cred.credentials == mock_creds
        assert mock_creds.refresh.called
        assert cred.method == "cloud"


def test_connect_cloud_failure():
    import google.auth.exceptions

    with patch("google.auth.compute_engine.Credentials") as mock_creds_class:
        mock_creds = Mock()
        mock_creds_class.return_value = mock_creds
        mock_creds.refresh.side_effect = google.auth.exceptions.RefreshError(
            "mock error"
        )

        with pytest.raises(ValueError, match="Invalid gcloud credentials"):
            GoogleCredentials(
                project="my-project", access="read_only", token="cloud", on_google=True
            )


def test_connect_cloud_not_on_google():
    with pytest.raises(ValueError):
        GoogleCredentials(
            project="my-project", access="read_only", token="cloud", on_google=False
        )


@pytest.mark.parametrize("token", ["", "incorrect.token", "x" * 100])
def test_credentials_from_raw_token(token, monkeypatch):
    # An invalid token yields a 401 "Invalid Credentials", which gcsfs treats as
    # retriable (to ride out transient token-expiry races). This token is
    # permanently invalid, so all 6 retries fire with exponential backoff (~33s).
    # The backoff is not what this test verifies, so no-op the retry sleep to keep
    # it fast while still asserting the eventual error.
    async def _no_backoff(*_args, **_kwargs):
        return None

    monkeypatch.setattr("gcsfs.retry.asyncio.sleep", _no_backoff)
    with patch.dict(os.environ, {"FETCH_RAW_TOKEN_EXPIRY": "false"}):
        fs = GCSFileSystem(project="myproject", token=token)
        if not fs.on_google:
            pytest.skip("Emulator does not validate tokens")
        with pytest.raises(HttpError, match="Invalid Credentials"):
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


class _MetadataHandler(BaseHTTPRequestHandler):
    """Answers like a GCE metadata server for the default service account."""

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/":
            body = b""
        elif path == "/computeMetadata/v1/instance/service-accounts/default/":
            body = json.dumps(
                {"email": "sa@my-project.iam.gserviceaccount.com", "scopes": []}
            ).encode()
        elif path == "/computeMetadata/v1/instance/service-accounts/default/token":
            body = json.dumps(
                {"access_token": "metadata-token", "expires_in": 3600}
            ).encode()
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Metadata-Flavor", "Google")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def _point_metadata_at(monkeypatch, host):
    # google-auth reads GCE_METADATA_IP on each request, but GCE_METADATA_HOST
    # only on import
    monkeypatch.setenv("GCE_METADATA_IP", host)
    monkeypatch.setattr("google.auth.compute_engine._metadata._GCE_METADATA_HOST", host)
    monkeypatch.setattr(
        "google.auth.compute_engine._metadata.detect_gce_residency_linux",
        lambda: False,
    )
    monkeypatch.delenv("NO_GCE_CHECK", raising=False)
    # leave "cloud" as the only method before "anon", as on GCE when
    # google_default finds a different project
    monkeypatch.setattr(
        GoogleCredentials, "_connect_google_default", Mock(side_effect=ValueError)
    )
    monkeypatch.setattr(
        GoogleCredentials, "_connect_cache", Mock(side_effect=ValueError)
    )


@pytest.fixture
def fake_metadata_server(monkeypatch):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _MetadataHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    _point_metadata_at(monkeypatch, "127.0.0.1:%d" % server.server_port)
    yield
    server.shutdown()
    server.server_close()


@pytest.fixture
def no_metadata_server(monkeypatch):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    # nothing listens on this port, so connections are refused
    _point_metadata_at(monkeypatch, "127.0.0.1:%d" % port)


def test_connect_without_metadata_server_is_fast(no_metadata_server):
    start = time.monotonic()
    cred = GoogleCredentials(project="myproject", token=None, access="read_only")
    elapsed = time.monotonic() - start

    assert cred.method == "anon"
    # google-auth retries a failed token request with backoff for ~15s
    assert elapsed < 5


def test_connect_cloud_on_gce_vm_when_ping_fails(no_metadata_server, monkeypatch):
    # on a GCE VM, the token request is still made (and retried by google-auth)
    monkeypatch.setattr(
        "google.auth.compute_engine._metadata.detect_gce_residency_linux",
        lambda: True,
    )
    with patch("google.auth.compute_engine.Credentials") as mock_creds_class:
        cred = GoogleCredentials(project="myproject", token=None, access="read_only")

    assert cred.method == "cloud"
    assert mock_creds_class.return_value.refresh.called


@pytest.mark.parametrize("helper", ["ping", "detect_gce_residency_linux"])
def test_connect_cloud_without_private_metadata_helpers(
    no_metadata_server, monkeypatch, helper
):
    # if google-auth drops its private helpers, "cloud" is tried as before
    monkeypatch.delattr("google.auth.compute_engine._metadata." + helper)
    with patch("google.auth.compute_engine.Credentials") as mock_creds_class:
        cred = GoogleCredentials(project="myproject", token=None, access="read_only")

    assert cred.method == "cloud"
    assert mock_creds_class.return_value.refresh.called


def test_connect_cloud_with_metadata_server(fake_metadata_server):
    cred = GoogleCredentials(project="myproject", token=None, access="read_only")

    assert cred.method == "cloud"
    assert cred.credentials.token == "metadata-token"
    assert (
        cred.credentials.service_account_email
        == "sa@my-project.iam.gserviceaccount.com"
    )
    headers = {}
    cred.apply(headers)
    assert headers["authorization"] == "Bearer metadata-token"
