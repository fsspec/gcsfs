"""
End-to-end telemetry tests: real upstream frameworks driving gcsfs against a GCS
endpoint over the JSON API (HTTP) path.

Where these run:
- Cloud Build e2e `standard` suite: real GCS, regional bucket (GCSFileSystem /
  ExtendedGcsFileSystem on a flat bucket).
- Cloud Build e2e `hns` suite: real GCS, HNS bucket.
- GitHub CI: the fake GCS emulator.

Rapid (zonal) and Rapid Cache Ultra buckets are intentionally not covered: their
data path uses gRPC bidi streaming APIs rather than the HTTP JSON API path.

Unlike `test_upstream_frameworks_real.py`, nothing in gcsfs is mocked here. Requests
go through the real `_call` / `_request` / aiohttp path, and the outgoing
`X-Goog-Storage-Gcsfs-Features` and `User-Agent` headers are recorded at the aiohttp
layer, i.e. exactly what is sent over the wire.
"""

from __future__ import annotations

import asyncio
import base64
import http.server
import threading
import uuid
from urllib.parse import urlparse

import aiohttp
import fsspec
import pytest

import gcsfs.telemetry.detector as detector_mod
from gcsfs.telemetry.context import (
    HTTP_HEADER_KEY,
    Dimension,
    reset_telemetry_context,
    set_telemetry_context,
)
from gcsfs.telemetry.detector import EnvironmentDetector, FrameworkDetector
from gcsfs.telemetry.features_proto import (
    GcsfsEnvironment,
    GcsfsFeatures,
    GcsfsFramework,
    GcsfsMethod,
)
from gcsfs.telemetry.manager import default_usage_tracker
from gcsfs.tests.settings import TEST_BUCKET

# Captured at import time, before session conftest stubs EnvironmentDetector.detect.
_REAL_ENVIRONMENT_DETECT = EnvironmentDetector.detect


class _WireRecorder:
    """Records User-Agent and feature headers of every HTTP request sent to the GCS endpoint."""

    def __init__(self, gcs_netloc: str):
        self.gcs_netloc = gcs_netloc
        self.user_agents: list[str] = []
        self.feature_headers: list[str] = []

    def clear(self):
        self.user_agents.clear()
        self.feature_headers.clear()

    def assert_all_tagged(self, fw: str):
        assert self.user_agents, "no HTTP requests reached the GCS endpoint"
        assert len(self.feature_headers) == len(self.user_agents)
        polluted = [ua for ua in self.user_agents if "fw/" in ua or "env/" in ua]
        assert not polluted, (
            f"{len(polluted)}/{len(self.user_agents)} GCS requests leaked telemetry "
            f"into User-Agent: {polluted[:5]}"
        )
        expected_fw = GcsfsFramework[f"GCSFS_FRAMEWORK_{fw.upper()}"]
        for idx, h in enumerate(self.feature_headers):
            assert h, (
                f"request {idx + 1}/{len(self.feature_headers)} missing "
                f"{HTTP_HEADER_KEY}"
            )
            try:
                decoded = GcsfsFeatures.deserialize(base64.urlsafe_b64decode(h))
            except Exception as exc:
                raise AssertionError(
                    f"request {idx + 1}/{len(self.feature_headers)} invalid "
                    f"{HTTP_HEADER_KEY} {h!r}: {exc}"
                ) from exc
            assert decoded.framework == expected_fw, (
                f"request {idx + 1}/{len(self.feature_headers)} expected "
                f"{expected_fw.name}, got {decoded.framework.name}"
            )


@pytest.fixture
def wire(gcs, monkeypatch):
    """
    Point frameworks at the same GCS endpoint/credentials as the `gcs` fixture and
    record outgoing User-Agent and feature headers at the aiohttp layer.
    """
    # Filesystems created internally by frameworks (pandas, Lightning, ...) pick up
    # the test endpoint/credentials through fsspec's per-protocol config defaults.
    storage_options = dict(gcs.storage_options)
    for proto in ("gs", "gcs"):
        monkeypatch.setitem(fsspec.config.conf, proto, storage_options)

    recorder = _WireRecorder(urlparse(gcs._location).netloc)
    feature_key_lower = HTTP_HEADER_KEY.lower()
    # ClientSession.request()/get()/... all funnel through the private `_request` (aiohttp 3.x).
    assert hasattr(
        aiohttp.ClientSession, "_request"
    ), "aiohttp.ClientSession._request not found; update the e2e request recorder"
    original_request = aiohttp.ClientSession._request

    async def recording_request(self, method, str_or_url, *args, **kwargs):
        if urlparse(str(str_or_url)).netloc == recorder.gcs_netloc:
            headers = kwargs.get("headers") or {}
            ua = (
                next((v for k, v in headers.items() if k.lower() == "user-agent"), "")
                or ""
            )
            recorder.user_agents.append(ua)
            feature_header = next(
                (v for k, v in headers.items() if k.lower() == feature_key_lower),
                "",
            )
            recorder.feature_headers.append(feature_header or "")
        return await original_request(self, method, str_or_url, *args, **kwargs)

    monkeypatch.setattr(aiohttp.ClientSession, "_request", recording_request)
    # Fresh instances so frameworks don't reuse a cached filesystem from another test
    gcs.clear_instance_cache()
    yield recorder
    gcs.clear_instance_cache()


def _unique_prefix() -> str:
    return f"{TEST_BUCKET}/telemetry-e2e/{uuid.uuid4().hex}"


def test_pandas_csv_round_trip_e2e(gcs, wire):
    """`df.to_csv` / `pd.read_csv` on gs:// URLs report `pandas` in X-Goog-Storage-Gcsfs-Features."""
    pd = pytest.importorskip("pandas")
    url = f"gs://{_unique_prefix()}/data.csv"
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    wire.clear()
    df.to_csv(url, index=False)
    wire.assert_all_tagged("pandas")

    wire.clear()
    out = pd.read_csv(url)
    wire.assert_all_tagged("pandas")
    pd.testing.assert_frame_equal(out, df)


def test_torch_dataloader_reads_e2e(gcs, wire):
    """Reads issued from a torch `Dataset` inside a `DataLoader` report `torch` in X-Goog-Storage-Gcsfs-Features."""
    torch = pytest.importorskip("torch")
    from torch.utils.data import DataLoader, Dataset

    prefix = _unique_prefix()
    for i in range(3):
        gcs.pipe(f"{prefix}/sample_{i}.bin", bytes([i]) * 16)

    class GCSDataset(Dataset):
        def __len__(self):
            return 3

        def __getitem__(self, idx):
            with fsspec.open(f"gs://{prefix}/sample_{idx}.bin", "rb") as f:
                data = f.read()
            return torch.tensor(list(data[:4]), dtype=torch.uint8)

    wire.clear()
    batches = [batch for batch in DataLoader(GCSDataset(), batch_size=1)]
    assert len(batches) == 3
    wire.assert_all_tagged("torch")


def test_lightning_checkpoint_save_e2e(gcs, wire, tmp_path):
    """Lightning checkpoint I/O (`trainer.save_checkpoint`) reports `lightning` in X-Goog-Storage-Gcsfs-Features."""
    lightning = pytest.importorskip("lightning")
    import torch

    class TinyModule(lightning.LightningModule):
        def __init__(self):
            super().__init__()
            self.layer = torch.nn.Linear(2, 1)

    trainer = lightning.Trainer(
        accelerator="cpu",
        logger=False,
        enable_checkpointing=False,
        enable_progress_bar=False,
        default_root_dir=tmp_path,
    )
    trainer.strategy.connect(TinyModule())
    path = f"{_unique_prefix()}/model.ckpt"

    wire.clear()
    trainer.save_checkpoint(f"gs://{path}")
    wire.assert_all_tagged("lightning")
    assert gcs.exists(path)


def test_adk_artifact_service_io_pattern_e2e(gcs, wire):
    """
    ADK `FsspecArtifactService` I/O (`asyncio.to_thread` into a `google.adk` function)
    reports `adk` in X-Goog-Storage-Gcsfs-Features on every request.

    google-adk is not a test dependency. Detection only reads `__name__` from frame
    globals, so a function defined with the real ADK module name stands in for ADK.
    """
    adk_globals = {
        "__name__": "google.adk.integrations.fsspec._fsspec_artifact_service"
    }
    exec(
        "def save_list_load(fs, prefix, path):\n"
        "    fs.pipe_file(path, b'artifact')\n"
        "    return fs.find(prefix), fs.cat_file(path)\n",
        adk_globals,
    )
    prefix = _unique_prefix()
    path = f"{prefix}/app/user/session/report.txt/0"

    wire.clear()
    listing, data = asyncio.run(
        asyncio.to_thread(adk_globals["save_list_load"], gcs, prefix, path)
    )
    wire.assert_all_tagged("adk")
    assert listing == [path]
    assert data == b"artifact"


def test_env_token_on_first_op_after_filesystem_init(
    gcs, skip_if_grpc_read, wire, monkeypatch, tmp_path
):
    """Init warmup probes metadata and reports gce in X-Goog-Storage-Gcsfs-Features."""
    path = f"{_unique_prefix()}/obj.bin"
    gcs.pipe(path, b"hello world")

    recorded_mds: list[tuple[str, str | None]] = []

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            recorded_mds.append((self.path, self.headers.get("Metadata-Flavor")))
            self.send_response(404)
            self.send_header("Metadata-Flavor", "Google")
            self.end_headers()

        def log_message(self, format, *args):
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        for name in (
            "NO_GCE_CHECK",
            "DO_NOT_TRACK",
            "GCSFS_NO_TELEMETRY",
            "KUBERNETES_SERVICE_HOST",
            "GCE_METADATA_HOST",
            "GCE_METADATA_ROOT",
            "GCE_METADATA_IP",
            "K_SERVICE",
            "K_REVISION",
            "CLOUD_RUN_JOB",
            "CLOUD_RUN_EXECUTION",
            "CLOUD_RUN_WORKER_POOL",
            "CLOUD_RUN_REVISION",
            "GAE_SERVICE",
            "GAE_INSTANCE",
        ):
            monkeypatch.delenv(name, raising=False)

        monkeypatch.setattr(detector_mod, "_DEFAULT_METADATA_HOST", f"127.0.0.1:{port}")
        monkeypatch.setattr(
            detector_mod,
            "_K8S_SERVICE_ACCOUNT_PATH",
            str(tmp_path / "missing-sa"),
        )
        monkeypatch.setattr(EnvironmentDetector, "detect", _REAL_ENVIRONMENT_DETECT)
        fresh_env = EnvironmentDetector()
        fresh_env._gcp_bios = True
        monkeypatch.setattr(
            default_usage_tracker,
            "detectors",
            [FrameworkDetector(), fresh_env],
        )

        fs = type(gcs)(**gcs.storage_options, skip_instance_cache=True)
        future = fresh_env._probe_future
        if future is not None:
            future.result(timeout=5)
        assert fresh_env.detect() == "gce"

        wire.clear()
        assert fs.info(path)["name"] == path
        with fs.open(path, "rb", cache_type="none") as f:
            assert f.read() == b"hello world"

        assert wire.user_agents
        assert len(wire.feature_headers) == len(wire.user_agents)
        assert all("env/" not in ua for ua in wire.user_agents)
        assert all(wire.feature_headers)
        decoded_env = [
            GcsfsFeatures.deserialize(base64.urlsafe_b64decode(h))
            for h in wire.feature_headers
        ]
        assert all(
            d.environment == GcsfsEnvironment.GCSFS_ENVIRONMENT_GCE for d in decoded_env
        )
        assert recorded_mds == [
            ("/computeMetadata/v1/instance/attributes/cluster-name", "Google")
        ]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_http_telemetry_opt_out_and_reenable_e2e(
    gcs_factory, wire, monkeypatch, tmp_path
):
    """Verify HTTP telemetry opt-out and re-enablement across request paths on one singleton."""
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    monkeypatch.delenv("GCSFS_NO_TELEMETRY", raising=False)
    fs1 = gcs_factory()
    fs2 = type(fs1)(**fs1.storage_options)
    assert fs1 is fs2
    remote_path = f"{_unique_prefix()}/opt-out.bin"
    payload = b"telemetry-e2e-payload"

    def _run_round_trip(fs_inst, tag: str):
        wire.clear()
        fs_inst.pipe(remote_path, payload)
        assert fs_inst.cat(remote_path) == payload
        split = len(wire.user_agents)
        dest = tmp_path / f"{tag}.bin"
        fs_inst.get_file(remote_path, str(dest))
        assert dest.read_bytes() == payload
        return (
            (wire.user_agents[:split], wire.feature_headers[:split]),
            (wire.user_agents[split:], wire.feature_headers[split:]),
        )

    def _assert_tagged_features(slices, expected_methods_by_slice):
        for (uas, features), expected_methods in zip(slices, expected_methods_by_slice):
            assert uas
            assert all("fw/" not in ua and "env/" not in ua for ua in uas)
            assert len(features) == len(uas)
            decoded = [
                GcsfsFeatures.deserialize(base64.urlsafe_b64decode(h)) for h in features
            ]
            assert all(
                d.framework == GcsfsFramework.GCSFS_FRAMEWORK_PANDAS
                and d.method in expected_methods
                for d in decoded
            )

    expected_slice_methods = (
        {GcsfsMethod.GCSFS_METHOD_PIPE, GcsfsMethod.GCSFS_METHOD_CAT},
        {GcsfsMethod.GCSFS_METHOD_GET_FILE},
    )
    token = set_telemetry_context(Dimension.FRAMEWORK, "pandas")
    try:
        _assert_tagged_features(_run_round_trip(fs1, "enabled"), expected_slice_methods)
        wire.assert_all_tagged("pandas")

        for env_key, env_val, header_key, caller_val in (
            ("GCSFS_NO_TELEMETRY", "true", HTTP_HEADER_KEY.lower(), "caller-val"),
            ("DO_NOT_TRACK", "1", "X-Goog-Storage-Gcsfs-Features", "caller-dnt"),
        ):
            monkeypatch.setenv(env_key, env_val)
            for uas, features in _run_round_trip(fs2, env_key):
                assert uas
                assert all("fw/" not in ua for ua in uas)
                assert features == [""] * len(uas)
            wire.clear()
            fs2.call(
                "GET",
                "b/{}/o",
                TEST_BUCKET,
                headers={header_key: caller_val},
                json_out=True,
                maxResults=1,
            )
            assert wire.user_agents
            assert all("fw/" not in ua for ua in wire.user_agents)
            assert wire.feature_headers == [""] * len(wire.user_agents)
            monkeypatch.delenv(env_key, raising=False)

        _assert_tagged_features(
            _run_round_trip(fs1, "reenabled"), expected_slice_methods
        )
        wire.assert_all_tagged("pandas")
    finally:
        reset_telemetry_context(token)
