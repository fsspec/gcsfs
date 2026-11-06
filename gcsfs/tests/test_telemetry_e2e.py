"""
End-to-end telemetry tests: real upstream frameworks driving gcsfs against a GCS
endpoint over the JSON API (HTTP) path.

Where these run:
- Cloud Build e2e `standard` suite: real GCS, regional bucket (GCSFileSystem /
  ExtendedGcsFileSystem on a flat bucket).
- Cloud Build e2e `hns` suite: real GCS, HNS bucket.
- GitHub CI: the fake GCS emulator.

Rapid (zonal) and Rapid Cache Ultra buckets are intentionally not covered: their
data path uses gRPC bidi streaming APIs, which don't go through the HTTP User-Agent
path at all.

Unlike `test_upstream_frameworks_real.py`, nothing in gcsfs is mocked here. Requests
go through the real `_call` / `_request` / aiohttp path, and the `User-Agent` header is
recorded at the aiohttp layer, i.e. exactly what is sent over the wire.
"""

from __future__ import annotations

import uuid
from urllib.parse import urlparse

import aiohttp
import fsspec
import pytest

from gcsfs.tests.settings import TEST_BUCKET


class _WireRecorder:
    """Records the User-Agent of every HTTP request sent to the GCS endpoint."""

    def __init__(self, gcs_netloc: str):
        self.gcs_netloc = gcs_netloc
        self.user_agents: list[str] = []

    def clear(self):
        self.user_agents.clear()

    def assert_all_tagged(self, fw: str):
        assert self.user_agents, "no HTTP requests reached the GCS endpoint"
        missing = [ua for ua in self.user_agents if f"fw/{fw}" not in ua]
        assert not missing, (
            f"{len(missing)}/{len(self.user_agents)} GCS requests missing fw/{fw}: "
            f"{missing[:5]}"
        )


@pytest.fixture
def wire(gcs, monkeypatch):
    """
    Point frameworks at the same GCS endpoint/credentials as the `gcs` fixture and
    record outgoing User-Agent headers at the aiohttp layer.
    """
    # Filesystems created internally by frameworks (pandas, Lightning, ...) pick up
    # the test endpoint/credentials through fsspec's per-protocol config defaults.
    storage_options = dict(gcs.storage_options)
    for proto in ("gs", "gcs"):
        monkeypatch.setitem(fsspec.config.conf, proto, storage_options)

    recorder = _WireRecorder(urlparse(gcs._location).netloc)
    # ClientSession.request()/get()/... all funnel through the private `_request` (aiohttp 3.x).
    assert hasattr(
        aiohttp.ClientSession, "_request"
    ), "aiohttp.ClientSession._request not found; update the e2e request recorder"
    original_request = aiohttp.ClientSession._request

    async def recording_request(self, method, str_or_url, *args, **kwargs):
        if urlparse(str(str_or_url)).netloc == recorder.gcs_netloc:
            headers = kwargs.get("headers") or {}
            recorder.user_agents.append(headers.get("User-Agent", ""))
        return await original_request(self, method, str_or_url, *args, **kwargs)

    monkeypatch.setattr(aiohttp.ClientSession, "_request", recording_request)
    # Fresh instances so frameworks don't reuse a cached filesystem from another test
    gcs.clear_instance_cache()
    yield recorder
    gcs.clear_instance_cache()


def _unique_prefix() -> str:
    return f"{TEST_BUCKET}/telemetry-e2e/{uuid.uuid4().hex}"


def test_pandas_csv_round_trip_e2e(gcs, wire):
    """`df.to_csv` / `pd.read_csv` on gs:// URLs send `fw/pandas` on every request."""
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
    """Reads issued from a torch `Dataset` inside a `DataLoader` send `fw/torch`."""
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
    """Lightning's own checkpoint I/O (`trainer.save_checkpoint`) sends `fw/lightning`."""
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
