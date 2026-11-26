"""Real integration tests verifying framework detection when upstream packages use gcsfs."""

from __future__ import annotations

import base64

import pytest

from gcsfs.telemetry.context import HTTP_HEADER_KEY
from gcsfs.telemetry.features_proto import GcsfsFeatures, GcsfsFramework

# Run every test against both gcsfs.core.GCSFileSystem and ExtendedGcsFileSystem
pytestmark = pytest.mark.parametrize(
    "mock_gcs_harness", ["core", "extended"], indirect=True, ids=["core", "extended"]
)


def _assert_all_feature_headers(harness, expected_fw: GcsfsFramework) -> None:
    assert len(harness.user_agents) > 0
    assert len(harness.feature_headers) == len(harness.user_agents)
    for ua in harness.user_agents:
        assert (
            "fw/" not in ua and "env/" not in ua
        ), f"Unexpected telemetry token in User-Agent: '{ua}'"
    for h in harness.feature_headers:
        assert h, f"Missing {HTTP_HEADER_KEY} header"
        decoded = GcsfsFeatures.deserialize(base64.urlsafe_b64decode(h))
        assert (
            decoded.framework == expected_fw
        ), f"Expected {expected_fw.name} in {HTTP_HEADER_KEY}, got: {decoded.framework.name}"


# ============================================================================
# 1. PyTorch Lightning Integration Test
# ============================================================================


def test_pytorch_lightning_real_invocation(mock_gcs_harness, tmp_path):
    """
    Verify that when PyTorch Lightning triggers dataset/file loading,
    'lightning' is accurately reported in X-Goog-Storage-Gcsfs-Features.
    """
    lightning = pytest.importorskip("lightning")
    import torch
    from torch.utils.data import DataLoader, Dataset

    # Create a custom PyTorch dataset that reads from GCS using fsspec/gcsfs
    class GCSStreamDataset(Dataset):
        def __init__(self, fs):
            self.fs = fs

        def __len__(self):
            return 2

        def __getitem__(self, idx):
            # Lightning initiates the data read through DataLoader
            with self.fs.open(f"gcs://test-bucket/data_{idx}.pt", "rb") as f:
                data = f.read(32)
            return torch.tensor([idx, len(data)], dtype=torch.float32)

    # Wrap inside a PyTorch Lightning DataModule
    class SampleLightningDataModule(lightning.LightningDataModule):
        def __init__(self, fs):
            super().__init__()
            self.fs = fs

        def train_dataloader(self):
            return DataLoader(GCSStreamDataset(self.fs), batch_size=1)

    # Minimal LightningModule
    class DummyModule(lightning.LightningModule):
        def __init__(self):
            super().__init__()
            self.layer = torch.nn.Linear(2, 1)

        def training_step(self, batch, batch_idx):
            loss = self.layer(batch).sum()
            return loss

        def configure_optimizers(self):
            return torch.optim.SGD(self.parameters(), lr=0.01)

    dm = SampleLightningDataModule(mock_gcs_harness.fs)
    model = DummyModule()

    # Run Lightning Trainer in fast_dev_run mode (1 batch)
    trainer = lightning.Trainer(
        max_epochs=1,
        fast_dev_run=True,
        enable_checkpointing=False,
        logger=False,
        enable_progress_bar=False,
        accelerator="cpu",
        default_root_dir=tmp_path,
    )

    mock_gcs_harness.clear()
    trainer.fit(model, datamodule=dm)

    _assert_all_feature_headers(
        mock_gcs_harness, GcsfsFramework.GCSFS_FRAMEWORK_LIGHTNING
    )


def test_pytorch_lightning_native_checkpoint_save(mock_gcs_harness, tmp_path):
    """
    Verify 100% framework-internal I/O where PyTorch Lightning itself
    invokes fsspec and gcsfs to save checkpoints via trainer.save_checkpoint().
    """
    lightning = pytest.importorskip("lightning")
    import torch

    class SimpleModule(lightning.LightningModule):
        def __init__(self):
            super().__init__()
            self.layer = torch.nn.Linear(2, 1)

    model = SimpleModule()
    # Keep Lightning from writing logs/checkpoints into the current working directory
    trainer = lightning.Trainer(
        accelerator="cpu",
        logger=False,
        enable_checkpointing=False,
        enable_progress_bar=False,
        default_root_dir=tmp_path,
    )
    trainer.strategy.connect(model)

    mock_gcs_harness.clear()
    # Lightning's internal TorchCheckpointIO executes fs.makedirs, fs.pipe, fs.open
    trainer.save_checkpoint("gcs://test-bucket/model.ckpt")

    _assert_all_feature_headers(
        mock_gcs_harness, GcsfsFramework.GCSFS_FRAMEWORK_LIGHTNING
    )


# ============================================================================
# 2. PyTorch Native DataLoader Integration Test
# ============================================================================


def test_pytorch_native_dataloader_real_invocation(mock_gcs_harness):
    """
    Verify that native PyTorch DataLoader reads report 'torch' in X-Goog-Storage-Gcsfs-Features.
    """
    torch = pytest.importorskip("torch")
    from torch.utils.data import DataLoader, Dataset

    class TorchGCSDataset(Dataset):
        def __init__(self, fs):
            self.fs = fs

        def __len__(self):
            return 3

        def __getitem__(self, idx):
            with self.fs.open(f"gcs://test-bucket/data_{idx}.pt", "rb") as f:
                content = f.read(32)
            return torch.tensor([idx, len(content)], dtype=torch.int64)

    loader = DataLoader(TorchGCSDataset(mock_gcs_harness.fs), batch_size=1)

    mock_gcs_harness.clear()
    for batch in loader:
        pass

    _assert_all_feature_headers(mock_gcs_harness, GcsfsFramework.GCSFS_FRAMEWORK_TORCH)


# ============================================================================
# 3. Pandas Native Integration Tests
# ============================================================================


def test_pandas_read_csv_native(mock_gcs_harness):
    """
    Verify that pd.read_csv("gcs://...") automatically reports 'pandas' in X-Goog-Storage-Gcsfs-Features.
    """
    pandas = pytest.importorskip("pandas")

    mock_gcs_harness.clear()
    df = pandas.read_csv(
        "gcs://test-bucket/data.csv", storage_options={"token": "anon"}
    )

    assert not df.empty
    _assert_all_feature_headers(mock_gcs_harness, GcsfsFramework.GCSFS_FRAMEWORK_PANDAS)


def test_pandas_to_csv_native(mock_gcs_harness):
    """
    Verify that df.to_csv("gcs://...") automatically reports 'pandas' in X-Goog-Storage-Gcsfs-Features.
    """
    pandas = pytest.importorskip("pandas")

    df = pandas.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    mock_gcs_harness.clear()
    df.to_csv("gcs://test-bucket/out.csv", storage_options={"token": "anon"})

    _assert_all_feature_headers(mock_gcs_harness, GcsfsFramework.GCSFS_FRAMEWORK_PANDAS)
