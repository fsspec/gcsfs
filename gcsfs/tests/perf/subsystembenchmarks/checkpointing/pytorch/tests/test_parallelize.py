import json
import os

import torch

pytest.importorskip("transformers")

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (
    build_model,
    build_optimizer,
    materialize,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parameters import (
    PyTorchCheckpointParameters,
)


def _create_tiny_llama_config_dir(tmp_path):
    config = {
        "model_type": "llama",
        "hidden_size": 64,
        "num_hidden_layers": 2,
        "num_attention_heads": 4,
        "num_key_value_heads": 2,
        "intermediate_size": 128,
        "vocab_size": 256,
        "max_position_embeddings": 512,
        "rope_scaling": None,
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config))
    return str(tmp_path)


def test_build_materialize_optimizer(tmp_path):
    config_dir = _create_tiny_llama_config_dir(tmp_path)
    params = PyTorchCheckpointParameters(
        name="test-case",
        bucket_name="test-bucket",
        rounds=1,
        framework="pytorch",
        scenario="checkpoint_read",
        strategy="fsdp",
        bucket_type="regional",
        model_id=config_dir,
        world_size=8,
    )

    model = build_model(params)

    # 1. Before materialize: all parameters must be on meta device
    model_params = list(model.parameters())
    assert len(model_params) > 0
    assert all(p.is_meta for p in model_params)

    # 2. Materialize
    materialize(model, seed=42)

    # 3. After materialize: all parameters on CPU, all require grad
    assert all(not p.is_meta and p.device.type == "cpu" for p in model.parameters())
    assert all(p.requires_grad for p in model.parameters())

    # 4. Optimizer: AdamW with exp_avg/exp_avg_sq populated for every parameter
    optimizer = build_optimizer(model)
    assert len(optimizer.state) == len(list(model.parameters()))
    for p in model.parameters():
        assert p in optimizer.state
        state = optimizer.state[p]
        assert "exp_avg" in state
        assert "exp_avg_sq" in state
        assert "step" in state
        assert state["exp_avg"].shape == p.shape
        assert state["exp_avg_sq"].shape == p.shape
        assert state["exp_avg"].device == p.device
        assert state["exp_avg_sq"].device == p.device
        assert torch.all(state["exp_avg"] == 0)
        assert torch.all(state["exp_avg_sq"] == 0)


def test_materialize_seed_determinism(tmp_path):
    config_dir = _create_tiny_llama_config_dir(tmp_path)
    params = PyTorchCheckpointParameters(
        name="test-case",
        bucket_name="test-bucket",
        rounds=1,
        framework="pytorch",
        scenario="checkpoint_read",
        strategy="fsdp",
        bucket_type="regional",
        model_id=config_dir,
        world_size=8,
    )

    model1 = build_model(params)
    materialize(model1, seed=123)

    model2 = build_model(params)
    materialize(model2, seed=123)

    model3 = build_model(params)
    materialize(model3, seed=456)

    # Check identical seeds produce identical values
    for p1, p2 in zip(model1.parameters(), model2.parameters()):
        assert torch.equal(p1, p2)

    # Check different seeds produce different values
    has_diff = any(
        not torch.equal(p1, p3)
        for p1, p3 in zip(model1.parameters(), model3.parameters())
    )
    assert has_diff


def test_resolve_model_config_staged(tmp_path, monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (
        _resolve_model_config,
    )

    staged_model_dir = tmp_path / "my_model"
    staged_model_dir.mkdir()
    config_data = {
        "model_type": "llama",
        "hidden_size": 32,
        "num_hidden_layers": 1,
        "num_attention_heads": 2,
        "num_key_value_heads": 1,
        "intermediate_size": 64,
        "vocab_size": 128,
        "max_position_embeddings": 256,
    }
    (staged_model_dir / "config.json").write_text(json.dumps(config_data))

    orig_join = os.path.join
    monkeypatch.setattr(
        "os.path.join",
        lambda *args: (
            str(staged_model_dir) if args == ("/tmp", "my_model") else orig_join(*args)
        ),
    )
    cfg = _resolve_model_config("gs://my-bucket/models/my_model")
    assert cfg.hidden_size == 32


def test_resolve_model_config_gcs_fallback(tmp_path, monkeypatch):
    from unittest.mock import MagicMock

    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (
        _resolve_model_config,
    )

    config_data = {
        "model_type": "llama",
        "hidden_size": 48,
        "num_hidden_layers": 1,
        "num_attention_heads": 2,
        "num_key_value_heads": 1,
        "intermediate_size": 96,
        "vocab_size": 128,
        "max_position_embeddings": 256,
    }

    mock_fs = MagicMock()

    def fake_get_file(rpath, lpath):
        with open(lpath, "w") as f:
            json.dump(config_data, f)

    mock_fs.get_file.side_effect = fake_get_file
    monkeypatch.setattr("gcsfs.GCSFileSystem", lambda: mock_fs)

    cfg = _resolve_model_config("gs://my-bucket/models/unstaged_model")
    assert cfg.hidden_size == 48
    assert mock_fs.get_file.call_count == 1
    assert mock_fs.get_file.call_args[0][0] == (
        "gs://my-bucket/models/unstaged_model/config.json"
    )
