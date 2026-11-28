"""Tests for PyTorch Lightning GPU support, torchrun launch, and TPU skip."""

from __future__ import annotations

import json
import os
from types import SimpleNamespace

import pytest

L = pytest.importorskip("lightning.pytorch")
torch = pytest.importorskip("torch")

from gcsfs.tests.perf.subsystembenchmarks._common import (  # noqa: E402
    accelerator,
    launcher,
    placement,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning import (  # noqa: E402
    common,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.parameters import (  # noqa: E402
    PLCheckpointParameters,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.read import (  # noqa: E402
    driver as read_driver,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.write import (  # noqa: E402
    driver as write_driver,
)


def _make_params(
    strategy="ddp",
    world_size=4,
    tp=2,
    dp=2,
    rounds=1,
    model_id="gs://bucket/Llama-3.1-8B",
    setup_world_size=None,
):
    return PLCheckpointParameters(
        name="pl-test",
        bucket_name="",
        bucket_type="regional",
        rounds=rounds,
        scenario="checkpoint_write",
        framework="pytorch_lightning",
        model_id=model_id,
        strategy=strategy,
        world_size=world_size,
        tensor_parallel_size=tp,
        data_parallel_size=dp,
        setup_world_size=setup_world_size,
    )


def test_trainer_device_args(monkeypatch):
    monkeypatch.setattr(accelerator, "device_kind", lambda: "cpu")
    assert common.trainer_device_args(4) == {"accelerator": "cpu", "devices": 4}

    monkeypatch.setattr(accelerator, "device_kind", lambda: "cuda")
    assert common.trainer_device_args(8) == {"accelerator": "gpu", "devices": 8}

    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    with pytest.raises(ValueError, match="tpu"):
        common.trainer_device_args(8)


@pytest.mark.parametrize(
    "strategy",
    [
        "ddp",
        "fsdp_sharded",
        "fsdp_full",
        "model_parallel_sharded",
        "model_parallel_full",
    ],
)
@pytest.mark.parametrize(
    "kind,expected_backend",
    [("cpu", "gloo"), ("cuda", "nccl")],
)
def test_strategy_backend_matches_accelerator(
    monkeypatch, strategy, kind, expected_backend
):
    monkeypatch.setattr(accelerator, "device_kind", lambda: kind)
    strat = common.get_strategy(_make_params(strategy=strategy))
    assert strat._process_group_backend == expected_backend


def test_root_device_override_cpu_only(monkeypatch):
    for cls, kwargs in (
        (common.HostAwareFSDPStrategy, {}),
        (
            common.HostAwareModelParallelStrategy,
            {"tensor_parallel_size": 2, "data_parallel_size": 2},
        ),
    ):
        strat = cls(**kwargs)
        monkeypatch.setattr(accelerator, "device_kind", lambda: "cpu")
        assert strat.root_device == torch.device("cpu")

        monkeypatch.setattr(accelerator, "device_kind", lambda: "cuda")
        fake_gpu = torch.device("cuda", 3)
        strat.parallel_devices = [fake_gpu]
        strat.cluster_environment = SimpleNamespace(local_rank=lambda: 0)
        assert strat.root_device == fake_gpu


def test_lightning_skipped_on_tpu(monkeypatch):
    monkeypatch.delenv(placement.NODES_ENV, raising=False)
    monkeypatch.delenv(placement.DEVICES_PER_NODE_ENV, raising=False)
    monkeypatch.delenv(placement.COLLECTED_CASES_FILE_ENV, raising=False)
    params = _make_params(strategy="ddp", world_size=8)

    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    monkeypatch.setattr(accelerator, "local_device_count", lambda: 8)
    reason = placement.skip_reason(params, "checkpointing/pytorch_lightning")
    assert reason is not None
    assert "limitations.md#lightning-tpu" in reason

    monkeypatch.setattr(accelerator, "device_kind", lambda: "cuda")
    monkeypatch.setattr(accelerator, "local_device_count", lambda: 8)
    assert placement.skip_reason(params, "checkpointing/pytorch_lightning") is None


def test_setup_read_trainer_uses_trainer_device_args_and_disables_logger(monkeypatch):
    events = []

    class _FakeStrategy:
        restore_checkpoint_after_setup = False

        def connect(self, model):
            events.append("connect")

        def setup_environment(self):
            events.append("setup_environment")

        def setup(self, trainer):
            events.append("setup")

    recorded_trainer_args = {}

    class _FakeTrainer:
        def __init__(self, **kwargs):
            recorded_trainer_args.update(kwargs)
            self.strategy = _FakeStrategy()
            self.state = SimpleNamespace(fn=None)

    fake_model = SimpleNamespace(to=lambda dtype: fake_model, trainer=None)
    monkeypatch.setattr(accelerator, "device_kind", lambda: "cuda")
    monkeypatch.setattr(read_driver, "DummyModel", lambda params: fake_model)
    monkeypatch.setattr(read_driver.L, "Trainer", _FakeTrainer)
    monkeypatch.setattr(
        read_driver, "_call_configure_model", lambda t: events.append("configure_model")
    )

    read_driver._setup_read_trainer("gs://b/p", _make_params(), world_size=4)
    assert recorded_trainer_args["accelerator"] == "gpu"
    assert recorded_trainer_args["devices"] == 4
    assert recorded_trainer_args["logger"] is False
    assert events == ["connect", "setup_environment", "configure_model"]


def test_write_driver_single_times_in_on_train_end_and_syncs(monkeypatch):
    events = []
    fake_dev = torch.device("cuda", 0)
    monkeypatch.setattr(accelerator, "device_kind", lambda: "cuda")
    monkeypatch.setattr(
        accelerator, "bind_rank", lambda r: events.append(("bind", r)) or fake_dev
    )
    monkeypatch.setattr(
        accelerator, "synchronize", lambda d: events.append(("sync", d))
    )

    fake_model = SimpleNamespace(to=lambda dtype: fake_model)
    recorded_args = {}

    class _FakeTrainer:
        def __init__(self, **kwargs):
            recorded_args.update(kwargs)

        def fit(self, model, train_dataloaders=None):
            events.append("fit")
            for cb in recorded_args["callbacks"]:
                cb.on_train_end(self, model)

        def save_checkpoint(self, path):
            events.append(("save", path))

    monkeypatch.setattr(write_driver, "DummyModel", lambda params: fake_model)
    monkeypatch.setattr(write_driver.L, "Trainer", _FakeTrainer)
    res = write_driver.PLCheckpointWriteDriver().run(
        "gs://b/p", _make_params(strategy="single", world_size=1, rounds=1)
    )
    assert len(res.durations) == 1
    assert res.window is None
    assert res.resource_metrics is None
    assert recorded_args["accelerator"] == "gpu"
    assert recorded_args["devices"] == 1
    assert recorded_args["logger"] is False
    assert events == [
        ("bind", 0),
        "fit",
        ("save", "gs://b/p/model.ckpt"),
        ("sync", fake_dev),
    ]


def test_distributed_save_and_load_invoke_run_torchrun_and_stage_model(monkeypatch):
    staged = []
    launches = []

    monkeypatch.setattr(write_driver, "stage_gcs_model", lambda m: staged.append(m))
    monkeypatch.setattr(read_driver, "stage_gcs_model", lambda m: staged.append(m))

    def fake_run_torchrun(module, args, *, world_size, rdzv_id, **kwargs):
        launches.append((module, list(args), world_size, rdzv_id))
        if "--result-file" in args:
            rf = args[args.index("--result-file") + 1]
            with open(rf, "w", encoding="utf-8") as f:
                json.dump({"durations": [1.5, 2.5]}, f)
        return True

    monkeypatch.setattr(launcher, "run_torchrun", fake_run_torchrun)

    params = _make_params(strategy="ddp", world_size=4, rounds=2, setup_world_size=2)
    write_driver.PLCheckpointWriteDriver().setup("gs://b/p", params)
    write_res = write_driver.PLCheckpointWriteDriver().run("gs://b/p", params)
    assert write_res.durations == [1.5, 2.5]
    assert write_res.window is None
    assert write_res.resource_metrics is None

    read_driver.PLCheckpointReadDriver().setup("gs://b/p", params)
    read_res = read_driver.PLCheckpointReadDriver().run("gs://b/p", params)
    assert read_res.durations == [1.5, 2.5]
    assert read_res.window is None
    assert read_res.resource_metrics is None

    # Staging happens in setup(), outside the monitored run().
    assert staged == ["gs://bucket/Llama-3.1-8B"] * 2
    expected_mod = (
        "gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning._worker"
    )
    assert [m for m, _, _, _ in launches] == [expected_mod] * 3
    ops = [args[args.index("--op") + 1] for _, args, _, _ in launches]
    assert ops == ["save", "setup_load", "load"]
    world_sizes = [ws for _, _, ws, _ in launches]
    assert world_sizes == [4, 2, 4]


@pytest.fixture
def tiny_llama_dir(tmp_path):
    from transformers import LlamaConfig, LlamaForCausalLM

    path = tmp_path / "tiny-llama"
    cfg = LlamaConfig(
        vocab_size=64,
        hidden_size=32,
        intermediate_size=64,
        num_hidden_layers=2,
        num_attention_heads=4,
        num_key_value_heads=2,
    )
    LlamaForCausalLM(cfg).to(torch.bfloat16).save_pretrained(path)
    return str(path)


def test_timed_saves_run_before_strategy_teardown(
    monkeypatch, tmp_path, tiny_llama_dir
):
    from lightning.pytorch.strategies.strategy import Strategy

    order = []
    orig_teardown, orig_save = Strategy.teardown, L.Trainer.save_checkpoint
    monkeypatch.setattr(
        Strategy,
        "teardown",
        lambda self: (order.append("teardown"), orig_teardown(self)),
    )
    monkeypatch.setattr(
        L.Trainer,
        "save_checkpoint",
        lambda self, *a, **k: (
            order.append("save_checkpoint"),
            orig_save(self, *a, **k),
        ),
    )
    monkeypatch.setattr(accelerator, "device_kind", lambda: "cpu")
    params = _make_params(
        strategy="single", world_size=1, rounds=2, model_id=tiny_llama_dir
    )

    res = write_driver.PLCheckpointWriteDriver().run(str(tmp_path / "ckpt"), params)

    assert order == ["save_checkpoint", "save_checkpoint", "teardown"]
    assert len(res.durations) == 2
    assert os.path.isfile(tmp_path / "ckpt" / "model.ckpt")


def test_ddp_timed_saves_via_torchrun_complete_two_rounds(
    monkeypatch, tmp_path, tiny_llama_dir
):
    for key in ("RANK", "LOCAL_RANK", "WORLD_SIZE", "MASTER_ADDR", "MASTER_PORT"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv(accelerator.DEVICE_ENV, "cpu")
    accelerator.reset_cache()
    params = _make_params(
        strategy="ddp", world_size=2, rounds=2, model_id=tiny_llama_dir
    )

    res = write_driver.PLCheckpointWriteDriver().run(str(tmp_path / "ddp"), params)

    assert len(res.durations) == 2
    assert all(d > 0 for d in res.durations)
    assert os.path.isfile(tmp_path / "ddp" / "model.ckpt")
