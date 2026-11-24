import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import torch

pytest.importorskip("transformers")

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch import (  # noqa: E402
    common,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.layout import (  # noqa: E402
    Layout,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.tests.tiny import (  # noqa: E402
    make_params,
)


def test_stats_roundtrip_in_rank_order(tmp_path):
    for rank in (1, 0):
        common.write_stats(str(tmp_path), rank, {"rank": rank})
    assert common.read_stats(str(tmp_path)) == [{"rank": 0}, {"rank": 1}]


def test_spawn_with_stats_returns_ranks_and_stats(monkeypatch):
    fake_ranks = MagicMock()

    def _fake_spawn(prefix, params, target_fn, world_size_override=None):
        assert world_size_override == 2
        target_fn(stats=7)
        return fake_ranks

    def _target(*, weights, stats_dir, stats):
        common.write_stats(stats_dir, 0, {"weights": sorted(weights), "n": stats})

    monkeypatch.setattr(common, "spawn_split", _fake_spawn)
    weights = {"w": 1}
    ranks, stats = common.spawn_with_stats("p", None, _target, 2, weights)
    assert ranks is fake_ranks
    assert stats == [{"weights": ["w"], "n": 7}]
    assert weights == {"w": 1}
    fake_ranks.join.assert_not_called()


def test_spawn_with_stats_joins_ranks_and_releases_weights_on_error(monkeypatch):
    fake_ranks = MagicMock()
    monkeypatch.setattr(common, "spawn_split", lambda *a, **kw: fake_ranks)
    monkeypatch.setattr(
        common, "read_stats", MagicMock(side_effect=OSError("corrupt stats_dir"))
    )
    weights = {"w": 1}
    with pytest.raises(OSError, match="corrupt stats_dir"):
        common.spawn_with_stats("p", None, lambda **kw: None, 2, weights)
    fake_ranks.join.assert_called_once_with()
    assert weights == {}


def test_timed_synchronizes_cuda_inside_the_window(monkeypatch):
    events = []
    cuda = torch.device("cuda", 0)
    monkeypatch.setattr(common.dist, "barrier", lambda: events.append("barrier"))
    monkeypatch.setattr(
        common.torch.cuda, "synchronize", lambda dev=None: events.append(("sync", dev))
    )

    begin, end = common.timed(cuda, lambda x: events.append(("fn", x)), 1)
    assert begin <= end
    assert events == ["barrier", ("fn", 1), ("sync", cuda), "barrier"]

    events.clear()
    common.timed(torch.device("cpu"), lambda: events.append("fn"))
    assert events == ["barrier", "fn", "barrier"]


def test_warm_up_touches_storage_then_synchronizes(tmp_path, monkeypatch):
    events = []
    monkeypatch.setattr(
        common.torch.cuda, "synchronize", lambda dev=None: events.append(dev)
    )
    common.warm_up(tmp_path.as_uri(), torch.device("cuda", 0))
    assert events == [torch.device("cuda", 0)]


def test_prepare_rejects_bad_layouts_before_staging(tmp_path, tiny_models, monkeypatch):
    staged = []
    monkeypatch.setattr(common, "stage_model", staged.append)
    driver = common.DCPDriver()
    fsdp = Layout("fsdp", world_size=2, data_parallel_size=2)
    tp = Layout("tp", world_size=2, tensor_parallel_size=2)

    tied_dir = tmp_path / "tied"
    tied_dir.mkdir()
    with open(f"{tiny_models['llama']}/config.json") as f:
        cfg = json.load(f)
    cfg["tie_word_embeddings"] = True
    (tied_dir / "config.json").write_text(json.dumps(cfg))
    with pytest.raises(ValueError, match="tie_word_embeddings"):
        driver.prepare(make_params(str(tied_dir)), fsdp)

    olmoe = make_params(tiny_models["olmoe"])
    for layouts in ((tp,), (fsdp, tp)):
        with pytest.raises(ValueError, match="MoE"):
            driver.prepare(olmoe, *layouts)
    assert staged == []

    driver.prepare(olmoe, fsdp, fsdp)
    assert staged == [tiny_models["olmoe"]]


def test_is_checkpoint_file():
    driver = common.DCPDriver()
    assert driver.is_checkpoint_file("ckpt/__0_0.distcp")
    assert driver.is_checkpoint_file("ckpt/.metadata")
    assert not driver.is_checkpoint_file("ckpt/rank0.json")


def test_rank_session_binds_device_and_tears_down(monkeypatch):
    calls = SimpleNamespace(setup=None, destroyed=0)
    monkeypatch.setattr(common, "run_device", lambda: "cpu")
    monkeypatch.setattr(
        common,
        "setup_distributed_env",
        lambda *a, **kw: setattr(calls, "setup", (a, kw)),
    )
    monkeypatch.setattr(
        common.dist,
        "destroy_process_group",
        lambda: setattr(calls, "destroyed", calls.destroyed + 1),
    )
    with pytest.raises(RuntimeError, match="boom"):
        with common.rank_session(1, 2, 1234) as device:
            assert device == torch.device("cpu")
            raise RuntimeError("boom")
    assert calls.setup == ((1, 2, 1234), {"device": "cpu"})
    assert calls.destroyed == 1
