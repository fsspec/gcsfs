import json
from types import SimpleNamespace

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


def test_import_fsspec_filesystem_returns_reader_and_writer():
    reader_cls, writer_cls = common.import_fsspec_filesystem()
    assert reader_cls.__name__ == "FsspecReader"
    assert writer_cls.__name__ == "FsspecWriter"


def test_timed_synchronizes_accelerator_inside_the_window(monkeypatch):
    events = []
    cuda = torch.device("cuda", 0)
    monkeypatch.setattr(common.dist, "barrier", lambda: events.append("barrier"))
    monkeypatch.setattr(
        common.accelerator, "synchronize", lambda dev: events.append(("sync", dev))
    )

    begin, end = common.timed(cuda, lambda x: events.append(("fn", x)), 1)
    assert begin <= end
    assert events == ["barrier", ("fn", 1), ("sync", cuda), "barrier"]


def test_warm_up_touches_storage_then_synchronizes(tmp_path, monkeypatch):
    events = []
    monkeypatch.setattr(
        common.accelerator, "synchronize", lambda dev: events.append(dev)
    )
    common.warm_up(tmp_path.as_uri(), torch.device("cuda", 0))
    assert events == [torch.device("cuda", 0)]


def test_prepare_rejects_bad_layouts_before_staging_and_ensures_canonical(
    tmp_path, tiny_models, monkeypatch
):
    staged = []
    canonicalized = []
    monkeypatch.setattr(common, "stage_model", staged.append)
    monkeypatch.setattr(common, "ensure_canonical_weights", canonicalized.append)
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
    assert canonicalized == []

    driver.prepare(olmoe, fsdp, fsdp)
    assert staged == [tiny_models["olmoe"]]
    assert canonicalized == [tiny_models["olmoe"]]


def test_is_checkpoint_file():
    driver = common.DCPDriver()
    assert driver.is_checkpoint_file("ckpt/__0_0.distcp")
    assert driver.is_checkpoint_file("ckpt/.metadata")
    assert not driver.is_checkpoint_file("ckpt/rank0.json")


def test_rank_session_binds_device_and_tears_down(monkeypatch):
    calls = SimpleNamespace(setup=None, destroyed=0, threads=[])
    monkeypatch.setattr(
        common,
        "setup_distributed_env",
        lambda: (setattr(calls, "setup", True), torch.device("cpu"))[1],
    )
    monkeypatch.setattr(
        common.dist,
        "destroy_process_group",
        lambda: setattr(calls, "destroyed", calls.destroyed + 1),
    )
    monkeypatch.setattr(
        common.torch, "set_num_threads", lambda n: calls.threads.append(n)
    )
    with pytest.raises(RuntimeError, match="boom"):
        with common.rank_session() as device:
            assert device == torch.device("cpu")
            raise RuntimeError("boom")
    assert calls.setup is True
    assert calls.destroyed == 1
    assert len(calls.threads) == 1


def test_rank_session_skips_set_num_threads_on_accelerator(monkeypatch):
    calls = SimpleNamespace(destroyed=0, threads=[])
    monkeypatch.setattr(
        common,
        "setup_distributed_env",
        lambda: torch.device("cuda", 0),
    )
    monkeypatch.setattr(
        common.dist,
        "destroy_process_group",
        lambda: setattr(calls, "destroyed", calls.destroyed + 1),
    )
    monkeypatch.setattr(
        common.torch, "set_num_threads", lambda n: calls.threads.append(n)
    )
    with common.rank_session() as device:
        assert device == torch.device("cuda", 0)
    assert calls.destroyed == 1
    assert calls.threads == []
