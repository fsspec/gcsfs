import struct

import fsspec
import pytest
import torch
from torch.distributed.checkpoint.fsspec_filesystem import FsspecReader
from torch.distributed.checkpoint.metadata import MetadataIndex, TensorStorageMetadata
from torch.multiprocessing.spawn import ProcessRaisedException

pytest.importorskip("transformers")

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch import (  # noqa: E402
    common,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.reference import (  # noqa: E402
    load_weights,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.tests.tiny import (  # noqa: E402
    make_params,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.write import (  # noqa: E402
    driver as write_driver_mod,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.write.driver import (  # noqa: E402
    PyTorchCheckpointWriteDriver,
)


def _params(model_id: str, **fields):
    return make_params(model_id, scenario="checkpoint_write", **fields)


_SAVE_CASES_4RANK = [
    pytest.param("llama", dict(strategy="fsdp", dp=4), id="fsdp"),
    pytest.param("llama", dict(strategy="hsdp", dpr=2, dp=2), id="hsdp"),
    pytest.param("llama", dict(strategy="tp", tp=4), id="tp"),
    pytest.param("llama", dict(strategy="pp_fsdp", pp=2, dp=2), id="pp_fsdp"),
    pytest.param("olmoe", dict(strategy="ep", dp=4, ep=4), id="ep"),
]


@pytest.mark.parametrize("model_kind, sizes", _SAVE_CASES_4RANK)
def test_save_roundtrip(tmp_path, tiny_models, monkeypatch, model_kind, sizes):
    params = _params(tiny_models[model_kind], world_size=4, rounds=3, **sizes)
    params.target_layout().validate()
    ckpt_dir = tmp_path / "ckpt"
    prefix = ckpt_dir.as_uri()
    driver = PyTorchCheckpointWriteDriver()

    captured_bytes_written = []
    orig_read_stats = common.read_stats

    def _spy_read_stats(stats_dir):
        stats = orig_read_stats(stats_dir)
        captured_bytes_written.extend(s["bytes_written"] for s in stats)
        return stats

    monkeypatch.setattr(common, "read_stats", _spy_read_stats)

    driver.setup(prefix, params)
    result = driver.run(prefix, params)

    # Previous step dirs (step-0/, step-1/) were removed; only step-2/ remains.
    assert [p.name for p in ckpt_dir.iterdir()] == ["step-2"]
    step2_uri = f"{prefix.rstrip('/')}/step-2/"

    fs, root = fsspec.core.url_to_fs(prefix)
    files = fs.find(root)
    ckpt_files = [f for f in files if driver.is_checkpoint_file(f)]
    assert any(f.endswith(".metadata") for f in ckpt_files)
    assert any(f.endswith(".distcp") for f in ckpt_files)
    assert all("/step-2/" in f for f in ckpt_files)

    assert len(result.durations) == 3
    assert all(d > 0 for d in result.durations)
    assert result.bytes_transferred is None
    assert result.extra_columns.keys() == {"dcp_write_bytes_per_rank_max"}
    assert len(captured_bytes_written) == 4
    assert result.extra_columns["dcp_write_bytes_per_rank_max"] == max(
        captured_bytes_written
    )

    # Replica deduplication & exactness: total bytes_written across all ranks
    # equals the tensor bytes of a single full copy of (model, optim).
    weights = load_weights(tiny_models[model_kind])
    one_copy_tensor_bytes = sum((3 * t.numel() + 1) * 4 for t in weights.values())
    md = FsspecReader(step2_uri).read_metadata().state_dict_metadata
    md_tensor_bytes = sum(
        c.sizes.numel() * item_md.properties.dtype.itemsize
        for item_md in md.values()
        if isinstance(item_md, TensorStorageMetadata)
        for c in item_md.chunks
    )
    assert md_tensor_bytes == one_copy_tensor_bytes
    assert sum(captured_bytes_written) == one_copy_tensor_bytes
    if sizes["strategy"] == "hsdp":
        # Without replica dedup each of the 4 ranks would write half the model.
        assert (
            result.extra_columns["dcp_write_bytes_per_rank_max"]
            < one_copy_tensor_bytes // 2
        )

    result.verify()


def test_save_verify_detects_corruption(tmp_path, tiny_models):
    params = _params(
        tiny_models["llama"], strategy="fsdp", dp=2, world_size=2, rounds=2
    )
    ckpt_dir = tmp_path / "ckpt"
    prefix = ckpt_dir.as_uri()
    driver = PyTorchCheckpointWriteDriver()

    driver.setup(prefix, params)
    result = driver.run(prefix, params)

    step_dir = ckpt_dir / "step-1"
    meta = FsspecReader(step_dir.as_uri()).read_metadata()
    idx = MetadataIndex("model.model.embed_tokens.weight", [0, 0])
    info = meta.storage_data[idx]
    distcp_path = step_dir / info.relative_path

    weights = load_weights(tiny_models["llama"])
    embed_f32 = weights["model.embed_tokens.weight"].to(dtype=torch.float32)
    shard0_bytes = embed_f32[: embed_f32.shape[0] // 2].contiguous().numpy().tobytes()
    blob = bytearray(distcp_path.read_bytes())
    pos = blob.find(shard0_bytes, info.offset, info.offset + info.length)
    assert pos != -1
    corrupted_val = float(embed_f32.reshape(-1)[0].item()) + 100.0
    blob[pos : pos + 4] = struct.pack("<f", corrupted_val)
    distcp_path.write_bytes(bytes(blob))

    with pytest.raises(
        (ProcessRaisedException, RuntimeError),
        match=(
            r"RuntimeError: rank 0: value mismatch in 1 entries, "
            r"e\.g\. model\.embed_tokens\.weight"
        ),
    ):
        result.verify()


@pytest.mark.parametrize(
    "prefix, kwargs",
    [
        ("file:///ckpt/step-1/", {}),
        ("gs://b/ckpt/step-1/", {"cache_type": "blockcache"}),
        ("gcs://b/ckpt/step-1/", {"cache_type": "blockcache"}),
    ],
)
def test_verify_reader_uses_blockcache_on_gcs_only(monkeypatch, prefix, kwargs):
    monkeypatch.setattr(write_driver_mod, "FsspecReader", lambda p, **kw: (p, kw))
    assert write_driver_mod._reader_for(prefix) == (prefix, kwargs)


@pytest.mark.parametrize("prefix", ["gcs://b/ckpt", "gcs://b/ckpt/"])
def test_step_prefix_is_normalized(prefix):
    assert write_driver_mod._step_prefix(prefix, 1) == "gcs://b/ckpt/step-1/"


@pytest.mark.parametrize("rounds, expected_barriers", [(1, 2), (3, 8)])
def test_rank_save_skips_post_round_barrier_on_round_zero(
    tmp_path, monkeypatch, rounds, expected_barriers
):
    import contextlib
    import os
    import queue

    barriers = []
    monkeypatch.setattr(
        write_driver_mod,
        "rank_session",
        lambda *a, **kw: contextlib.nullcontext(torch.device("cpu")),
    )
    monkeypatch.setattr(write_driver_mod, "build_state", lambda *a, **kw: {})
    monkeypatch.setattr(write_driver_mod, "warm_up", lambda *a, **kw: None)
    monkeypatch.setattr(write_driver_mod.dist, "barrier", lambda: barriers.append(1))

    def fake_dcp_save(state, *, storage_writer, planner):
        fs, root = fsspec.core.url_to_fs(storage_writer.path)
        os.makedirs(root, exist_ok=True)
        with open(os.path.join(root, ".metadata"), "wb") as f:
            f.write(b"ok")

    monkeypatch.setattr(write_driver_mod.dcp, "save", fake_dcp_save)

    ckpt_dir = tmp_path / f"ckpt-r{rounds}"
    stats_dir = tmp_path / f"stats-r{rounds}"
    stats_dir.mkdir()
    q = queue.Queue()
    params = _params("fake", strategy="fsdp", dp=2, world_size=2, rounds=rounds)

    write_driver_mod._rank_save(
        0,
        2,
        29500,
        ckpt_dir.as_uri(),
        params,
        q,
        weights={},
        stats_dir=str(stats_dir),
    )

    assert len(q.get_nowait()) == rounds
    assert len(barriers) == expected_barriers
    assert [p.name for p in ckpt_dir.iterdir()] == [f"step-{rounds - 1}"]


def test_write_driver_close_clears_weights_without_verify(
    tmp_path, tiny_models, monkeypatch
):
    from unittest.mock import MagicMock

    params = _params(
        tiny_models["llama"], strategy="fsdp", dp=2, world_size=2, rounds=1
    )
    prefix = (tmp_path / "ckpt_close").as_uri()
    driver = PyTorchCheckpointWriteDriver()
    driver.setup(prefix, params)

    captured_weights = []
    orig_load_weights = write_driver_mod.load_weights

    def spy_load_weights(mid):
        w = orig_load_weights(mid)
        captured_weights.append(w)
        return w

    verify_spawns = []
    orig_run_split = write_driver_mod.run_split

    def spy_run_split(*args, **kwargs):
        verify_spawns.append(args)
        return orig_run_split(*args, **kwargs)

    monkeypatch.setattr(write_driver_mod, "load_weights", spy_load_weights)
    monkeypatch.setattr(write_driver_mod, "run_split", spy_run_split)

    result = driver.run(prefix, params)
    assert getattr(result, "close", None) is not None
    assert len(captured_weights) == 1 and len(captured_weights[0]) > 0
    result.close()
    result.close()
    assert captured_weights == [{}]
    assert verify_spawns == []

    # Error after load_weights before CheckpointResult return also clears weights.
    weights_err = {"w": 1}
    fake_ranks = MagicMock()
    monkeypatch.setattr(write_driver_mod, "load_weights", lambda mid: weights_err)
    monkeypatch.setattr(
        write_driver_mod,
        "spawn_with_stats",
        lambda *a, **kw: (fake_ranks, [{"corrupt": 1}]),
    )
    with pytest.raises(KeyError):
        driver.run(prefix, params)
    fake_ranks.join.assert_called_once_with()
    assert weights_err == {}
