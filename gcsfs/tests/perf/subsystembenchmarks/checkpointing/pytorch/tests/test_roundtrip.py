import dataclasses
import functools

import fsspec
import pytest
from torch.distributed.checkpoint.fsspec_filesystem import FsspecReader
from torch.distributed.checkpoint.metadata import TensorStorageMetadata
from torch.distributed.checkpoint.planner import LoadItemType
from torch.multiprocessing.spawn import ProcessRaisedException

pytest.importorskip("transformers")

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch import (  # noqa: E402
    common,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.planners import (  # noqa: E402
    RecordingLoadPlanner,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.read import (  # noqa: E402
    driver as read_driver_mod,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.read.driver import (  # noqa: E402
    PyTorchCheckpointReadDriver,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.tests.tiny import (  # noqa: E402
    make_params,
)

_LOAD_CASES_4RANK = [
    pytest.param("llama", dict(strategy="fsdp", dp=4), id="fsdp"),
    pytest.param("llama", dict(strategy="hsdp", dpr=2, dp=2), id="hsdp"),
    pytest.param("llama", dict(strategy="tp", tp=4), id="tp"),
    pytest.param("llama", dict(strategy="fsdp_tp", dp=2, tp=2), id="fsdp_tp"),
    pytest.param("llama", dict(strategy="pp_fsdp", pp=2, dp=2), id="pp_fsdp"),
    pytest.param("olmoe", dict(strategy="ep", dp=4, ep=4), id="ep"),
]


@pytest.mark.parametrize("model_kind, sizes", _LOAD_CASES_4RANK)
def test_load_roundtrip(tmp_path, tiny_models, model_kind, sizes):
    params = make_params(tiny_models[model_kind], world_size=4, rounds=2, **sizes)
    params.target_layout().validate()
    prefix = (tmp_path / "ckpt").as_uri()
    driver = PyTorchCheckpointReadDriver()

    driver.setup(prefix, params)
    result = driver.run(prefix, params)
    result.verify()

    cols = result.extra_columns
    assert len(result.durations) == 2
    assert all(d > 0 for d in result.durations)
    assert cols.keys() == {
        "dcp_read_items_per_rank_max",
        "dcp_read_bytes_fetched_total",
        "dcp_read_bytes_fetched_per_rank_max",
    }
    assert cols["dcp_read_items_per_rank_max"] > 0
    assert result.bytes_transferred == cols["dcp_read_bytes_fetched_total"]
    assert (
        0
        < cols["dcp_read_bytes_fetched_per_rank_max"]
        <= cols["dcp_read_bytes_fetched_total"]
    )

    fs, root = fsspec.core.url_to_fs(prefix)
    files = fs.find(root)
    ckpt_files = [f for f in files if driver.is_checkpoint_file(f)]
    assert any(f.endswith(".metadata") for f in ckpt_files)
    assert sum(f.endswith(".distcp") for f in ckpt_files) == 4
    assert not any("checksum" in f for f in files)


_RESHARD_PAIRS = [
    pytest.param(
        "llama",
        dict(strategy="fsdp", data_parallel_size=4, world_size=4),
        dict(strategy="fsdp", dp=2, world_size=2),
        id="fsdp-dp4-to-dp2",
    ),
    pytest.param(
        "llama",
        dict(strategy="fsdp", data_parallel_size=2, world_size=2),
        dict(strategy="fsdp", dp=4, world_size=4),
        id="fsdp-dp2-to-dp4",
    ),
    pytest.param(
        "llama",
        dict(strategy="fsdp", data_parallel_size=4, world_size=4),
        dict(strategy="hsdp", dpr=2, dp=2, world_size=4),
        id="fsdp-dp4-to-hsdp-dpr2dp2",
    ),
    pytest.param(
        "llama",
        dict(strategy="tp", tensor_parallel_size=4, world_size=4),
        dict(strategy="tp", tp=2, world_size=2),
        id="tp4-to-tp2",
    ),
    pytest.param(
        "llama",
        dict(strategy="tp", tensor_parallel_size=4, world_size=4),
        dict(strategy="fsdp", dp=4, world_size=4),
        id="tp4-to-fsdp-dp4",
    ),
    pytest.param(
        "llama",
        dict(
            strategy="fsdp_tp",
            data_parallel_size=2,
            tensor_parallel_size=2,
            world_size=4,
        ),
        dict(strategy="fsdp", dp=4, world_size=4),
        id="fsdp_tp-dp2tp2-to-fsdp-dp4",
    ),
    pytest.param(
        "llama",
        dict(
            strategy="pp_fsdp_tp",
            pipeline_parallel_size=2,
            data_parallel_size=2,
            tensor_parallel_size=2,
            world_size=8,
        ),
        dict(strategy="fsdp_tp", dp=4, tp=2, world_size=8),
        id="pp_fsdp_tp-pp2dp2tp2-to-fsdp_tp-dp4tp2",
    ),
    pytest.param(
        "olmoe",
        dict(
            strategy="ep",
            data_parallel_size=4,
            expert_parallel_size=4,
            world_size=4,
        ),
        dict(strategy="ep", dp=2, ep=2, world_size=2),
        id="ep4-to-ep2",
    ),
    pytest.param(
        "olmoe",
        dict(
            strategy="ep",
            data_parallel_size=4,
            expert_parallel_size=4,
            world_size=4,
        ),
        dict(strategy="fsdp", dp=4, world_size=4),
        id="ep4-to-fsdp-dp4",
    ),
]


@pytest.mark.parametrize("model_kind, source, target", _RESHARD_PAIRS)
def test_reshard_roundtrip(tmp_path, tiny_models, model_kind, source, target):
    params = make_params(tiny_models[model_kind], source=dict(source), **target)
    params.source_layout().validate()
    params.target_layout().validate()
    assert params.source_layout() != params.target_layout()

    prefix = (tmp_path / "ckpt").as_uri()
    driver = PyTorchCheckpointReadDriver()

    driver.setup(prefix, params)
    fs, root = fsspec.core.url_to_fs(prefix)
    distcp_files = [f for f in fs.find(root) if f.endswith(".distcp")]
    assert len(distcp_files) == params.source_layout().world_size

    result = driver.run(prefix, params)
    result.verify()
    assert len(result.durations) == params.rounds

    storage = FsspecReader(prefix).read_metadata().storage_data
    stored = sum(info.length for info in storage.values())
    total = result.extra_columns["dcp_read_bytes_fetched_total"]
    assert result.bytes_transferred == total
    assert stored <= total <= params.world_size * stored
    assert 0 < result.extra_columns["dcp_read_bytes_fetched_per_rank_max"] <= total


def _drop_first_tensor_on_rank0_worker(
    rank, world_size, port, prefix, params, q, *, inner_target
):
    orig_create_local_plan = RecordingLoadPlanner.create_local_plan

    def _patched_create_local_plan(self):
        plan = orig_create_local_plan(self)
        if rank != 0:
            return plan
        items = list(plan.items)
        for i, item in enumerate(items):
            if item.type == LoadItemType.TENSOR:
                items.pop(i)
                break
        return self._record(dataclasses.replace(plan, items=items))

    RecordingLoadPlanner.create_local_plan = _patched_create_local_plan
    try:
        inner_target(rank, world_size, port, prefix, params, q)
    finally:
        RecordingLoadPlanner.create_local_plan = orig_create_local_plan


def test_reshard_detects_skipped_item(tmp_path, tiny_models, monkeypatch):
    params = make_params(
        tiny_models["llama"],
        strategy="fsdp",
        dp=2,
        world_size=2,
        source={"strategy": "tp", "tensor_parallel_size": 2, "world_size": 2},
    )
    prefix = (tmp_path / "ckpt").as_uri()
    driver = PyTorchCheckpointReadDriver()
    driver.setup(prefix, params)

    orig_spawn_split = common.spawn_split

    def _wrapped_spawn_split(prefix_, params_, target_fn, world_size_override=None):
        return orig_spawn_split(
            prefix_,
            params_,
            functools.partial(
                _drop_first_tensor_on_rank0_worker, inner_target=target_fn
            ),
            world_size_override=world_size_override,
        )

    monkeypatch.setattr(common, "spawn_split", _wrapped_spawn_split)

    result = driver.run(prefix, params)
    with pytest.raises(
        (ProcessRaisedException, RuntimeError),
        match=(
            r"RuntimeError: rank 0: ("
            r"DCP requested \d+ tensor bytes, expected \d+|"
            r"value mismatch in \d+ entries, e\.g\. \S+)"
        ),
    ):
        result.verify()


def test_bytes_fetched_tp_to_fsdp(tmp_path, tiny_models):
    world_size = 2
    driver = PyTorchCheckpointReadDriver()

    # 1. Tiny tp2 -> fsdp dp2 reshard
    reshard_dir = tmp_path / "tp_to_fsdp"
    reshard_prefix = reshard_dir.as_uri()
    reshard_params = make_params(
        tiny_models["llama"],
        strategy="fsdp",
        dp=world_size,
        world_size=world_size,
        source={
            "strategy": "tp",
            "tensor_parallel_size": world_size,
            "world_size": world_size,
        },
    )
    driver.setup(reshard_prefix, reshard_params)
    reshard_result = driver.run(reshard_prefix, reshard_params)
    reshard_result.verify()

    # Hand-compute storage_data bytes touched across the 2 FSDP target ranks:
    # - Colwise TP tensors (Shard(0)): each saved chunk aligns with 1 FSDP rank -> 1x
    # - Rowwise TP tensors (Shard(1)): each saved chunk overlaps all W FSDP ranks -> Wx
    # - Replicated items (norms, scalar step tensors, BYTE_IO param_groups) -> Wx
    colwise_names = (
        "embed_tokens",
        "lm_head",
        "q_proj",
        "k_proj",
        "v_proj",
        "gate_proj",
        "up_proj",
    )
    rowwise_names = ("o_proj", "down_proj")

    tp_meta = FsspecReader(reshard_prefix).read_metadata()
    colwise_bytes = 0
    rowwise_bytes = 0
    replicated_bytes = 0
    for idx, info in tp_meta.storage_data.items():
        fqn = idx.fqn
        is_param_or_moment = not fqn.endswith(".step") and not fqn.startswith(
            "optim.param_groups."
        )
        if is_param_or_moment and any(n in fqn for n in rowwise_names):
            rowwise_bytes += info.length
        elif is_param_or_moment and any(n in fqn for n in colwise_names):
            colwise_bytes += info.length
        else:
            replicated_bytes += info.length

    expected_tp_to_fsdp = colwise_bytes + world_size * (
        rowwise_bytes + replicated_bytes
    )
    reshard_physical = sum(
        p.stat().st_size
        for p in reshard_dir.iterdir()
        if driver.is_checkpoint_file(p.name)
    )
    reshard_total = reshard_result.extra_columns["dcp_read_bytes_fetched_total"]
    assert reshard_total == expected_tp_to_fsdp
    assert reshard_result.bytes_transferred == expected_tp_to_fsdp
    assert reshard_total > reshard_physical

    # 2. Same-layout fsdp dp2 row: total ~= physical size
    fsdp_dir = tmp_path / "fsdp_same"
    fsdp_prefix = fsdp_dir.as_uri()
    fsdp_params = make_params(
        tiny_models["llama"],
        strategy="fsdp",
        dp=world_size,
        world_size=world_size,
    )
    driver.setup(fsdp_prefix, fsdp_params)
    fsdp_result = driver.run(fsdp_prefix, fsdp_params)
    fsdp_result.verify()

    fsdp_meta = FsspecReader(fsdp_prefix).read_metadata()
    sharded_bytes = 0
    fsdp_replicated_bytes = 0
    for idx, info in fsdp_meta.storage_data.items():
        item_md = fsdp_meta.state_dict_metadata[idx.fqn]
        if isinstance(item_md, TensorStorageMetadata) and len(item_md.chunks) > 1:
            sharded_bytes += info.length
        else:
            fsdp_replicated_bytes += info.length

    fsdp_physical = sum(
        p.stat().st_size
        for p in fsdp_dir.iterdir()
        if driver.is_checkpoint_file(p.name)
    )
    metadata_bytes = (fsdp_dir / ".metadata").stat().st_size
    fsdp_total = fsdp_result.extra_columns["dcp_read_bytes_fetched_total"]
    assert fsdp_total == sharded_bytes + world_size * fsdp_replicated_bytes
    # Every parameter and norm shard is fetched once; fsdp_total differs from
    # physical size only by .metadata vs duplicated scalar/BYTE_IO entries.
    assert abs(fsdp_total - fsdp_physical) <= max(fsdp_replicated_bytes, metadata_bytes)
    assert fsdp_total < reshard_total


def test_setup_checks_both_layouts_and_releases_weights(
    tmp_path, tiny_models, monkeypatch
):
    from unittest.mock import MagicMock

    fsdp8 = {"strategy": "fsdp", "data_parallel_size": 8, "world_size": 8}
    params = make_params(tiny_models["llama"], dp=2, world_size=2, source=fsdp8)
    driver = PyTorchCheckpointReadDriver()
    prepared = []
    monkeypatch.setattr(driver, "prepare", lambda p, *layouts: prepared.append(layouts))

    weights_ok = {"w": 1}
    monkeypatch.setattr(read_driver_mod, "load_weights", lambda mid: weights_ok)
    monkeypatch.setattr(read_driver_mod, "run_split", lambda *a, **kw: None)
    driver.setup((tmp_path / "ckpt_ok").as_uri(), params)
    assert prepared == [(params.source_layout(), params.target_layout())]
    assert weights_ok == {}

    weights_err = {"w": 1}
    monkeypatch.setattr(read_driver_mod, "load_weights", lambda mid: weights_err)
    monkeypatch.setattr(
        read_driver_mod,
        "run_split",
        MagicMock(side_effect=RuntimeError("source save failed")),
    )
    with pytest.raises(RuntimeError, match="source save failed"):
        driver.setup((tmp_path / "ckpt_err").as_uri(), params)
    assert weights_err == {}
