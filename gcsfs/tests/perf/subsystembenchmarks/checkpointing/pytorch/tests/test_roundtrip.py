import fsspec
import pytest
import torch.distributed as dist
import torch.distributed.checkpoint as dcp
from torch.distributed.checkpoint._fsspec_filesystem import FsspecReader, FsspecWriter
from torch.distributed.checkpoint.metadata import TensorStorageMetadata
from torch.distributed.checkpoint.state_dict import set_state_dict
from torch.multiprocessing.spawn import ProcessRaisedException

pytest.importorskip("transformers")

from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
    run_split,
    setup_distributed_env,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import build_model
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parallelize import (
    build_mesh,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.read.driver import (
    PyTorchCheckpointReadDriver,
    _build,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.state import (
    STATE_DICT_OPTIONS,
    get_fqn_state_dict,
    state_checksum,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.tests.test_parallelize import (
    _create_tiny_llama_config_dir,
    _params,
)

_SEED_A, _SEED_B = 1, 2

_STRATEGIES_4RANK = [
    pytest.param(dict(strategy="fsdp", dp=4), id="fsdp"),
    pytest.param(dict(strategy="hsdp", dpr=2, dp=2), id="hsdp"),
    pytest.param(dict(strategy="tp", tp=4), id="tp"),
    pytest.param(dict(strategy="fsdp_tp", dp=2, tp=2), id="fsdp_tp"),
]


def _roundtrip_rank(rank, world_size, port, prefix, params, q):
    setup_distributed_env(rank, world_size, port)
    try:
        mesh = build_mesh(params)
        model, optim = _build(params, mesh, _SEED_A)
        state = get_fqn_state_dict(model, optim)
        dcp.save(state, storage_writer=FsspecWriter(prefix))
        saved = state_checksum(state)

        model, optim = _build(params, mesh, _SEED_B)
        state = get_fqn_state_dict(model, optim)
        assert state_checksum(state) != saved
        dcp.load(state, storage_reader=FsspecReader(prefix))

        loaded = get_fqn_state_dict(model, optim)
        assert state_checksum(loaded) == saved
        n_exp_avg = sum(k.endswith(".exp_avg") for k in loaded["optim"])
        assert n_exp_avg == len(list(model.parameters()))

        groups = [dict(g) for g in optim.param_groups]
        set_state_dict(
            model,
            optim,
            model_state_dict=state["model"],
            optim_state_dict=state["optim"],
            options=STATE_DICT_OPTIONS,
        )
        assert optim.param_groups == groups
        q.put(None)
    finally:
        dist.destroy_process_group()


def _run_roundtrip(tmp_path, world_size, sizes):
    config_dir = _create_tiny_llama_config_dir(tmp_path, num_layers=8)
    params = _params(config_dir, world_size=world_size, **sizes)
    prefix = (tmp_path / "ckpt").as_uri()
    assert run_split(prefix, params, _roundtrip_rank, world_size) == []


@pytest.mark.parametrize("sizes", _STRATEGIES_4RANK)
def test_roundtrip(tmp_path, sizes):
    _run_roundtrip(tmp_path, 4, sizes)


@pytest.mark.parametrize(
    "sizes",
    [
        pytest.param(dict(strategy="hsdp_tp", dpr=2, dp=2, tp=2), id="hsdp_tp"),
        pytest.param(dict(strategy="pp", pp=8), id="pp"),
        pytest.param(dict(strategy="pp_fsdp", pp=2, dp=4), id="pp_fsdp"),
        pytest.param(dict(strategy="pp_fsdp_tp", pp=2, dp=2, tp=2), id="pp_fsdp_tp"),
    ],
)
def test_roundtrip_8rank(tmp_path, sizes):
    _run_roundtrip(tmp_path, 8, sizes)


@pytest.mark.parametrize(
    "sizes",
    [
        pytest.param(
            dict(strategy="pp_hsdp_tp", pp=2, dpr=2, dp=2, tp=2), id="pp_hsdp_tp"
        )
    ],
)
def test_roundtrip_16rank(tmp_path, sizes):
    _run_roundtrip(tmp_path, 16, sizes)


def _checksum_rank(rank, world_size, port, prefix, params, q):
    setup_distributed_env(rank, world_size, port)
    try:
        model, optim = _build(params, build_mesh(params), _SEED_A)
        q.put((rank, state_checksum(get_fqn_state_dict(model, optim))))
    finally:
        dist.destroy_process_group()


def _rank_checksums(tmp_path, world_size, **sizes):
    import torch.multiprocessing as mp

    from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import find_free_port

    params = _params(
        _create_tiny_llama_config_dir(tmp_path), world_size=world_size, **sizes
    )
    with mp.get_context("spawn").Manager() as manager:
        q = manager.Queue()
        mp.spawn(
            _checksum_rank,
            args=(world_size, find_free_port(), None, params, q),
            nprocs=world_size,
            join=True,
        )
        return dict(q.get() for _ in range(world_size))


_Q = "model.layers.0.self_attn.q_proj.weight"
_NORM = "model.layers.0.input_layernorm.weight"


def test_seeding_differs_per_shard(tmp_path):
    # Shards of one tensor must not be byte-identical across ranks, or the
    # per-rank checksum cannot tell a reader serving another rank's shard.
    r = _rank_checksums(tmp_path, 4, strategy="fsdp", dp=4)
    for fqn in (_Q, _NORM):
        for key in (f"model.{fqn}", f"optim.state.{fqn}.exp_avg"):
            assert len({r[rank][key] for rank in range(4)}) == 4
    assert len({r[rank][f"optim.state.{_Q}.step"] for rank in range(4)}) == 1


def test_seeding_keeps_replicas_identical(tmp_path):
    # dcp.save keeps one copy of a replicated tensor, so replicas must agree
    # with each other at setup or every other replica fails the load checksum.
    r = _rank_checksums(tmp_path, 4, strategy="hsdp", dpr=2, dp=2)
    assert r[0] == r[2] and r[1] == r[3]  # dp_replicate copies of each shard
    assert r[0][f"model.{_Q}"] != r[1][f"model.{_Q}"]

    # TP leaves norms as plain Parameters replicated on every rank.
    r = _rank_checksums(tmp_path, 4, strategy="tp", tp=4)
    assert len({r[rank][f"model.{_NORM}"] for rank in range(4)}) == 1
    assert len({r[rank][f"model.{_Q}"] for rank in range(4)}) == 4

    # fsdp_tp shards norms on dp_shard only, so ranks 0 and 1 (tp 0/1) share them.
    r = _rank_checksums(tmp_path, 4, strategy="fsdp_tp", dp=2, tp=2)
    assert r[0][f"model.{_NORM}"] == r[1][f"model.{_NORM}"]
    assert r[0][f"model.{_NORM}"] != r[2][f"model.{_NORM}"]
    assert len({r[rank][f"model.{_Q}"] for rank in range(4)}) == 4


def _n_bytes_entries(prefix):
    """Returns the number of non-tensor (BYTE_IO) entries in the checkpoint."""
    metadata = FsspecReader(prefix).read_metadata()
    return sum(
        not isinstance(md, TensorStorageMetadata)
        for md in metadata.state_dict_metadata.values()
    )


@pytest.mark.parametrize("sizes", _STRATEGIES_4RANK)
def test_driver_local_fs(tmp_path, sizes):
    params = _params(
        _create_tiny_llama_config_dir(tmp_path), world_size=4, rounds=2, **sizes
    )
    prefix = (tmp_path / "ckpt").as_uri()
    driver = PyTorchCheckpointReadDriver()

    driver.setup(prefix, params)
    result = driver.run(prefix, params)
    result.verify()

    cols = result.extra_columns
    assert len(result.durations) == 2
    assert all(d > 0 for d in result.durations)
    assert cols.keys() == {"dcp_read_items_per_rank_max"}

    # One read item per local tensor (param, exp_avg, exp_avg_sq, step) plus
    # one per BYTE_IO param_groups entry.
    n_local_tensors = 4 * len(list(build_model(params).parameters()))
    n_items = n_local_tensors + _n_bytes_entries(prefix)
    assert cols["dcp_read_items_per_rank_max"] == n_items
    assert driver.read_count(params) == sizes.get("dpr", 1)

    fs, root = fsspec.core.url_to_fs(prefix)
    files = fs.find(root)
    ckpt_files = [f for f in files if driver.is_checkpoint_file(f)]
    assert any(f.endswith(".metadata") for f in ckpt_files)
    assert sum(f.endswith(".distcp") for f in ckpt_files) == 4
    assert sum("checksums" in f for f in files) == 4
    assert not any("checksums" in f for f in ckpt_files)


def test_driver_local_fs_pp(tmp_path):
    params = _params(
        _create_tiny_llama_config_dir(tmp_path, num_layers=8), "pp", pp=8, world_size=8
    )
    prefix = (tmp_path / "ckpt").as_uri()
    driver = PyTorchCheckpointReadDriver()

    driver.setup(prefix, params)
    result = driver.run(prefix, params)
    result.verify()

    # The last stage owns one layer plus norm and lm_head. Each param has 3
    # tensors, a step, and its own BYTE_IO param_groups entries.
    model = build_model(params)
    n_last_stage_params = len(list(model.model.layers[0].parameters())) + 2
    n_bytes_per_param = _n_bytes_entries(prefix) // len(list(model.parameters()))
    n_items = (4 + n_bytes_per_param) * n_last_stage_params
    assert result.extra_columns["dcp_read_items_per_rank_max"] == n_items
    assert driver.read_count(params) == 1


def test_checksum_mismatch_raises(tmp_path):
    params = _params(
        _create_tiny_llama_config_dir(tmp_path), "fsdp", dp=4, world_size=4
    )
    prefix = (tmp_path / "ckpt").as_uri()
    driver = PyTorchCheckpointReadDriver()
    driver.setup(prefix, params)

    # The largest saved item is mostly raw tensor bytes, so its middle is payload.
    storage = FsspecReader(prefix).read_metadata().storage_data
    info = max(storage.values(), key=lambda s: s.length)
    path = tmp_path / "ckpt" / info.relative_path
    data = bytearray(path.read_bytes())
    mid = info.offset + info.length // 2
    data[mid : mid + 16] = bytes(b ^ 0xFF for b in data[mid : mid + 16])
    path.write_bytes(bytes(data))

    # run() returns the timings; the checksum check runs in verify(), which
    # the harness calls after the monitored window.
    result = driver.run(prefix, params)
    with pytest.raises(ProcessRaisedException, match="checksum mismatch"):
        result.verify()
