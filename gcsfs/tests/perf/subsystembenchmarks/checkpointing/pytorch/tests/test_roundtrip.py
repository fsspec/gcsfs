import pytest
import torch.distributed as dist
import torch.distributed.checkpoint as dcp
from torch.distributed.checkpoint._fsspec_filesystem import FsspecReader, FsspecWriter
from torch.distributed.tensor import DTensor

pytest.importorskip("transformers")

from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
    run_split,
    setup_distributed_env,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (
    build_model,
    build_optimizer,
    materialize,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parallelize import (
    build_mesh,
    parallelize,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.state import (
    get_fqn_state_dict,
    state_checksum,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.tests.test_parallelize import (
    _create_tiny_llama_config_dir,
    _params,
)

_SEED_A, _SEED_B = 1, 2


def _build(params, mesh, seed):
    model = parallelize(build_model(params), mesh, params)
    materialize(model, seed)
    optim = build_optimizer(model)
    # Seed-dependent optimizer state so the round trip also checks it is loaded.
    for state in optim.state.values():
        for t in (state["exp_avg"], state["exp_avg_sq"]):
            (t.to_local() if isinstance(t, DTensor) else t).normal_()
        state["step"].fill_(seed)
    return model, optim


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
        assert len(loaded["optim"]["state"]) == len(list(model.parameters()))
        q.put(None)
    finally:
        dist.destroy_process_group()


def _run_roundtrip(tmp_path, world_size, sizes):
    params = _params(_create_tiny_llama_config_dir(tmp_path), **sizes)
    prefix = (tmp_path / "ckpt").as_uri()
    assert run_split(prefix, params, _roundtrip_rank, world_size) == []


@pytest.mark.parametrize(
    "sizes",
    [
        pytest.param(dict(strategy="fsdp", dp=4), id="fsdp"),
        pytest.param(dict(strategy="hsdp", dpr=2, dp=2), id="hsdp"),
        pytest.param(dict(strategy="tp", tp=4), id="tp"),
        pytest.param(dict(strategy="fsdp_tp", dp=2, tp=2), id="fsdp_tp"),
    ],
)
def test_roundtrip(tmp_path, sizes):
    _run_roundtrip(tmp_path, 4, sizes)


@pytest.mark.parametrize(
    "sizes",
    [pytest.param(dict(strategy="hsdp_tp", dpr=2, dp=2, tp=2), id="hsdp_tp")],
)
def test_roundtrip_8rank(tmp_path, sizes):
    _run_roundtrip(tmp_path, 8, sizes)
