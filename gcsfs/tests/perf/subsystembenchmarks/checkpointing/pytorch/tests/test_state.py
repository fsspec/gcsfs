import functools

import pytest
import torch
import torch.distributed as dist
import torch.distributed.checkpoint as dcp
from torch.distributed.checkpoint.default_planner import DefaultLoadPlanner
from torch.distributed.checkpoint.filesystem import _StorageInfo
from torch.distributed.checkpoint.fsspec_filesystem import FsspecReader, FsspecWriter
from torch.distributed.checkpoint.metadata import (
    BytesStorageMetadata,
    ChunkStorageMetadata,
    Metadata,
    MetadataIndex,
    TensorProperties,
    TensorStorageMetadata,
)
from torch.distributed.checkpoint.planner import (
    LoadItemType,
    LoadPlan,
    ReadItem,
    SavePlan,
    TensorWriteData,
    WriteItem,
    WriteItemType,
)
from torch.distributed.tensor import DTensor
from torch.testing._internal.distributed.fake_pg import FakeStore

pytest.importorskip("transformers")

from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (  # noqa: E402
    run_split,
    setup_distributed_env,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.layout import (  # noqa: E402
    Layout,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (  # noqa: E402
    RUN_SEED,
    SETUP_SEED,
    build,
    expected_local,
    expected_lr,
    expected_step,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.planners import (  # noqa: E402
    RecordingLoadPlanner,
    RecordingSavePlanner,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.reference import (  # noqa: E402
    load_weights,
    local_slice,
    random_full,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.state import (  # noqa: E402
    assert_loaded_state,
    expected_bytes_requested,
    get_fqn_state_dict,
    verify_state,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.tests.tiny import (  # noqa: E402
    make_params,
    tiny_model_dir,
)

_STRATEGIES_4RANK = [
    ("llama", Layout("fsdp", world_size=4, data_parallel_size=4)),
    (
        "llama",
        Layout(
            "hsdp",
            world_size=4,
            data_parallel_replicate_size=2,
            data_parallel_size=2,
        ),
    ),
    ("llama", Layout("tp", world_size=4, tensor_parallel_size=4)),
    (
        "llama",
        Layout(
            "fsdp_tp",
            world_size=4,
            data_parallel_size=2,
            tensor_parallel_size=2,
        ),
    ),
    ("llama", Layout("pp", world_size=4, pipeline_parallel_size=4)),
    (
        "olmoe",
        Layout(
            "ep",
            world_size=4,
            data_parallel_size=4,
            expert_parallel_size=4,
        ),
    ),
]


def _build_then_verify_rank(
    rank, world_size, port, prefix, params, q, *, models_and_weights
):
    cpu = setup_distributed_env(rank, world_size, port)
    try:
        for kind, layout in _STRATEGIES_4RANK:
            model_path, weights = models_and_weights[kind]
            model, optim = build(model_path, layout, SETUP_SEED, weights, cpu)

            named_params = list(model.named_parameters())
            assert len(named_params) > 0, layout
            assert all(p.dtype == torch.float32 for _, p in named_params), layout
            assert all(p.grad is None for _, p in named_params), layout
            assert len(optim.state) == len(named_params), layout
            assert len(optim.param_groups) == 1, layout
            assert optim.defaults["foreach"] is False, layout

            # Independent spot-check on the first parameter owned by this rank:
            # SETUP_SEED loads real model weights cast to fp32, step=1000.0, lr=1e-3.
            first_fqn, first_p = named_params[0]
            first_local = (
                first_p.to_local() if isinstance(first_p, DTensor) else first_p
            )
            assert torch.equal(
                first_local,
                local_slice(weights[first_fqn].to(dtype=torch.float32), first_p),
            ), (layout, first_fqn)

            state = get_fqn_state_dict(model, optim)
            assert state["optim"][f"state.{first_fqn}.step"].item() == 1000.0
            assert state["optim"][f"param_groups.{first_fqn}.lr"] == 1e-3

            assert verify_state(state, SETUP_SEED, weights, cpu) == [], layout
            mismatches = verify_state(state, RUN_SEED, weights, cpu)
            assert len(mismatches) > 0, layout

            # Also verify RUN_SEED state passes against RUN_SEED and fails against SETUP_SEED.
            run_model, run_optim = build(model_path, layout, RUN_SEED, weights, cpu)
            run_state = get_fqn_state_dict(run_model, run_optim)
            assert verify_state(run_state, RUN_SEED, weights, cpu) == [], layout
            assert len(verify_state(run_state, SETUP_SEED, weights, cpu)) > 0, layout

        q.put(None)
    finally:
        dist.destroy_process_group()


def test_build_then_verify_passes(tiny_models):
    models_and_weights = {
        kind: (path, load_weights(path)) for kind, path in tiny_models.items()
    }
    target = functools.partial(
        _build_then_verify_rank, models_and_weights=models_and_weights
    )
    assert (
        run_split(None, make_params(tiny_models["llama"], world_size=4), target, 4)
        == []
    )


def test_verify_names_corrupted_key(tmp_path):
    llama_dir = tiny_model_dir(tmp_path, kind="llama", num_layers=2)
    weights = load_weights(llama_dir)
    layout = Layout("fsdp", world_size=4, data_parallel_size=4)
    cpu = torch.device("cpu")
    target_fqn = "model.layers.0.mlp.up_proj.weight"

    dist.init_process_group("fake", store=FakeStore(), rank=0, world_size=4)
    try:
        model, optim = build(llama_dir, layout, SETUP_SEED, weights, cpu)
        state = get_fqn_state_dict(model, optim)
        assert verify_state(state, SETUP_SEED, weights, cpu) == []

        # Flip one element of model.layers.0.mlp.up_proj.weight's local shard.
        up_local = state["model"][target_fqn].to_local()
        orig_up = up_local.reshape(-1)[0].item()
        with torch.no_grad():
            up_local.reshape(-1)[0] = orig_up + 1.0
        bad = verify_state(state, SETUP_SEED, weights, cpu)
        assert target_fqn in bad
        assert bad == [target_fqn]
        with torch.no_grad():
            up_local.reshape(-1)[0] = orig_up
        assert verify_state(state, SETUP_SEED, weights, cpu) == []

        # Flip optimizer moments, step, and lr individually.
        for moment in ("exp_avg", "exp_avg_sq"):
            m_key = f"state.{target_fqn}.{moment}"
            m_local = state["optim"][m_key].to_local()
            orig_m = m_local.reshape(-1)[0].item()
            with torch.no_grad():
                m_local.reshape(-1)[0] = orig_m + 1.0
            assert verify_state(state, SETUP_SEED, weights, cpu) == [m_key]
            with torch.no_grad():
                m_local.reshape(-1)[0] = orig_m

        step_key = f"state.{target_fqn}.step"
        state["optim"][step_key].fill_(expected_step(RUN_SEED))
        assert verify_state(state, SETUP_SEED, weights, cpu) == [step_key]
        state["optim"][step_key].fill_(expected_step(SETUP_SEED))

        lr_key = f"param_groups.{target_fqn}.lr"
        state["optim"][lr_key] = expected_lr(RUN_SEED)
        assert verify_state(state, SETUP_SEED, weights, cpu) == [lr_key]
        state["optim"][lr_key] = expected_lr(SETUP_SEED)
        assert verify_state(state, SETUP_SEED, weights, cpu) == []

        # assert_loaded_state checks byte count first, then verify_state.
        bytes_expected = expected_bytes_requested(state)
        assert_loaded_state(
            0, state, bytes_expected, bytes_expected, SETUP_SEED, weights, cpu
        )
        with pytest.raises(RuntimeError, match="rank 2.*123.*456"):
            assert_loaded_state(2, state, 123, 456, SETUP_SEED, weights, cpu)

        with torch.no_grad():
            up_local.reshape(-1)[0] = orig_up + 1.0
        with pytest.raises(RuntimeError, match=f"rank 3.*{target_fqn}"):
            assert_loaded_state(
                3, state, bytes_expected, bytes_expected, SETUP_SEED, weights, cpu
            )
    finally:
        dist.destroy_process_group()


def _planners_roundtrip_rank(rank, world_size, port, prefix, params, q, *, weights):
    cpu = setup_distributed_env(rank, world_size, port)
    try:
        layout = Layout("fsdp", world_size=world_size, data_parallel_size=world_size)
        model, optim = build(params, layout, SETUP_SEED, weights, cpu)
        save_state = get_fqn_state_dict(model, optim)
        save_planner = RecordingSavePlanner()
        dcp.save(save_state, storage_writer=FsspecWriter(prefix), planner=save_planner)
        assert save_planner.bytes_written > 0
        written_total = torch.tensor(save_planner.bytes_written, dtype=torch.int64)
        dist.all_reduce(written_total, op=dist.ReduceOp.SUM)
        md = FsspecReader(prefix).read_metadata().state_dict_metadata
        expected_saved_bytes = sum(
            c.sizes.numel() * item_md.properties.dtype.itemsize
            for item_md in md.values()
            if isinstance(item_md, TensorStorageMetadata)
            for c in item_md.chunks
        )
        assert written_total.item() == expected_saved_bytes

        load_model, load_optim = build(params, layout, RUN_SEED, weights, cpu)
        load_state = get_fqn_state_dict(load_model, load_optim)
        bytes_expected = expected_bytes_requested(load_state)
        load_planner = RecordingLoadPlanner()
        dcp.load(load_state, storage_reader=FsspecReader(prefix), planner=load_planner)

        assert load_planner.read_items > 0
        assert load_planner.bytes_requested == bytes_expected
        assert load_planner.bytes_fetched >= bytes_expected
        assert_loaded_state(
            rank,
            load_state,
            load_planner.bytes_requested,
            bytes_expected,
            SETUP_SEED,
            weights,
            cpu,
        )
        q.put(None)
    finally:
        dist.destroy_process_group()


def test_recording_planners_and_dcp_roundtrip(tmp_path, monkeypatch):
    llama_dir = tiny_model_dir(tmp_path / "llama", kind="llama", num_layers=2)
    weights = load_weights(llama_dir)
    prefix = (tmp_path / "ckpt").as_uri()
    target = functools.partial(_planners_roundtrip_rank, weights=weights)
    assert run_split(prefix, make_params(llama_dir, world_size=2), target, 2) == []

    # Unit-check deduplicated storage_index accounting in RecordingLoadPlanner
    # and tensor-only accounting in RecordingSavePlanner.
    shared_idx = MetadataIndex("w", [0, 0])
    byte_idx = MetadataIndex("pg")
    load_plan = LoadPlan(
        [
            ReadItem(
                type=LoadItemType.TENSOR,
                dest_index=MetadataIndex("w", [0, 0]),
                dest_offsets=torch.Size([0, 0]),
                storage_index=shared_idx,
                storage_offsets=torch.Size([0, 0]),
                lengths=torch.Size([2, 4]),
            ),
            ReadItem(
                type=LoadItemType.TENSOR,
                dest_index=MetadataIndex("w", [2, 0]),
                dest_offsets=torch.Size([2, 0]),
                storage_index=shared_idx,
                storage_offsets=torch.Size([2, 0]),
                lengths=torch.Size([2, 4]),
            ),
            ReadItem(
                type=LoadItemType.BYTE_IO,
                dest_index=byte_idx,
                dest_offsets=torch.Size([]),
                storage_index=byte_idx,
                storage_offsets=torch.Size([]),
                lengths=torch.Size([]),
            ),
        ]
    )
    metadata = Metadata(
        state_dict_metadata={
            "w": TensorStorageMetadata(
                properties=TensorProperties(dtype=torch.float32),
                size=torch.Size([4, 4]),
                chunks=[
                    ChunkStorageMetadata(
                        offsets=torch.Size([0, 0]), sizes=torch.Size([4, 4])
                    )
                ],
            ),
            "pg": BytesStorageMetadata(),
        },
        storage_data={
            shared_idx: _StorageInfo(
                relative_path="__0_0.distcp", offset=0, length=1000
            ),
            byte_idx: _StorageInfo(
                relative_path="__0_0.distcp", offset=1000, length=64
            ),
        },
    )
    monkeypatch.setattr(DefaultLoadPlanner, "create_local_plan", lambda self: load_plan)
    lp = RecordingLoadPlanner()
    lp.metadata = metadata
    lp.create_local_plan()
    assert lp.read_items == 3
    assert lp.bytes_requested == (2 * 4 + 2 * 4) * 4
    assert lp.bytes_fetched == 1000 + 64

    save_plan = SavePlan(
        [
            WriteItem(
                index=MetadataIndex("w0", [0, 0]),
                type=WriteItemType.SHARD,
                tensor_data=TensorWriteData(
                    chunk=ChunkStorageMetadata(
                        offsets=torch.Size([0, 0]), sizes=torch.Size([3, 5])
                    ),
                    properties=TensorProperties(dtype=torch.float32),
                    size=torch.Size([6, 5]),
                ),
            ),
            WriteItem(index=MetadataIndex("pg"), type=WriteItemType.BYTE_IO),
        ]
    )
    sp = RecordingSavePlanner()
    sp.finish_plan(save_plan)
    assert sp.bytes_written == 3 * 5 * 4


def test_expected_local_matches_full_slice(tmp_path):
    llama_dir = tiny_model_dir(tmp_path, kind="llama", num_layers=2)
    weights = load_weights(llama_dir)
    cpu = torch.device("cpu")
    layouts = [
        Layout("fsdp_tp", 4, data_parallel_size=2, tensor_parallel_size=2),
        Layout("tp", 4, tensor_parallel_size=4),
    ]

    dist.init_process_group("fake", store=FakeStore(), rank=0, world_size=4)
    try:
        for layout in layouts:
            model, optim = build(llama_dir, layout, SETUP_SEED, weights, cpu)
            for seed in (SETUP_SEED, RUN_SEED):
                for fqn, p in model.named_parameters():
                    full = (
                        weights[fqn].float()
                        if seed == SETUP_SEED
                        else random_full(seed, fqn, p.shape, cpu)
                    )
                    assert torch.equal(
                        expected_local(fqn, p, seed, weights, cpu),
                        local_slice(full, p),
                    ), fqn
                    for moment, ref in (
                        (
                            "exp_avg",
                            random_full(seed, f"{fqn}.exp_avg", p.shape, cpu) * 1e-3,
                        ),
                        (
                            "exp_avg_sq",
                            random_full(
                                seed, f"{fqn}.exp_avg_sq", p.shape, cpu
                            ).square()
                            * 1e-6,
                        ),
                    ):
                        m = optim.state[p][moment]
                        assert torch.equal(
                            expected_local(f"{fqn}.{moment}", m, seed, weights, cpu),
                            local_slice(ref, m),
                        )

        first_fqn, first_p = next(iter(model.named_parameters()))
        sharded_model, _ = build(llama_dir, layouts[0], SETUP_SEED, weights, cpu)
        q_fqn = "model.layers.0.self_attn.q_proj.weight"
        q_param = dict(sharded_model.named_parameters())[q_fqn]
        fp32_weights = {**weights, q_fqn: weights[q_fqn].float()}
        q_got = expected_local(q_fqn, q_param, SETUP_SEED, fp32_weights, cpu)
        assert q_got.numel() < fp32_weights[q_fqn].numel()
        assert q_got.untyped_storage().nbytes() == q_got.numel() * 4

        with pytest.raises(ValueError, match="shape"):
            expected_local(
                first_fqn,
                first_p,
                SETUP_SEED,
                {**weights, first_fqn: torch.zeros(1)},
                cpu,
            )
    finally:
        dist.destroy_process_group()
