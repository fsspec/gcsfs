import dataclasses

import pytest

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.layout import (
    STRATEGY_MESH_DIMS,
    Layout,
)


@pytest.mark.parametrize(
    "layout, expected",
    [
        (Layout("fsdp", 8, data_parallel_size=8), "fsdp-ws8"),
        (Layout("fsdp", 1, data_parallel_size=1), "fsdp"),
        (
            Layout("hsdp", 8, data_parallel_replicate_size=2, data_parallel_size=4),
            "hsdp-dpr2dp4-ws8",
        ),
        (Layout("tp", 8, tensor_parallel_size=8), "tp-tp8-ws8"),
        (
            Layout("fsdp_tp", 8, data_parallel_size=2, tensor_parallel_size=4),
            "fsdp-tp-dp2tp4-ws8",
        ),
        (
            Layout(
                "hsdp_tp",
                8,
                data_parallel_replicate_size=2,
                data_parallel_size=2,
                tensor_parallel_size=2,
            ),
            "hsdp-tp-dpr2dp2tp2-ws8",
        ),
        (Layout("pp", 8, pipeline_parallel_size=8), "pp-pp8-ws8"),
        (
            Layout("pp_fsdp", 8, pipeline_parallel_size=2, data_parallel_size=4),
            "pp-fsdp-pp2dp4-ws8",
        ),
        (
            Layout(
                "pp_fsdp_tp",
                8,
                pipeline_parallel_size=2,
                data_parallel_size=2,
                tensor_parallel_size=2,
            ),
            "pp-fsdp-tp-pp2dp2tp2-ws8",
        ),
        (Layout("pp_hsdp_tp", 16, 2, 2, 2, 2), "pp-hsdp-tp-pp2dpr2dp2tp2-ws16"),
        (
            Layout("ep", 8, data_parallel_size=8, expert_parallel_size=8),
            "ep-dp8ep8-ws8",
        ),
    ],
)
def test_layout_token_matches_main_ids(layout, expected):
    assert layout.token() == expected


def test_strategy_mesh_dims_has_all_ten_strategies():
    assert set(STRATEGY_MESH_DIMS) == {
        "fsdp",
        "hsdp",
        "tp",
        "fsdp_tp",
        "hsdp_tp",
        "pp",
        "pp_fsdp",
        "pp_fsdp_tp",
        "pp_hsdp_tp",
        "ep",
    }


def test_layout_is_frozen_and_orders_mesh_dims():
    layout = Layout(
        strategy="pp_hsdp_tp",
        world_size=16,
        pipeline_parallel_size=2,
        data_parallel_replicate_size=2,
        data_parallel_size=2,
        tensor_parallel_size=2,
    )
    assert layout.mesh_dims() == [
        ("pp", 2),
        ("dp_replicate", 2),
        ("dp_shard", 2),
        ("tp", 2),
    ]
    assert Layout(
        "ep", 8, data_parallel_size=8, expert_parallel_size=8
    ).mesh_dims() == [("dp_shard", 8)]
    with pytest.raises(dataclasses.FrozenInstanceError):
        layout.world_size = 8


def test_layout_validate_rejects_bad_mesh_product():
    layout = Layout(
        strategy="fsdp_tp",
        world_size=8,
        data_parallel_size=2,
        tensor_parallel_size=2,
    )
    with pytest.raises(
        ValueError,
        match=r"Mesh product \(dpr=1 \* dp=2 \* tp=2 \* pp=1\) = 4 must equal world_size=8",
    ):
        layout.validate()


def test_layout_validate_rejects_ep_size_mismatch():
    ep_mismatch = Layout(
        strategy="ep",
        world_size=8,
        data_parallel_size=8,
        expert_parallel_size=4,
    )
    with pytest.raises(
        ValueError,
        match="expert_parallel_size=4 must be 8 for strategy 'ep'",
    ):
        ep_mismatch.validate()

    non_ep_mismatch = Layout(
        strategy="fsdp",
        world_size=8,
        data_parallel_size=8,
        expert_parallel_size=2,
    )
    with pytest.raises(
        ValueError,
        match="expert_parallel_size=2 must be 1 for strategy 'fsdp'",
    ):
        non_ep_mismatch.validate()


def test_layout_validate_rejects_unsupported_strategy_and_wrong_dims():
    with pytest.raises(
        ValueError,
        match="Unsupported strategy 'ddp' for framework 'pytorch'",
    ):
        Layout(strategy="ddp", world_size=8, data_parallel_size=8).validate()

    with pytest.raises(
        ValueError,
        match=r"Strategy 'tp' needs non-unit sizes for exactly \('tp',\)",
    ):
        Layout(strategy="tp", world_size=8, data_parallel_size=8).validate()


@pytest.mark.parametrize(
    "kwargs, match",
    [
        pytest.param(
            dict(
                strategy="fsdp",
                world_size=0,
                data_parallel_size=8,
                tensor_parallel_size=0,
            ),
            "world_size",
            id="zero-world-and-tp",
        ),
        pytest.param(
            dict(
                strategy="fsdp",
                world_size=8,
                data_parallel_size=8,
                data_parallel_replicate_size=-1,
                tensor_parallel_size=-1,
            ),
            "data_parallel_replicate_size",
            id="negative-pair-canceling-in-product",
        ),
        pytest.param(
            dict(
                strategy="fsdp",
                world_size=8,
                data_parallel_size=8,
                pipeline_parallel_size=0,
            ),
            "pipeline_parallel_size",
            id="zero-pp",
        ),
        pytest.param(
            dict(
                strategy="fsdp",
                world_size=8,
                data_parallel_size=8,
                expert_parallel_size=0,
            ),
            "expert_parallel_size",
            id="zero-ep",
        ),
        pytest.param(
            dict(
                strategy="fsdp",
                world_size=8,
                data_parallel_size=8,
                tensor_parallel_size=True,
            ),
            "tensor_parallel_size",
            id="bool-tp",
        ),
        pytest.param(
            dict(strategy="fsdp", world_size=True, data_parallel_size=1),
            "world_size",
            id="bool-world-size",
        ),
        pytest.param(
            dict(strategy="fsdp", world_size=8, data_parallel_size=8.0),
            "data_parallel_size",
            id="float-dp",
        ),
    ],
)
def test_layout_validate_rejects_non_positive_or_non_int_sizes(kwargs, match):
    with pytest.raises(ValueError, match=match):
        Layout(**kwargs).validate()


def test_layout_validate_allows_world_size_above_max_ranks():
    Layout(strategy="fsdp", world_size=64, data_parallel_size=64).validate()


@pytest.mark.parametrize(
    "m, match",
    [
        pytest.param(["fsdp"], "layout must be a mapping", id="not-a-mapping"),
        pytest.param(
            {"strategy": "fsdp", "world_size": 8, "dp": 8},
            r"Unknown layout keys: \['dp'\]",
            id="unknown-key",
        ),
        pytest.param(
            {"strategy": "fsdp", "data_parallel_size": 8},
            r"Missing required layout keys: \['world_size'\]",
            id="missing-world-size",
        ),
        pytest.param(
            {"world_size": 8},
            r"Missing required layout keys: \['strategy'\]",
            id="missing-strategy",
        ),
    ],
)
def test_from_mapping_rejects_unknown_keys(m, match):
    with pytest.raises(ValueError, match=match):
        Layout.from_mapping(m)


def test_from_mapping_constructs_layout_with_defaults():
    layout = Layout.from_mapping(
        {"strategy": "fsdp", "world_size": 8, "data_parallel_size": 8}
    )
    assert layout == Layout("fsdp", 8, data_parallel_size=8)
