import pytest

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch import configs
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.configs import (
    PyTorchCheckpointConfigurator,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parameters import (
    PyTorchCheckpointParameters,
)

CONFIG = configs.__file__


def _cases():
    return PyTorchCheckpointConfigurator(CONFIG).generate_cases()


def test_case_names():
    cases = _cases()
    names = [c.name for c in cases]
    expected_names = [
        "load-llama_3_1_8b-fsdp-ws8-reg",
        "load-llama_3_1_8b-hsdp-dpr2dp4-ws8-reg",
        "load-llama_3_1_8b-hsdp-dpr4dp2-ws8-reg",
        "load-llama_3_1_8b-tp-tp8-ws8-reg",
        "load-llama_3_1_8b-fsdp-tp-dp2tp4-ws8-reg",
        "load-llama_3_1_8b-hsdp-tp-dpr2dp2tp2-ws8-reg",
    ]
    assert names == expected_names


def test_mesh_product_equals_world_size():
    cases = _cases()
    assert len(cases) == 6
    for c in cases:
        mesh_product = (
            c.data_parallel_replicate_size
            * c.data_parallel_size
            * c.tensor_parallel_size
            * c.pipeline_parallel_size
        )
        assert mesh_product == c.world_size
        assert c.world_size == 8


@pytest.mark.parametrize(
    "sizes",
    [
        pytest.param(dict(strategy="tp", data_parallel_size=8), id="tp-without-tp"),
        pytest.param(
            dict(strategy="fsdp", data_parallel_replicate_size=2, data_parallel_size=4),
            id="fsdp-with-replicate",
        ),
    ],
)
def test_validate_case_rejects_sizes_unused_by_strategy(sizes):
    p = PyTorchCheckpointParameters(
        name="c",
        bucket_name="",
        bucket_type="regional",
        rounds=1,
        scenario="checkpoint_read",
        framework="pytorch",
        model_id="m",
        world_size=8,
        **sizes,
    )
    with pytest.raises(ValueError, match="needs non-unit sizes"):
        PyTorchCheckpointConfigurator(CONFIG).validate_case(p)


def test_baseline_is_fsdp():
    cases = _cases()
    baseline_cases = [c for c in cases if c.sweep_axis == "baseline"]
    assert len(baseline_cases) == 1
    baseline = baseline_cases[0]
    assert baseline.strategy == "fsdp"
    assert baseline.name == "load-llama_3_1_8b-fsdp-ws8-reg"
    assert baseline.scenario == "checkpoint_read"

    strategy_cases = [c for c in cases if c.sweep_axis == "strategy"]
    assert len(strategy_cases) == 5
    for c in strategy_cases:
        assert c.scenario == "checkpoint_read"


def test_extra_columns():
    cases = _cases()
    for c in cases:
        extra = c.extra_columns()
        assert "data_parallel_replicate_size" in extra
        assert "pipeline_parallel_size" in extra
        assert extra["data_parallel_replicate_size"] == c.data_parallel_replicate_size
        assert extra["pipeline_parallel_size"] == c.pipeline_parallel_size
