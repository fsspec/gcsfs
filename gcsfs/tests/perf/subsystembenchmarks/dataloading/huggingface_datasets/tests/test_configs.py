import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading.huggingface_datasets import (
    configs,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.huggingface_datasets.configs import (
    HuggingFaceReadConfigurator,
)

CONFIG = configs.__file__  # the configurator resolves configs.yaml next to it


def _cases():
    return HuggingFaceReadConfigurator(CONFIG).generate_cases()


_YAML_WITH_BASELINE_CLASH = """
common:
  rounds: 3
  seq_len: 2048
  batch_size: 64
  baseline:
    fmt: "pretok_parquet"
    file_count: 8
    rows_per_file: 4096
    row_group_size: 1024
    access: "sequential"
    num_workers: 8
    prefetch_factor: 2
    split_by_node: false
    world_size: 1
    rounds: 5
scenarios:
  - name: "read"
    scenario: "read"
    variants: []
"""


def test_baseline_key_colliding_with_common_is_rejected(tmp_path):
    """Ensure baseline keys colliding with common parameters raise an error."""
    (tmp_path / "configs.yaml").write_text(_YAML_WITH_BASELINE_CLASH)
    configurator = HuggingFaceReadConfigurator(str(tmp_path / "configs.py"))
    with pytest.raises(ValueError, match="rounds"):
        configurator.generate_cases()


def test_case_ids_unique_and_named():
    cases = _cases()
    assert len(cases) == len({c.name for c in cases})
    assert all(c.name.startswith("read-hf-") for c in cases)
    assert all(c.scenario == "read" for c in cases)


def test_macrobenchmark_baseline_present():
    baseline = next(c for c in _cases() if c.sweep_axis == "baseline")
    assert baseline.name == (
        "read-hf-txpq-shuf-nw16-rg10920-fc64x10920-" "splitws8div-mbis10-reg"
    )
    assert baseline.fmt == "text_parquet"
    assert baseline.file_count == 64
    assert baseline.rows_per_file == 10920
    assert baseline.row_group_size == 10920
    assert baseline.access == "shuffled"
    assert baseline.batch_size == 8
    assert baseline.num_workers == 16
    assert baseline.prefetch_factor == 2
    assert baseline.split_by_node
    assert baseline.world_size == 8
    assert baseline.shuffle_buffer_size == 10000
    assert baseline.max_buffer_input_shards == 10


def test_bucket_type_uniform_from_run_env(monkeypatch):
    monkeypatch.delenv("GCSFS_SUBSYSTEM_BUCKET_TYPE", raising=False)
    assert {c.bucket_type for c in _cases()} == {"regional"}
    monkeypatch.setenv("GCSFS_SUBSYSTEM_BUCKET_TYPE", "hns")
    assert {c.bucket_type for c in _cases()} == {"hns"}


def test_shuffle_split_controls_present():
    cases = _cases()
    controls = {
        c.max_buffer_input_shards: c
        for c in cases
        if c.access == "shuffled"
        and c.fmt == "text_parquet"
        and c.file_count == 64
        and c.num_workers == 16
        and c.prefetch_factor == 2
    }
    assert set(controls) == {8, 10, 64}
    assert all(c.split_by_node and c.world_size == 8 for c in controls.values())
    assert controls[10].sweep_axis == "baseline"
    assert controls[8].sweep_axis == "shuffle_split"
    assert controls[64].sweep_axis == "shuffle"


def test_sequential_control_present():
    sequential = [c for c in _cases() if c.access == "sequential"]
    assert len(sequential) == 1
    assert sequential[0].sweep_axis == "shuffle"
    assert sequential[0].split_by_node
    assert sequential[0].world_size == 8


def test_worker_sweep_present():
    assert {c.num_workers for c in _cases()} == {0, 1, 8, 16}


def test_all_three_formats_present():
    fmts = {c.fmt for c in _cases()}
    assert fmts == {"pretok_parquet", "text_parquet", "pretok_jsonl"}


def test_scaled_climbmix_case_present():
    cases = [c for c in _cases() if c.sweep_axis == "climbmix"]
    assert len(cases) == 1
    case = cases[0]
    assert case.fmt == "pretok_jsonl"
    assert case.file_count == 100
    assert case.rows_per_file == 6988
    assert case.access == "shuffled"
    assert case.num_workers == 16
    assert case.split_by_node
    assert case.world_size == 8
    assert case.shuffle_buffer_size == 10000
    assert case.max_buffer_input_shards == 10


def test_prefetch_variants_present():
    pfs = {c.prefetch_factor for c in _cases()}
    assert {2, 4, 8} <= pfs
    names = {c.name for c in _cases()}
    assert any(n.endswith("-pf4") for n in names)
    assert any(n.endswith("-pf8") for n in names)


def test_scale_axis_present():
    """Verify scale axis sweeps shard count holding total rows fixed."""
    cases = _cases()
    scale = [c for c in cases if c.sweep_axis == "scale"]
    assert {c.file_count for c in scale} == {512}
    base = next(c for c in cases if c.sweep_axis == "baseline")
    base_rows = base.file_count * base.rows_per_file
    for c in scale:
        assert c.file_count * c.rows_per_file == base_rows
        assert f"-fc{c.file_count}x{c.rows_per_file}-" in c.name


def test_deferred_layout_and_rowgroup_scenarios_are_absent():
    cases = _cases()
    assert not [c for c in cases if c.sweep_axis in {"layout", "rowgroup"}]
    assert not [c for c in cases if c.file_count in {4, 1024, 4096}]
