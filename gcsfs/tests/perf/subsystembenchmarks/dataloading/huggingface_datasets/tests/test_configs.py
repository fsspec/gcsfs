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
        "read-hf-txpq-shuf-nw4-rg1800-fc64x10920-" "splitws8div-mbis10-reg"
    )
    assert baseline.fmt == "text_parquet"
    assert baseline.file_count == 64
    assert baseline.rows_per_file == 10920
    assert baseline.row_group_size == 1800
    assert baseline.access == "shuffled"
    assert baseline.batch_size == 8
    assert baseline.num_workers == 4
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


def test_non_split_axes_inherit_default_shuffle_grouping():
    cases = _cases()
    non_split_axes = {"workers", "format", "climbmix", "scale", "prefetch"}
    selected = [c for c in cases if c.sweep_axis in non_split_axes]

    assert selected
    assert all(c.max_buffer_input_shards == 10 for c in selected)
    assert not [c for c in cases if c.sweep_axis.endswith("_split_fallback")]


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
