import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data import configs
from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.configs import (
    RayDataReadConfigurator,
)

CONFIG = configs.__file__


def _cases():
    return RayDataReadConfigurator(CONFIG).generate_cases()


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
    num_workers: 0
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
    configurator = RayDataReadConfigurator(str(tmp_path / "configs.py"))
    with pytest.raises(ValueError, match="rounds"):
        configurator.generate_cases()


def test_case_ids_unique_and_named():
    cases = _cases()
    assert len(cases) == len({c.name for c in cases})
    assert all(c.name.startswith("read-ray-") for c in cases)
    assert all(c.scenario == "read" for c in cases)


def test_baseline_present():
    baseline = next(c for c in _cases() if c.sweep_axis == "baseline")
    assert baseline.name == ("read-ray-ptpq-shuf-nw0-rg1800-fc64x10920-splitws8div-reg")
    assert baseline.fmt == "pretok_parquet"
    assert baseline.file_count == 64
    assert baseline.rows_per_file == 10920
    assert baseline.row_group_size == 1800
    assert baseline.access == "shuffled"
    assert baseline.read_access_pattern == "shuffled"
    assert baseline.batch_size == 8
    assert baseline.prefetch_factor == 2
    assert baseline.split_by_node
    assert baseline.world_size == 8
    assert baseline.num_workers == 0
    assert baseline.file_shuffle is True
    assert baseline.shuffle_buffer_size == 0


def test_shuffle_access_patterns_published_correctly():
    """Verify read_access_pattern and shuffle_buffer_size semantics."""
    cases = _cases()
    shuffled = next(c for c in cases if c.sweep_axis == "baseline")
    buf10k = next(c for c in cases if c.sweep_axis == "shuffle")

    assert shuffled.read_access_pattern == "shuffled"
    assert shuffled.extra_columns()["shuffle_buffer_size"] == 0

    assert buf10k.read_access_pattern == "shuffled"
    assert buf10k.extra_columns()["shuffle_buffer_size"] == 10000
    assert "buf10000" in buf10k.name


def test_concurrency_axis_present():
    cases = _cases()
    concurrency_cases = [c for c in cases if c.sweep_axis == "concurrency"]
    assert {c.num_workers for c in concurrency_cases} == {4, 16}
    for c in concurrency_cases:
        assert f"-nw{c.num_workers}-" in c.name


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


def test_feature_and_cache_variants():
    cases = {
        c.name: c
        for c in RayDataReadConfigurator(CONFIG).generate_cases(include_disabled=True)
        if c.sweep_axis in ("features", "gcsfs_cache")
    }
    stem = "read-ray-ptpq-shuf-nw0-rg1800-fc64x10920-feat1134v10-splitws8div"
    assert set(cases) == {
        f"{stem}-proj286r1-reg",
        f"{stem}-proj286r16-reg",
        f"{stem}-proj286r286-reg",
        f"{stem}-reg",
        f"{stem}-proj286r1-ctreadahead-bs5m-reg",
        f"{stem}-proj286r1-ctreadahead-bs8m-reg",
    }
    projected = cases[f"{stem}-proj286r1-reg"]
    assert projected.read_columns()[:2] == ["tokens", "label"]
    assert len(projected.read_columns()) == 2 + 286
    assert projected.extra_columns()["read_projected_feature_column_count"] == 286
    assert projected.extra_columns()["read_projection_run_length_columns"] == 1
    assert projected.cache_type is None and projected.block_size is None
    contiguous = cases[f"{stem}-proj286r286-reg"]
    assert len(contiguous.read_columns()) == 2 + 286
    assert contiguous.extra_columns()["read_projection_run_length_columns"] == 286
    every = cases[f"{stem}-reg"]
    assert len(every.read_columns()) == 2 + 1134
    assert every.extra_columns()["read_projection_run_length_columns"] is None
    readahead = cases[f"{stem}-proj286r1-ctreadahead-bs5m-reg"]
    assert (readahead.cache_type, readahead.block_size) == ("readahead", 5242880)
    assert readahead.extra_columns()["gcsfs_read_cache_type"] == "readahead"
    assert readahead.extra_columns()["gcsfs_read_block_size_bytes"] == 5242880


def test_baseline_has_no_features_or_gcsfs_settings():
    baseline = next(c for c in _cases() if c.sweep_axis == "baseline")
    assert baseline.read_columns() == ["tokens", "label"]
    assert baseline.cache_type is None and baseline.block_size is None
    assert baseline.extra_columns()["parquet_feature_column_count"] is None


@pytest.mark.parametrize(
    "variant, match",
    [
        (
            "feature_column_count: 10, projected_feature_count: 11",
            "projected_feature_count",
        ),
        (
            "fmt: text_parquet, feature_column_count: 10",
            "only generated for pretok_parquet",
        ),
        (
            "feature_column_count: 10, projected_feature_count: 10",
            "use 0 to read every feature column",
        ),
        (
            "feature_column_count: 10, projection_run_length: 4",
            "projection_run_length needs",
        ),
        ("projection_run_length: 4", "projection_run_length needs"),
        (
            "feature_column_count: 10, projected_feature_count: 3, "
            "projection_run_length: 4",
            "between 1 and projected_feature_count",
        ),
    ],
)
def test_feature_validation(tmp_path, variant, match):
    yaml_text = _YAML_WITH_BASELINE_CLASH.replace("    rounds: 5\n", "").replace(
        "variants: []", f"variants:\n      - {{ axis: bad, {variant} }}"
    )
    (tmp_path / "configs.yaml").write_text(yaml_text)
    with pytest.raises(ValueError, match=match):
        RayDataReadConfigurator(str(tmp_path / "configs.py")).generate_cases()


def test_columns_in_schema():
    import json
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[7]
    schema_path = (
        root / "cloudbuild/subsystembenchmarks/subsystembenchmarks_schema.json"
    )
    with open(schema_path) as f:
        schema_fields = {f["name"] for f in json.load(f)["schema"]["fields"]}
    for c in RayDataReadConfigurator(CONFIG).generate_cases(include_disabled=True):
        for col in c.extra_columns():
            assert col in schema_fields, f"param column {col} missing from schema"
