from dataclasses import dataclass

import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading.configurator import (
    OneFactorReadConfigurator,
    ReadParameters,
)


@dataclass
class _FakeParams(ReadParameters):
    LOADER_TAG = "fk"


class _FakeConfigurator(OneFactorReadConfigurator):
    FRAMEWORK = "fake"
    PARAMS_CLASS = _FakeParams


_YAML = """
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
scenarios:
  - name: "read"
    scenario: "read"
    variants:
      - {axis: "workers", num_workers: 1}
      - {axis: "bucket_type", bucket_type: "hns"}   # run-level key -> must reject
"""


def _write(tmp_path, text):
    (tmp_path / "configs.yaml").write_text(text)
    return _FakeConfigurator(str(tmp_path / "configs.py"))


def test_baseline_and_variant_expand_with_stable_ids(tmp_path):
    text = _YAML.replace(
        '      - {axis: "bucket_type", bucket_type: "hns"}   # run-level key -> must reject\n',
        "",
    )
    cases = _write(tmp_path, text).generate_cases()
    assert [c.name for c in cases] == [
        "read-fk-ptpq-seq-nw8-rg1024-fc8x4096-reg",
        "read-fk-ptpq-seq-nw1-rg1024-fc8x4096-reg",
    ]
    assert all(c.framework == "fake" for c in cases)
    assert [c.sweep_axis for c in cases] == ["baseline", "workers"]


def test_variant_setting_run_level_key_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="run-level"):
        _write(tmp_path, _YAML).generate_cases()


def test_zero_rounds_is_rejected(tmp_path):
    text = _YAML.replace(
        '      - {axis: "bucket_type", bucket_type: "hns"}   # run-level key -> must reject\n',
        '      - {axis: "smoke", rounds: 0}\n',
    )
    with pytest.raises(ValueError, match="rounds must be >= 1"):
        _write(tmp_path, text).generate_cases()


def test_extra_columns_and_id_hooks_default_empty(tmp_path):
    text = _YAML.replace(
        '      - {axis: "bucket_type", bucket_type: "hns"}   # run-level key -> must reject\n',
        "",
    )
    cases = _write(tmp_path, text).generate_cases()
    assert cases[0].extra_columns() == {}
    assert cases[0]._id_extra_tokens() == []
