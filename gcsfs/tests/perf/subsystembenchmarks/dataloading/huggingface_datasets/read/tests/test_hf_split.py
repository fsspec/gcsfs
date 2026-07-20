import dataclasses

import pytest

pytest.importorskip("datasets")
pytest.importorskip("torch")
pytest.importorskip("pyarrow")

from gcsfs.tests.perf.subsystembenchmarks.dataloading import datagen  # noqa: E402
from gcsfs.tests.perf.subsystembenchmarks.dataloading.huggingface_datasets.parameters import (  # noqa: E402
    HFReadParameters,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.huggingface_datasets.read import (  # noqa: E402
    driver as _hf_read,
)


def _ingest(tmp_path, files, rows=20):
    prefix = f"file://{tmp_path}/data/"
    man = datagen.ingest_dataset(
        prefix,
        fmt="pretok_parquet",
        seq_len=8,
        file_count=files,
        rows_per_file=rows,
        row_group_size=10,
    )
    return prefix, man


def test_divisible_shards_are_disjoint(tmp_path):
    prefix, man = _ingest(tmp_path, files=8)  # 8 % 4 == 0
    per = [_hf_read.rank_rows(prefix, "pretok_parquet", r, 4) for r in range(4)]
    assert sum(per) == man["sample_count"]
    assert all(x == man["sample_count"] // 4 for x in per)


def test_indivisible_every_rank_reads_some(tmp_path):
    prefix, man = _ingest(tmp_path, files=7)  # 7 % 4 != 0
    per = [_hf_read.rank_rows(prefix, 'pretok_parquet', r, 4) for r in range(4)]
    assert sum(per) == man['sample_count']
    assert all(x > 0 for x in per)


def test_shuffle_collapses_num_shards_by_default(tmp_path):
    """Verify streaming shuffle collapses num_shards to 1 by default."""
    prefix, _ = _ingest(tmp_path, files=8)
    ds = _hf_read._build_dataset(prefix, "pretok_parquet", access="shuffled", seed=0)
    assert ds.num_shards == 1


def test_max_buffer_input_shards_one_preserves_num_shards(tmp_path):
    prefix, _ = _ingest(tmp_path, files=8)
    ds = _hf_read._build_dataset(
        prefix, "pretok_parquet", access="shuffled", seed=0, max_buffer_input_shards=1
    )
    assert ds.num_shards == 8


def _split_params(prefix, man, world_size=4):
    return HFReadParameters(
        name="split",
        bucket_name="",
        bucket_type="regional",
        rounds=1,
        scenario="read",
        framework="huggingface_datasets",
        fmt="pretok_parquet",
        seq_len=8,
        file_count=man["file_count"],
        rows_per_file=man["rows_per_file"],
        row_group_size=10,
        access="sequential",
        num_workers=0,
        split_by_node=True,
        world_size=world_size,
        batch_size=8,
    )


def test_run_epochs_reports_build_seconds(tmp_path):
    """Verify run_epochs returns dataset build seconds."""
    prefix, man = _ingest(tmp_path, files=4)
    durations, rows_list, ttfb, build_seconds = _hf_read.run_epochs(
        prefix=prefix,
        fmt="pretok_parquet",
        access="sequential",
        num_workers=0,
        batch_size=8,
        rounds=1,
    )
    assert rows_list[0] == man["sample_count"]
    assert isinstance(build_seconds, float)
    assert build_seconds > 0.0


def test_run_split_epochs_totals(tmp_path):
    prefix, man = _ingest(tmp_path, files=8)
    durations, rows_list, ttfb, build_seconds = _hf_read.run_split_epochs(
        prefix,
        _split_params(prefix, man),
    )
    assert rows_list[0] == man["sample_count"]
    assert 0.0 < durations[0] < 300.0
    assert ttfb >= 0.0
    assert build_seconds > 0.0


def test_run_split_epochs_totals_shuffled_with_mitigation(tmp_path):
    """Verify run_split_epochs with max_buffer_input_shards parameter."""
    prefix, man = _ingest(tmp_path, files=8)
    params = dataclasses.replace(
        _split_params(prefix, man),
        access="shuffled",
        max_buffer_input_shards=1,
    )
    durations, rows_list, ttfb, build_seconds = _hf_read.run_split_epochs(
        prefix, params
    )
    assert rows_list[0] == man["sample_count"]
    assert 0.0 < durations[0] < 300.0
    assert ttfb >= 0.0
    assert build_seconds > 0.0
