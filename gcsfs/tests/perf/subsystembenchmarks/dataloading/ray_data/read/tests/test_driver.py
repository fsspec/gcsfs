import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.parameters import (
    RayDataReadParameters,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.read import driver


@pytest.fixture(scope="module")
def ray_cluster():
    driver.ensure_ray_initialized()
    yield
    import ray

    if ray.is_initialized():
        ray.shutdown()


@pytest.fixture
def parquet_dataset(tmp_path):
    """Creates 4 Parquet shards with 25 rows each (100 rows total)."""
    schema = pa.schema(
        [
            ("tokens", pa.list_(pa.int64())),
            ("label", pa.int64()),
        ]
    )
    for i in range(4):
        table = pa.table(
            {
                "tokens": [[1, 2, 3, 4] for _ in range(25)],
                "label": [i % 2 for _ in range(25)],
            },
            schema=schema,
        )
        pq.write_table(table, tmp_path / f"shard_{i:05d}.parquet")
    return str(tmp_path)


def test_resolve_parquet_source(parquet_dataset):
    arrow_fs, paths = driver.resolve_parquet_source(parquet_dataset)
    assert len(paths) == 4
    assert all(p.endswith(".parquet") for p in paths)


def test_resolve_parquet_source_raises_on_empty(tmp_path):
    with pytest.raises(FileNotFoundError, match="no Parquet files"):
        driver.resolve_parquet_source(str(tmp_path))


def test_driver_single_rank_reads_exact_sample_count(ray_cluster, parquet_dataset):
    params = RayDataReadParameters(
        name="test-single",
        bucket_name="test-bucket",
        bucket_type="regional",
        rounds=2,
        scenario="read",
        framework="ray_data",
        fmt="pretok_parquet",
        file_count=4,
        rows_per_file=25,
        access="sequential",
        num_workers=0,
        batch_size=10,
        prefetch_factor=2,
        split_by_node=False,
        world_size=1,
    )
    d = driver.RayDataReadDriver()
    result = d.run_read(parquet_dataset, params, manifest=None)

    assert len(result.durations) == 2
    assert result.rows_per_epoch == [100, 100]
    assert result.ttfb_seconds > 0
    assert result.build_seconds >= 0


def test_driver_multi_rank_splits_equally_and_preserves_counts(
    ray_cluster, parquet_dataset
):
    params = RayDataReadParameters(
        name="test-multi",
        bucket_name="test-bucket",
        bucket_type="regional",
        rounds=2,
        scenario="read",
        framework="ray_data",
        fmt="pretok_parquet",
        file_count=4,
        rows_per_file=25,
        access="shuffled",
        file_shuffle=True,
        shuffle_buffer_size=50,
        num_workers=0,
        batch_size=5,
        prefetch_factor=2,
        split_by_node=True,
        world_size=2,
    )
    d = driver.RayDataReadDriver()
    result = d.run_read(parquet_dataset, params, manifest=None)

    assert len(result.durations) == 2
    assert result.rows_per_epoch == [100, 100]
    assert result.ttfb_seconds > 0
    assert result.build_seconds >= 0


def test_driver_reads_text_parquet_single_and_multi_rank(ray_cluster, tmp_path):
    schema = pa.schema(
        [
            ("text", pa.string()),
            ("label", pa.int64()),
        ]
    )
    for i in range(2):
        table = pa.table(
            {
                "text": [f"text_sample_{j}" for j in range(20)],
                "label": [j % 2 for j in range(20)],
            },
            schema=schema,
        )
        pq.write_table(table, tmp_path / f"shard_{i:05d}.parquet")

    params = RayDataReadParameters(
        name="test-text-multi",
        bucket_name="test-bucket",
        bucket_type="regional",
        rounds=2,
        scenario="read",
        framework="ray_data",
        fmt="text_parquet",
        file_count=2,
        rows_per_file=20,
        access="shuffled",
        file_shuffle=True,
        shuffle_buffer_size=10,
        num_workers=0,
        batch_size=4,
        prefetch_factor=2,
        split_by_node=True,
        world_size=2,
    )
    d = driver.RayDataReadDriver()
    result = d.run_read(str(tmp_path), params, manifest=None)

    assert len(result.durations) == 2
    assert result.rows_per_epoch == [40, 40]
    assert result.ttfb_seconds > 0
    assert result.build_seconds >= 0
