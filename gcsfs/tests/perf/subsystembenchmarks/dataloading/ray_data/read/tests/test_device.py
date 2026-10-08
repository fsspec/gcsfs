"""Ray Data read driver wiring for CPU/GPU delivery."""

import pytest

torch = pytest.importorskip("torch")
pytest.importorskip("ray")

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.parameters import (  # noqa: E402
    RayDataReadParameters,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.read import (  # noqa: E402
    driver,
)

FAKE_GPU = torch.device("meta")


def _params(**overrides):
    values = dict(
        name="device",
        bucket_name="",
        bucket_type="regional",
        rounds=2,
        scenario="read",
        framework="ray_data",
        fmt="pretok_parquet",
        file_count=4,
        rows_per_file=5,
        access="sequential",
        num_workers=0,
        batch_size=4,
        prefetch_factor=2,
        split_by_node=False,
        world_size=1,
    )
    values.update(overrides)
    return RayDataReadParameters(**values)


@pytest.fixture(scope="module")
def ray_cluster():
    driver.ensure_ray_initialized()
    yield
    driver.shutdown_ray()


def _write_shards(root, fmt):
    for i in range(4):
        if fmt == "pretok_parquet":
            table = pa.table({"tokens": [[1, 2, 3]] * 5, "label": [i] * 5})
        else:
            table = pa.table({"text": [f"doc {i}"] * 5, "label": [i] * 5})
        pq.write_table(table, root / f"shard_{i:05d}.parquet")
    return str(root)


@pytest.fixture
def sync_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(torch.cuda, "synchronize", calls.append)
    return calls


@pytest.fixture
def cpu_batches_from_ray(monkeypatch):
    """Ray itself stays on CPU: pinning pretok batches needs a real GPU."""
    real_kwargs = driver._torch_batch_kwargs

    def kwargs(params, shuffle_seed, collate_fn, device):
        return real_kwargs(params, shuffle_seed, collate_fn, None)

    monkeypatch.setattr(driver, "_torch_batch_kwargs", kwargs)


def test_torch_batch_kwargs_on_cpu_match_todays_arguments():
    kwargs = driver._torch_batch_kwargs(_params(), None, None, None)
    assert kwargs["device"] == torch.device("cpu")
    assert kwargs["pin_memory"] is False


def test_torch_batch_kwargs_on_gpu_pin_pretok_batches():
    kwargs = driver._torch_batch_kwargs(_params(), None, None, FAKE_GPU)
    assert kwargs["device"] == FAKE_GPU
    assert kwargs["pin_memory"] is True


def test_torch_batch_kwargs_on_gpu_skip_pinning_with_custom_collate():
    collate = driver._TextParquetCollateFn()
    kwargs = driver._torch_batch_kwargs(
        _params(fmt="text_parquet"), None, collate, FAKE_GPU
    )
    assert kwargs["device"] == FAKE_GPU
    assert kwargs["pin_memory"] is False
    assert kwargs["collate_fn"] is collate


def test_text_parquet_labels_reach_device_and_strings_survive(
    ray_cluster, tmp_path, sync_calls
):
    # Unmocked: Ray gets device=FAKE_GPU but no pinning, and leaves this
    # non-tensor batch on the host for feed to move.
    params = _params(fmt="text_parquet")
    arrow_fs, paths = driver.resolve_parquet_source(
        _write_shards(tmp_path, "text_parquet")
    )
    dataset = driver.build_dataset(arrow_fs, paths, params)

    batches = list(
        driver._iter_batches(
            dataset, params, None, driver._TextParquetCollateFn(), FAKE_GPU
        )
    )

    assert sum(len(b["text"]) for b in batches) == 20
    assert all(isinstance(t, str) for b in batches for t in b["text"])
    assert all(b["label"].device == FAKE_GPU for b in batches)
    assert sync_calls == [FAKE_GPU]


def test_single_rank_binds_rank0_and_synchronizes_each_round(
    ray_cluster, tmp_path, monkeypatch, sync_calls, cpu_batches_from_ray
):
    ranks = []
    monkeypatch.setattr(
        driver.device_lib, "rank_device", lambda rank: ranks.append(rank) or FAKE_GPU
    )
    params = _params()
    arrow_fs, paths = driver.resolve_parquet_source(
        _write_shards(tmp_path, "pretok_parquet")
    )

    _, rows, _ = driver.run_single_rank(
        driver.build_dataset(arrow_fs, paths, params), params
    )

    assert rows == [20, 20]
    assert ranks == [0]
    assert sync_calls == [FAKE_GPU, FAKE_GPU]


def test_consume_shard_binds_its_gpu_and_synchronizes(
    ray_cluster, tmp_path, monkeypatch, sync_calls, cpu_batches_from_ray
):
    """The task body, run locally: Ray's num_gpus scoping makes rank 0 its GPU."""
    ranks = []
    monkeypatch.setattr(
        driver.device_lib, "rank_device", lambda rank: ranks.append(rank) or FAKE_GPU
    )
    params = _params()
    arrow_fs, paths = driver.resolve_parquet_source(
        _write_shards(tmp_path, "pretok_parquet")
    )
    (split,) = driver.build_dataset(arrow_fs, paths, params).streaming_split(
        n=1, equal=False
    )

    _, _, rows = driver._consume_shard._function(split, params, None, None)

    assert rows == 20
    assert ranks == [0]
    assert sync_calls == [FAKE_GPU]


def test_multi_rank_tasks_request_their_gpu_share(ray_cluster, tmp_path, monkeypatch):
    requested = []
    real_consume = driver._consume_shard

    class _RecordingRemote:
        def options(self, **options):
            requested.append(options)
            # No GPUs in the test cluster: schedule on CPU after recording.
            return real_consume.options(**{**options, "num_gpus": 0})

    monkeypatch.setattr(driver, "_consume_shard", _RecordingRemote())
    monkeypatch.setattr(driver.device_lib, "ray_task_num_gpus", lambda ws: 0.375)
    params = _params(split_by_node=True, world_size=2)
    arrow_fs, paths = driver.resolve_parquet_source(
        _write_shards(tmp_path, "pretok_parquet")
    )

    _, rows, _ = driver.run_multi_rank(
        driver.build_dataset(arrow_fs, paths, params), params
    )

    assert rows == [20, 20]
    # One task per rank per round.
    assert requested == [{"num_gpus": 0.375}] * (params.world_size * params.rounds)
