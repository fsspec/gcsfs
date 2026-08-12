import os

import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset import (
    configs,
    imagegen,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset.read import driver

_TINY_PIXEL_BUDGET = imagegen.FACTOR * imagegen.FACTOR * 4


def _case(axis=None, predicate=None):
    cases = configs.WebDatasetReadConfigurator(configs.__file__).generate_cases()
    if predicate:
        return next(c for c in cases if predicate(c))
    return next(c for c in cases if c.sweep_axis == (axis or "baseline"))


def test_shard_urls_are_explicit_ordered_and_carry_the_format_extension():
    urls = driver.shard_urls("gs://bucket/data/", _case())
    assert len(urls) == 64
    assert urls[0] == "gs://bucket/data/shard_00000.tar"
    assert urls[-1] == "gs://bucket/data/shard_00063.tar"

    gz = _case(predicate=lambda c: c.fmt == "image_tar_gz")
    assert driver.shard_urls("gs://b/d/", gz)[0].endswith("shard_00000.tar.gz")


def test_collect_sample_picks_every_image_member_and_ignores_metadata():
    """Verifies image extraction across single-image and interleaved samples."""
    pair = {
        "__key__": "k",
        "__url__": "gs://x",
        "jpg": b"i",
        "txt": "cap",
        "json": b"{}",
    }
    assert driver.collect_sample(pair) == {"images": [b"i"], "text": "cap"}

    interleaved = {
        "__key__": "k",
        "image_0.jpg": b"a",
        "image_1.png": b"b",
        "image_2.npy": b"c",
        "txt": "caption",
        "json": b"{}",
    }
    assert driver.collect_sample(interleaved)["images"] == [b"a", b"b", b"c"]


def test_rank_targets_sum_to_expected():
    assert driver.rank_targets(50176, 8) == [6272] * 8
    assert sum(driver.rank_targets(50160, 8)) == 50160
    assert driver.rank_targets(10, 4) == [3, 3, 2, 2]


# Probe WebDataset worker info inside spawned child process.
def _rank_world_probe(rank, world_size, queue):
    import webdataset as wds

    driver._set_distributed_env(rank, world_size)
    resolved_rank, resolved_world_size, _worker, _num_workers = (
        wds.utils.pytorch_worker_info()
    )
    queue.put((rank, resolved_rank, resolved_world_size))


def test_spawned_ranks_resolve_distinct_rank_and_world_size_in_webdataset():
    import torch.multiprocessing as mp

    world_size = 3
    ctx = mp.get_context("spawn")
    with ctx.Manager() as manager:
        queue = manager.Queue()
        mp.spawn(
            _rank_world_probe,
            args=(world_size, queue),
            nprocs=world_size,
            join=True,
        )
        results = sorted(queue.get() for _ in range(world_size))

    assert results == [(0, 0, 3), (1, 1, 3), (2, 2, 3)]


def _keep_source_url(sample):
    """Extracts source shard URL for shard order verification."""
    return {"url": sample["__url__"]}


def _observed_shard_order(dataset):
    """Returns shard sequence observed in one full epoch pass."""
    order = []
    for sample in dataset:
        shard = int(sample["url"].rsplit("shard_", 1)[1].split(".", 1)[0])
        if not order or order[-1] != shard:
            order.append(shard)
    return order


def test_build_dataset_reshuffles_shard_order_each_round_reproducibly(
    tmp_path, monkeypatch
):
    """Shard order changes by epoch but is reproducible after reconstruction."""
    file_count = 6
    prefix = str(tmp_path) + "/data/"
    imagegen.ingest_tar_shards(
        prefix,
        fmt="image_tar",
        file_count=file_count,
        rows_per_file=2,
        pixel_budget=_TINY_PIXEL_BUDGET,
        image_encoding="jpeg",
        jpeg_quality=75,
        sample_shape="pairs",
    )
    monkeypatch.setattr(driver, "collect_sample", _keep_source_url)

    params = _case()  # Baseline: access="shuffled" -> shard_shuffle_enabled
    params.file_count = file_count
    params.rows_per_file = 2
    params.pixel_budget = _TINY_PIXEL_BUDGET
    params.decode = False
    params.shuffle_buffer_size = 1  # Disables sample-level reordering.
    params.cache_dir_enabled = False

    dataset = driver.build_dataset(prefix, params, split_by_node=False)
    round1 = _observed_shard_order(dataset)
    round2 = _observed_shard_order(dataset)  # Next epoch on same dataset.

    assert sorted(round1) == sorted(round2) == list(range(file_count))
    assert round1 != round2, "round 2 must reorder shards, not repeat round 1's order"

    rebuilt = driver.build_dataset(prefix, params, split_by_node=False)
    round3 = _observed_shard_order(rebuilt)
    assert (
        round3 == round1
    ), "a rebuild with the same inputs must reproduce round 1's order"


# FileCache keys by shard basename; verify isolation across case roots.
def test_case_cache_roots_do_not_collide_across_cases():
    params = _case(axis="cache")
    assert params.cache_dir_enabled

    with driver.case_cache_root(params) as first:
        with driver.case_cache_root(params) as second:
            assert first != second
            assert os.path.join(first, "shard_00000.tar") != os.path.join(
                second, "shard_00000.tar"
            )


def test_case_cache_root_is_removed_even_when_the_case_raises():
    params = _case(axis="cache")
    with pytest.raises(RuntimeError, match="boom"):
        with driver.case_cache_root(params) as root:
            open(os.path.join(root, "shard_00000.tar"), "wb").close()
            raise RuntimeError("boom")
    assert not os.path.exists(root), "a failed case must not leak its cached corpus"


def test_case_cache_root_is_absent_when_caching_is_disabled():
    with driver.case_cache_root(_case()) as root:  # Baseline without cache
        assert root is None
    assert driver.rank_cache_dir(None, 0) is None


def test_rank_cache_dirs_are_isolated_inside_one_case_root():
    params = _case(axis="cache")
    with driver.case_cache_root(params) as root:
        rank0 = driver.rank_cache_dir(root, 0)
        rank1 = driver.rank_cache_dir(root, 1)
        assert rank0 != rank1
        assert os.path.isdir(rank0) and os.path.isdir(rank1)
        assert rank0.startswith(root) and rank1.startswith(root)


# Test multi-rank execution with resampled shard streams.
_RESAMPLED_FILE_COUNT = 3
_RESAMPLED_ROWS_PER_FILE = 4
_RESAMPLED_TARGET_TOTAL = _RESAMPLED_FILE_COUNT * _RESAMPLED_ROWS_PER_FILE


def _resampled_rank_probe(rank, world_size, prefix, queue):
    driver._set_distributed_env(rank, world_size)
    params = _case()
    params.file_count = _RESAMPLED_FILE_COUNT
    params.rows_per_file = _RESAMPLED_ROWS_PER_FILE
    params.pixel_budget = _TINY_PIXEL_BUDGET
    params.resampled = True
    params.split_by_node = True
    params.world_size = world_size
    params.num_workers = 0
    params.decode = False
    params.rounds = 1
    try:
        per_epoch, _ttfb, _build_seconds = driver.run_rank_epochs(
            rank, world_size, prefix, params, sample_count=_RESAMPLED_TARGET_TOTAL
        )
    except Exception as exc:  # Catch rank startup errors.
        queue.put(("error", f"{type(exc).__name__}: {exc}"))
    else:
        queue.put(("ok", per_epoch[0][2]))


def test_resampled_multi_rank_reads_without_raising_before_a_byte(tmp_path):
    import torch.multiprocessing as mp

    prefix = str(tmp_path) + "/data/"
    imagegen.ingest_tar_shards(
        prefix,
        fmt="image_tar",
        file_count=_RESAMPLED_FILE_COUNT,
        rows_per_file=_RESAMPLED_ROWS_PER_FILE,
        pixel_budget=_TINY_PIXEL_BUDGET,
        image_encoding="jpeg",
        jpeg_quality=75,
        sample_shape="pairs",
    )

    world_size = 2
    ctx = mp.get_context("spawn")
    with ctx.Manager() as manager:
        queue = manager.Queue()
        mp.spawn(
            _resampled_rank_probe,
            args=(world_size, prefix, queue),
            nprocs=world_size,
            join=True,
        )
        results = [queue.get() for _ in range(world_size)]

    for status, payload in results:
        assert status == "ok", f"a resampled rank raised instead of reading: {payload}"
    assert sorted(payload for _, payload in results) == [6, 6]


def test_resampled_rank_refuses_to_run_without_a_sample_budget():
    """Resampled streams are infinite and require an explicit sample budget."""
    params = _case()
    params.resampled = True
    with pytest.raises(ValueError, match="sample_count"):
        driver.run_rank_epochs(0, 1, "gs://unused/data/", params)


def test_resampled_dataset_builds_without_a_webdataset_warning(tmp_path):
    """WebDataset warns if shardshuffle is configured alongside resampled shards."""
    import warnings

    prefix = str(tmp_path) + "/data/"
    imagegen.ingest_tar_shards(
        prefix,
        fmt="image_tar",
        file_count=2,
        rows_per_file=2,
        pixel_budget=_TINY_PIXEL_BUDGET,
        image_encoding="jpeg",
        jpeg_quality=75,
        sample_shape="pairs",
    )
    params = _case()  # Baseline: access="shuffled", shard_shuffle=True
    params.file_count = 2
    params.rows_per_file = 2
    params.resampled = True

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        driver.build_dataset(prefix, params, split_by_node=False)

    assert [str(w.message) for w in caught] == []


# Variable image shapes require unstacked list batches.
def test_loader_yields_list_batches_for_variable_shape_samples(tmp_path):
    file_count, rows_per_file = 3, 4
    prefix = str(tmp_path) + "/data/"
    imagegen.ingest_tar_shards(
        prefix,
        fmt="image_tar",
        file_count=file_count,
        rows_per_file=rows_per_file,
        pixel_budget=_TINY_PIXEL_BUDGET,
        image_encoding="jpeg",
        jpeg_quality=75,
        sample_shape="pairs",
    )

    params = _case()
    params.file_count = file_count
    params.rows_per_file = rows_per_file
    params.pixel_budget = _TINY_PIXEL_BUDGET
    params.access = "sequential"  # Deterministic batch ordering.
    params.batch_size = 5
    params.num_workers = 0

    dataset = driver.build_dataset(prefix, params, split_by_node=False)
    loader = driver.build_loader(dataset, params)
    batches = list(loader)
    assert all(isinstance(b, list) for b in batches)
    assert sorted(len(b) for b in batches) == [2, 5, 5]
    assert sum(len(batch) for batch in batches) == 12
