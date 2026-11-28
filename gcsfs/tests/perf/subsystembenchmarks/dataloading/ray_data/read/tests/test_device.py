"""Ray Data read driver wiring for CPU/GPU/TPU delivery and isolated TPU worker."""

import itertools
import json
import logging
import os
import sys
import types

import pytest

torch = pytest.importorskip("torch")
pytest.importorskip("ray")

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from gcsfs.tests.perf.subsystembenchmarks._common import (  # noqa: E402
    accelerator,
    launcher,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import (  # noqa: E402
    ReadResult,
    _write_worker_spec,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.parameters import (  # noqa: E402
    RayDataReadParameters,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.read import (  # noqa: E402
    driver,
)

FAKE_GPU = torch.device("meta")
_FEATURES = dict(seq_len=2, feature_column_count=6, projected_feature_count=3)


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
    monkeypatch.setattr(accelerator, "synchronize", calls.append)
    return calls


@pytest.fixture
def cpu_batches_from_ray(monkeypatch):
    """Ray itself stays on CPU: pinning pretok batches needs a real GPU."""
    real_kwargs = driver._torch_batch_kwargs

    def kwargs(params, shuffle_seed, collate_fn, device):
        return real_kwargs(params, shuffle_seed, collate_fn, None)

    monkeypatch.setattr(driver, "_torch_batch_kwargs", kwargs)


def test_uses_tpu_actors_only_on_tpu_without_features(monkeypatch):
    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    assert driver._uses_tpu_actors(_params()) is True
    assert driver._uses_tpu_actors(_params(**_FEATURES)) is False

    for kind in ("cpu", "cuda"):
        monkeypatch.setattr(accelerator, "device_kind", lambda k=kind: k)
        assert driver._uses_tpu_actors(_params()) is False
        assert driver._uses_tpu_actors(_params(**_FEATURES)) is False


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


def test_multi_rank_tasks_request_their_accelerator_resources(
    ray_cluster, tmp_path, monkeypatch
):
    requested = []
    real_consume = driver._consume_shard

    class _RecordingRemote:
        def options(self, **options):
            requested.append(options)
            # No accelerators in the test cluster: schedule on CPU after recording.
            return real_consume.options(num_gpus=0, resources={})

    monkeypatch.setattr(driver, "_consume_shard", _RecordingRemote())
    monkeypatch.setattr(
        driver.device_lib,
        "ray_task_resources",
        lambda ws, has_features: {} if has_features else {"num_gpus": 0.375},
    )
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


def test_feature_columns_skip_the_accelerator(
    ray_cluster, tmp_path, monkeypatch, sync_calls
):
    ranks, resource_calls = [], []
    monkeypatch.setattr(
        driver.device_lib, "rank_device", lambda rank: ranks.append(rank) or FAKE_GPU
    )
    real_resources = driver.device_lib.ray_task_resources

    def spy_resources(ws, has_features):
        res = real_resources(ws, has_features)
        resource_calls.append((ws, has_features, res))
        return res

    monkeypatch.setattr(driver.device_lib, "ray_task_resources", spy_resources)
    monkeypatch.setattr(accelerator, "device_kind", lambda: "cuda")
    monkeypatch.setattr(accelerator, "local_device_count", lambda: 8)
    params = _params(**_FEATURES)
    prefix = f"{tmp_path}/features/"
    params.ingest(prefix)
    arrow_fs, paths = driver.resolve_parquet_source(prefix)
    dataset = driver.build_dataset(arrow_fs, paths, params)

    _, rows, _ = driver.run_single_rank(dataset, params)
    (split,) = dataset.streaming_split(n=1, equal=False)
    _, _, shard_rows = driver._consume_shard._function(split, params, None, None)
    multi = _params(split_by_node=True, world_size=2, **_FEATURES)
    _, multi_rows, _ = driver.run_multi_rank(dataset, multi)

    assert rows == [20, 20] and shard_rows == 20 and multi_rows == [20, 20]
    assert ranks == [] and sync_calls == []
    assert resource_calls == [(2, True, {})]


class _FakeActor:
    def __init__(self, log, rank, world_size, source, opts, fail_consume=False):
        self.log = log
        self.rank = rank
        self.fail_consume = fail_consume
        log["inits"].append((rank, world_size, source, opts))

    class _Method:
        def __init__(self, fn):
            self._fn = fn

        def remote(self, *args, **kwargs):
            return self._fn(*args, **kwargs)

    @property
    def ready(self):
        return self._Method(lambda: self.log["ready"].append(self.rank) or True)

    @property
    def consume(self):
        def run(seed, collate):
            self.log["consume"].append((self.rank, seed))
            if self.fail_consume:
                raise RuntimeError("consume boom")
            return (10.0 + self.rank, 20.0 + self.rank, 5)

        return self._Method(run)


@pytest.fixture
def fake_tpu_actors(monkeypatch):
    """Simulated TPU host whose _ShardConsumer actors run in-process."""
    log = {
        "inits": [],
        "ready": [],
        "consume": [],
        "killed": [],
        "fail_consume": False,
    }

    class _ConsumerCls:
        def options(self, **opts):
            class _Bound:
                def remote(self, rank, world_size, params, source):
                    return _FakeActor(
                        log,
                        rank,
                        world_size,
                        source,
                        opts,
                        fail_consume=log["fail_consume"],
                    )

            return _Bound()

    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    monkeypatch.setattr(accelerator, "local_device_count", lambda: 8)
    monkeypatch.setattr(driver, "_ShardConsumer", _ConsumerCls())
    monkeypatch.setattr(driver, "timestamp", itertools.count(0.0).__next__)
    monkeypatch.setattr(
        driver.ray, "get", lambda refs: list(refs) if isinstance(refs, list) else refs
    )
    monkeypatch.setattr(
        driver.ray,
        "kill",
        lambda actor, no_restart=True: log["killed"].append((actor.rank, no_restart)),
    )
    return log


class _CountingDataset:
    """Dataset stand-in that counts streaming_split calls."""

    def __init__(self, dataset=None):
        self._dataset = dataset
        self.split_calls = 0

    def streaming_split(self, n, equal=False):
        self.split_calls += 1
        if self._dataset is None:
            return [f"split-{i}" for i in range(n)]
        return self._dataset.streaming_split(n, equal=equal)


def test_tpu_multi_rank_gives_each_actor_its_split_once(fake_tpu_actors):
    dataset = _CountingDataset()
    params = _params(split_by_node=True, world_size=4, rounds=2)

    durations, rows, _ = driver.run_multi_rank(dataset, params)

    assert dataset.split_calls == 1
    assert fake_tpu_actors["inits"] == [
        (r, 4, f"split-{r}", {"resources": {"TPU": 1.0}}) for r in range(4)
    ]
    assert fake_tpu_actors["ready"] == [0, 1, 2, 3]
    assert len(fake_tpu_actors["consume"]) == 8
    assert rows == [20, 20]
    assert len(durations) == 2 and all(d > 0 for d in durations)
    assert fake_tpu_actors["killed"] == [(r, True) for r in range(4)]


def test_tpu_multi_rank_kills_actors_on_consume_failure(fake_tpu_actors):
    fake_tpu_actors["fail_consume"] = True
    dataset = _CountingDataset()
    params = _params(split_by_node=True, world_size=2, rounds=2)

    with pytest.raises(RuntimeError, match="consume boom"):
        driver.run_multi_rank(dataset, params)

    assert fake_tpu_actors["killed"] == [(0, True), (1, True)]


def test_tpu_single_rank_gives_the_actor_its_dataset_once(fake_tpu_actors, monkeypatch):
    parent_binds = []
    monkeypatch.setattr(driver.device_lib, "rank_device", parent_binds.append)
    params = _params(split_by_node=False, world_size=1, rounds=2)

    durations, rows, _ = driver.run_single_rank("fake-ds", params)

    assert parent_binds == []
    assert fake_tpu_actors["inits"] == [(0, 1, "fake-ds", {"resources": {"TPU": 1.0}})]
    assert fake_tpu_actors["consume"] == [(0, None), (0, None)]
    assert rows == [5, 5]
    assert len(durations) == 2 and all(d > 0 for d in durations)
    assert fake_tpu_actors["killed"] == [(0, True)]


def test_shard_consumer_binds_tpu_rank_with_world_size(monkeypatch):
    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    bound = []
    monkeypatch.setattr(
        accelerator,
        "bind_rank",
        lambda local_rank, *, global_rank=None, world_size=None: bound.append(
            (local_rank, global_rank, world_size)
        )
        or FAKE_GPU,
    )
    consumer_cls = driver._ShardConsumer.__ray_metadata__.modified_class
    params = _params(split_by_node=True, world_size=4)

    consumer = consumer_cls(2, 4, params, "split-2")

    assert consumer._device == FAKE_GPU
    assert bound == [(2, 2, 4)]


def _write_labeled_shards(root, count):
    for i in range(count):
        table = pa.table({"tokens": [[1, 2, 3]] * 5, "label": [i] * 5})
        pq.write_table(table, root / f"shard_{i:05d}.parquet")
    return str(root)


def _shard_order(labels):
    order = []
    for label in labels:
        if not order or order[-1] != label:
            order.append(label)
    return order


@pytest.fixture
def preserve_order():
    ctx = driver.ray.data.DataContext.get_current()
    previous = ctx.execution_options.preserve_order
    ctx.execution_options.preserve_order = True
    yield
    ctx.execution_options.preserve_order = previous


class _RecordingSource:
    """Dataset or split wrapper that logs the shard labels each pass reads."""

    def __init__(self, source, log_dir, tag="rank0"):
        self._source, self._log_dir, self._tag = source, log_dir, tag
        self.split_calls = 0

    def streaming_split(self, n, equal=False):
        self.split_calls += 1
        splits = self._source.streaming_split(n, equal=equal)
        return [
            _RecordingSource(split, self._log_dir, f"rank{i}")
            for i, split in enumerate(splits)
        ]

    def iter_torch_batches(self, **kwargs):
        import time

        labels = []
        for batch in self._source.iter_torch_batches(**kwargs):
            labels.extend(int(v) for v in batch["label"])
            yield batch
        path = os.path.join(self._log_dir, f"{time.time_ns()}-{self._tag}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(labels, f)


def _round_orders(log_dir, passes_per_round):
    paths = sorted(log_dir.iterdir())
    rounds = []
    for i in range(0, len(paths), passes_per_round):
        group = []
        for path in paths[i : i + passes_per_round]:
            tag = path.stem.split("-", 1)[1]
            group.append((tag, tuple(_shard_order(json.loads(path.read_text())))))
        rounds.append(tuple(sorted(group)))
    return rounds


@pytest.mark.parametrize("path", ["cpu-multi", "tpu-single", "tpu-multi"])
def test_shuffled_rounds_read_files_in_a_new_order(
    ray_cluster, tmp_path, monkeypatch, preserve_order, request, path
):
    """Real Ray tasks/actors: every round reshuffles files, with exact rows."""
    from ray import cloudpickle

    # Ray workers cannot import this test module, so ship _RecordingSource by value.
    module = sys.modules[__name__]
    cloudpickle.register_pickle_by_value(module)
    request.addfinalizer(lambda: cloudpickle.unregister_pickle_by_value(module))
    multi = path.endswith("multi")
    params = _params(
        access="shuffled",
        file_shuffle=True,
        rounds=3,
        file_count=6,
        split_by_node=multi,
        world_size=2 if multi else 1,
    )
    if path.startswith("tpu"):
        # The TPU actor path, scheduled on this CPU-only test cluster.
        monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
        monkeypatch.setattr(
            driver.device_lib, "ray_task_resources", lambda ws, has_features: {}
        )
    shards = tmp_path / "shards"
    shards.mkdir()
    log_dir = tmp_path / "log"
    log_dir.mkdir()
    arrow_fs, paths = driver.resolve_parquet_source(_write_labeled_shards(shards, 6))
    source = _RecordingSource(driver.build_dataset(arrow_fs, paths, params), log_dir)

    run = driver.run_multi_rank if multi else driver.run_single_rank
    _, rows, _ = run(source, params)

    assert rows == [30, 30, 30]
    assert source.split_calls == (1 if multi else 0)
    rounds = _round_orders(log_dir, passes_per_round=2 if multi else 1)
    assert len(rounds) == 3
    for round_orders in rounds:
        assert sorted(s for _, order in round_orders for s in order) == list(range(6))
    assert len(set(rounds)) == 3, rounds


@pytest.mark.parametrize(
    "overrides",
    [
        dict(split_by_node=True, world_size=4),
        dict(split_by_node=False, world_size=1),
    ],
    ids=["tpu-multi", "tpu-single"],
)
def test_run_read_on_tpu_delegates_to_isolated_subprocess(monkeypatch, overrides):
    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    ensured = []
    launched = []

    monkeypatch.setattr(driver, "ensure_ray_initialized", lambda: ensured.append(True))

    def fake_run_subprocess(module, args, *, env=None):
        spec_file = args[args.index("--spec-file") + 1]
        result_file = args[args.index("--result-file") + 1]
        with open(spec_file, encoding="utf-8") as f:
            spec = json.load(f)
        launched.append((module, spec, env))
        with open(result_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "durations": [1.5, 1.25],
                    "rows": [20, 20],
                    "ttfb": 0.2,
                    "build_seconds": 0.4,
                },
                f,
            )

    monkeypatch.setattr(launcher, "run_subprocess", fake_run_subprocess)

    result = driver.RayDataReadDriver().run_read(
        "gs://b/p/", _params(**overrides), manifest=None
    )

    assert ensured == []
    assert len(launched) == 1
    module, spec, env = launched[0]
    assert (
        module
        == "gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.read._worker"
    )
    assert spec["prefix"] == "gs://b/p/"
    assert spec["params"]["world_size"] == overrides["world_size"]
    assert env is None
    assert result == ReadResult([1.5, 1.25], [20, 20], 0.2, 0.4)


def test_run_read_on_tpu_with_features_stays_in_process(monkeypatch):
    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    ensured, launched = [], []
    monkeypatch.setattr(driver, "ensure_ray_initialized", lambda: ensured.append(True))
    monkeypatch.setattr(
        launcher, "run_subprocess", lambda *a, **kw: launched.append((a, kw))
    )
    monkeypatch.setattr(
        driver,
        "_run_read_in_process",
        lambda prefix, params: ReadResult([1.0], [20], 0.1, 0.2),
    )

    result = driver.RayDataReadDriver().run_read(
        "gs://b/p/", _params(split_by_node=True, world_size=4, **_FEATURES), None
    )

    assert ensured == [True]
    assert launched == []
    assert result.rows_per_epoch == [20]


def test_worker_multi_rank_tpu_sets_launch_env_before_ray_init_and_shuts_down(
    tmp_path, monkeypatch
):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.read import _worker

    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    monkeypatch.setattr(accelerator, "local_device_count", lambda: 8)
    monkeypatch.setenv("TORCH_TPU_TOPOLOGY", "stale-topo")
    monkeypatch.setenv("TORCH_TPU_SLICEBUILDER_ADDRESSES", "stale-addr")
    monkeypatch.setenv("TORCH_TPU_DEVICES_PER_HOST", "8")
    monkeypatch.delenv("RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS", raising=False)

    events = []

    def fake_set_tpu_launch_env(*, nproc_per_node):
        events.append(
            (
                "set_tpu_launch_env",
                nproc_per_node,
                os.environ.get("TORCH_TPU_TOPOLOGY"),
                os.environ.get("TORCH_TPU_SLICEBUILDER_ADDRESSES"),
                os.environ.get("TORCH_TPU_DEVICES_PER_HOST"),
                os.environ.get("RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS"),
            )
        )

    fake_tpu_dist = types.ModuleType("torch.tpu.distributed")
    fake_tpu_dist.set_tpu_launch_env = fake_set_tpu_launch_env
    fake_tpu = types.ModuleType("torch.tpu")
    fake_tpu.distributed = fake_tpu_dist
    monkeypatch.setitem(sys.modules, "torch.tpu", fake_tpu)
    monkeypatch.setitem(sys.modules, "torch.tpu.distributed", fake_tpu_dist)
    monkeypatch.setattr(torch, "tpu", fake_tpu, raising=False)

    initialized = {"state": False}

    def fake_ray_init(**kwargs):
        initialized["state"] = True
        events.append(
            (
                "ray_init",
                kwargs,
                os.environ.get("RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS"),
            )
        )

    def fake_ray_shutdown():
        initialized["state"] = False
        events.append(("ray_shutdown",))

    class _FakeDataContext:
        enable_progress_bars = True
        target_max_block_size = 0

    monkeypatch.setattr(driver.ray, "is_initialized", lambda: initialized["state"])
    monkeypatch.setattr(driver.ray, "init", fake_ray_init)
    monkeypatch.setattr(driver.ray, "shutdown", fake_ray_shutdown)
    monkeypatch.setattr(
        driver.ray.data.DataContext, "get_current", staticmethod(_FakeDataContext)
    )

    def fake_run_in_process(prefix, params):
        assert isinstance(params, RayDataReadParameters)
        assert params.has_features is False
        events.append(("run", prefix, params.world_size))
        return ReadResult([1.1, 1.2], [20, 20], 0.05, 0.3)

    monkeypatch.setattr(driver, "_run_read_in_process", fake_run_in_process)

    spec_file = tmp_path / "spec.json"
    result_file = tmp_path / "result.json"
    _write_worker_spec(
        str(spec_file), "gs://b/p/", _params(split_by_node=True, world_size=4), None
    )

    _worker.main(["--spec-file", str(spec_file), "--result-file", str(result_file)])

    assert events == [
        ("set_tpu_launch_env", 4, None, None, None, "1"),
        (
            "ray_init",
            {
                "address": "local",
                "include_dashboard": False,
                "ignore_reinit_error": True,
                "logging_level": logging.WARNING,
            },
            "1",
        ),
        ("run", "gs://b/p/", 4),
        ("ray_shutdown",),
    ]
    assert json.loads(result_file.read_text()) == {
        "durations": [1.1, 1.2],
        "rows": [20, 20],
        "ttfb": 0.05,
        "build_seconds": 0.3,
    }


def test_worker_single_rank_tpu_skips_set_tpu_launch_env(tmp_path, monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.read import _worker

    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    monkeypatch.setattr(accelerator, "local_device_count", lambda: 8)
    monkeypatch.setenv("TORCH_TPU_TOPOLOGY", "stale-topo")
    monkeypatch.setenv("TORCH_TPU_DEVICES_PER_HOST", "8")

    tpu_env_calls = []
    fake_tpu_dist = types.ModuleType("torch.tpu.distributed")
    fake_tpu_dist.set_tpu_launch_env = lambda **kw: tpu_env_calls.append(kw)
    fake_tpu = types.ModuleType("torch.tpu")
    fake_tpu.distributed = fake_tpu_dist
    monkeypatch.setitem(sys.modules, "torch.tpu", fake_tpu)
    monkeypatch.setitem(sys.modules, "torch.tpu.distributed", fake_tpu_dist)
    monkeypatch.setattr(torch, "tpu", fake_tpu, raising=False)

    events = []
    monkeypatch.setattr(driver, "ensure_ray_initialized", lambda: events.append("init"))
    monkeypatch.setattr(driver, "shutdown_ray", lambda: events.append("shutdown"))
    monkeypatch.setattr(
        driver,
        "_run_read_in_process",
        lambda prefix, params: events.append("run")
        or ReadResult([0.5], [20], 0.1, 0.2),
    )

    spec_file = tmp_path / "spec.json"
    result_file = tmp_path / "result.json"
    _write_worker_spec(
        str(spec_file), "gs://b/p/", _params(split_by_node=False, world_size=1), None
    )

    _worker.main(["--spec-file", str(spec_file), "--result-file", str(result_file)])

    assert tpu_env_calls == []
    assert "TORCH_TPU_TOPOLOGY" not in os.environ
    assert "TORCH_TPU_DEVICES_PER_HOST" not in os.environ
    assert events == ["init", "run", "shutdown"]
    assert json.loads(result_file.read_text())["rows"] == [20]


def test_worker_tpu_rejects_world_size_above_host_tpus(tmp_path, monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.read import _worker

    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    monkeypatch.setattr(accelerator, "local_device_count", lambda: 4)
    inits = []
    monkeypatch.setattr(driver, "ensure_ray_initialized", lambda: inits.append(True))

    spec_file = tmp_path / "spec.json"
    result_file = tmp_path / "result.json"
    _write_worker_spec(
        str(spec_file), "gs://b/p/", _params(split_by_node=True, world_size=8), None
    )

    with pytest.raises(ValueError, match="exceeds 4"):
        _worker.main(["--spec-file", str(spec_file), "--result-file", str(result_file)])

    assert inits == []


def test_worker_shuts_down_ray_on_run_error(tmp_path, monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.read import _worker

    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    monkeypatch.setattr(accelerator, "local_device_count", lambda: 8)
    shutdowns = []
    monkeypatch.setattr(driver, "ensure_ray_initialized", lambda: None)
    monkeypatch.setattr(driver, "shutdown_ray", lambda: shutdowns.append(True))

    def boom(prefix, params):
        raise RuntimeError("worker boom")

    monkeypatch.setattr(driver, "_run_read_in_process", boom)

    spec_file = tmp_path / "spec.json"
    result_file = tmp_path / "result.json"
    _write_worker_spec(
        str(spec_file), "gs://b/p/", _params(split_by_node=False, world_size=1), None
    )

    with pytest.raises(RuntimeError, match="worker boom"):
        _worker.main(["--spec-file", str(spec_file), "--result-file", str(result_file)])

    assert shutdowns == [True]
    assert not result_file.exists()
