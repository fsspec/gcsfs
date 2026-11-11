import logging
import multiprocessing
import os

import fsspec
import pytest

import gcsfs.core
from gcsfs.extended_gcsfs import ExtendedGcsFileSystem
from gcsfs.retry import HttpError
from gcsfs.tests import layouts
from gcsfs.tests.perf.subsystembenchmarks.dataloading import rcu


@pytest.mark.parametrize(
    "bucket_type, state",
    [
        ("standard_rcu_z", "z"),
        ("standard_rcu_warm", "warm"),
        ("standard_rcu_cold", "cold"),
        ("hns_rcu_z", "z"),
        ("hns_rcu_warm", "warm"),
        ("hns_rcu_cold", "cold"),
        ("regional", None),
        ("zonal", None),
        ("rapid_cache_warm", None),
        ("rapid_cache_cold", None),
    ],
)
def test_rcu_state(bucket_type, state):
    assert rcu.rcu_state(bucket_type) == state


class _ApiFS:
    """Records fs.call requests and raises ``error`` from each call if set."""

    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def call(self, method, path, *, json=None, json_out=False):
        self.calls.append((method, path, json))
        if self.error is not None:
            raise self.error
        return {}


def test_create_asks_for_a_rapid_cache_ultra_cache_in_the_zone():
    fs = _ApiFS()
    rcu.create(fs, "bkt", "us-central1-c")
    assert fs.calls == [
        (
            "POST",
            "b/bkt/rapidCaches",
            {
                "zone": "us-central1-c",
                "cacheType": "rapid-cache-ultra",
                "admissionPolicy": "admit-on-first-miss",
            },
        )
    ]


def test_create_accepts_a_cache_that_already_exists():
    # gcsfs retries transient errors, and a retried create whose first attempt went
    # through gets this 409.
    error = HttpError(
        {"code": 409, "message": "Rapid Cache Ultra instance already exists."}
    )
    rcu.create(_ApiFS(error), "bkt", "us-central1-c")


def test_create_raises_other_errors():
    with pytest.raises(HttpError):
        rcu.create(_ApiFS(HttpError({"code": 403, "message": "denied"})), "bkt", "zone")


def test_disable_posts_the_disable_and_never_raises():
    fs = _ApiFS()
    rcu.disable(fs, "bkt", "us-central1-c")
    assert fs.calls == [("POST", "b/bkt/rapidCaches/us-central1-c/disable", None)]
    rcu.disable(_ApiFS(HttpError({"code": 503, "message": "x"})), "bkt", "zone")


class _Clock:
    """Fake monotonic clock that sleep() advances."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def storage_layout_rpc(monkeypatch):
    """Fakes GetStorageLayout on real ExtendedGcsFileSystem instances.

    Set ``rpc.responses``; the last one repeats. ``rpc.calls`` records the bucket of
    each RPC and ``rpc.closed`` counts instances whose gRPC resources were closed.
    """

    class _Rpc:
        responses = ()
        calls = []
        closed = 0

    rpc = _Rpc()

    async def get_bucket_layout(self, bucket):
        rpc.calls.append(bucket)
        return rpc.responses[min(len(rpc.calls), len(rpc.responses)) - 1]

    async def close_resources(self):
        rpc.closed += 1

    monkeypatch.setattr(ExtendedGcsFileSystem, "_get_bucket_layout", get_bucket_layout)
    monkeypatch.setattr(ExtendedGcsFileSystem, "_close_resources", close_resources)
    return rpc


@pytest.mark.parametrize(
    "bucket_type, before, after",
    [
        ("standard_rcu_z", layouts.STANDARD, layouts.STANDARD_RCU),
        ("hns_rcu_warm", layouts.STANDARD_HIERARCHICAL, layouts.HIERARCHICAL_RCU),
    ],
)
def test_wait_for_rcu_layout_polls_until_the_cache_is_reported(
    storage_layout_rpc, bucket_type, before, after
):
    storage_layout_rpc.responses = (before, before, after)
    clock = _Clock()

    rcu.wait_for_rcu_layout(
        "rcu-bkt", bucket_type, "proj", sleep=clock.sleep, clock=clock
    )

    # A non-RCU layout stays cached on the instance, so every poll must drop it.
    assert storage_layout_rpc.calls == ["rcu-bkt"] * 3
    assert clock.sleeps == [10, 10]
    assert storage_layout_rpc.closed == 1


def test_wait_for_rcu_layout_times_out_after_ten_minutes(storage_layout_rpc):
    storage_layout_rpc.responses = (layouts.STANDARD,)
    clock = _Clock()

    with pytest.raises(TimeoutError, match="rcu-bkt"):
        rcu.wait_for_rcu_layout(
            "rcu-bkt", "standard_rcu_z", "proj", sleep=clock.sleep, clock=clock
        )

    assert clock.sleeps == [10] * 60
    assert storage_layout_rpc.closed == 1


@pytest.mark.parametrize(
    "bucket_type, layout",
    [
        ("hns_rcu_z", layouts.STANDARD_RCU),
        ("standard_rcu_z", layouts.HIERARCHICAL_RCU),
    ],
)
def test_wait_for_rcu_layout_rejects_a_bucket_with_the_wrong_namespace(
    storage_layout_rpc, bucket_type, layout
):
    storage_layout_rpc.responses = (layout,)
    clock = _Clock()

    with pytest.raises(ValueError, match="rcu-bkt"):
        rcu.wait_for_rcu_layout(
            "rcu-bkt", bucket_type, "proj", sleep=clock.sleep, clock=clock
        )

    assert clock.sleeps == []


_FLAG = "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"


def _set_flag(monkeypatch, value):
    if value is None:
        monkeypatch.delenv(_FLAG, raising=False)
    else:
        monkeypatch.setenv(_FLAG, value)


@pytest.mark.parametrize("before", ["true", None])
@pytest.mark.parametrize(
    "bucket_type, http",
    [
        ("standard_rcu_warm", True),
        ("hns_rcu_cold", True),
        ("standard_rcu_z", False),
        ("regional", False),
    ],
)
def test_http_staging_env_turns_the_flag_off_for_warm_and_cold_only(
    monkeypatch, bucket_type, http, before
):
    _set_flag(monkeypatch, before)

    with rcu.http_staging_env(bucket_type):
        inside = os.environ.get(_FLAG)

    assert inside == ("false" if http else before)
    assert os.environ.get(_FLAG) == before


@pytest.mark.parametrize("before", ["true", None])
def test_http_staging_env_restores_the_flag_when_staging_fails(monkeypatch, before):
    _set_flag(monkeypatch, before)

    with pytest.raises(RuntimeError, match="staging failed"):
        with rcu.http_staging_env("standard_rcu_cold"):
            raise RuntimeError("staging failed")

    assert os.environ.get(_FLAG) == before


def _put_gcsfs_class(queue):
    import gcsfs

    queue.put(f"{gcsfs.GCSFileSystem.__module__}.{gcsfs.GCSFileSystem.__name__}")


def _gcsfs_class_in_a_spawned_process():
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    process = ctx.Process(target=_put_gcsfs_class, args=(queue,))
    process.start()
    try:
        return queue.get(timeout=120)
    finally:
        process.join()


def test_processes_spawned_in_http_staging_env_use_the_http_only_filesystem(
    monkeypatch,
):
    monkeypatch.setenv(_FLAG, "true")

    with rcu.http_staging_env("standard_rcu_warm"):
        inside = _gcsfs_class_in_a_spawned_process()
    outside = _gcsfs_class_in_a_spawned_process()

    assert inside == "gcsfs.core.GCSFileSystem"
    assert outside == "gcsfs.extended_gcsfs.ExtendedGcsFileSystem"


class _FakeGCS:
    """A fresh gcsfs.core.GCSFileSystem over objects {path: (size, storage class)}."""

    def __init__(self, objects, fail_reads=False):
        self.objects = objects
        self.fail_reads = fail_reads
        self.reads = []
        self.closed = []
        self.loop = fsspec.asyn.get_loop()
        self._session = object()

    def find(self, path, detail=False):
        root = path.removeprefix("gs://").rstrip("/") + "/"
        return {
            name: {"name": name, "size": size, "storageClass": cls, "type": "file"}
            for name, (size, cls) in self.objects.items()
            if name.startswith(root)
        }

    def cat_file(self, path, start=None, end=None):
        if self.fail_reads:
            raise OSError(f"read of {path} failed")
        self.reads.append((path, start, end))
        return b"\0" * (end - start)

    def close_session(self, loop, session):
        assert (loop, session) == (self.loop, self._session)
        self.closed.append("session")


class _FakeExtendedGCS(_FakeGCS):
    """A fresh ExtendedGcsFileSystem, which also holds gRPC clients."""

    async def _close_resources(self):
        self.closed.append("grpc")


def test_close_logs_a_failed_grpc_close_and_still_closes_the_session(caplog):
    fs = _FakeExtendedGCS({})

    async def fail():
        raise RuntimeError("grpc close failed")

    fs._close_resources = fail

    with caplog.at_level(logging.WARNING):
        rcu._close(fs)

    assert fs.closed == ["session"]
    assert any("grpc close failed" in rec.message for rec in caplog.records)


@pytest.fixture
def core_fs(monkeypatch):
    """Serves _FakeGCS instances as gcsfs.core.GCSFileSystem; records their kwargs."""

    class _Factory:
        fake = None
        kwargs = []

        def __call__(self, **kwargs):
            self.kwargs.append(kwargs)
            return self.fake

    factory = _Factory()
    monkeypatch.setattr(gcsfs.core, "GCSFileSystem", factory)
    return factory


_PREFIX = "gs://b/case/data/"


@pytest.mark.parametrize(
    "bucket_type, staged",
    [
        ("standard_rcu_z", "RAPID"),
        ("hns_rcu_warm", "STANDARD"),
        ("standard_rcu_cold", "STANDARD"),
    ],
)
def test_assert_storage_class_accepts_the_class_the_state_stages(
    core_fs, bucket_type, staged
):
    core_fs.fake = _FakeGCS(
        {
            "b/case/data/a": (10, staged),
            "b/case/data/b": (20, staged),
            # Directory placeholders carry no data and are skipped.
            "b/case/data/": (0, "MULTI_REGIONAL"),
            "b/other-case/data/c": (10, "MULTI_REGIONAL"),
        }
    )

    rcu.assert_storage_class(_PREFIX, bucket_type)

    assert core_fs.kwargs == [{"skip_instance_cache": True}]
    assert core_fs.fake.closed == ["session"]
    assert core_fs.fake.reads == []


def test_assert_storage_class_names_objects_with_another_class(core_fs):
    core_fs.fake = _FakeGCS(
        {"b/case/data/a": (10, "RAPID"), "b/case/data/b": (10, "STANDARD")}
    )

    with pytest.raises(AssertionError, match=r"b/case/data/b \(STANDARD\)"):
        rcu.assert_storage_class(_PREFIX, "standard_rcu_z")

    assert core_fs.fake.closed == ["session"]


def test_assert_storage_class_fails_on_an_empty_prefix(core_fs):
    core_fs.fake = _FakeGCS({})

    with pytest.raises(AssertionError, match="no objects"):
        rcu.assert_storage_class(_PREFIX, "standard_rcu_z")


@pytest.mark.parametrize(
    "prefix, bucket_type",
    [
        (_PREFIX, "regional"),
        (_PREFIX, "rapid_cache_warm"),
        ("/tmp/case/data/", "standard_rcu_z"),
    ],
)
def test_assert_storage_class_skips_other_types_and_local_runs(
    core_fs, prefix, bucket_type
):
    rcu.assert_storage_class(prefix, bucket_type)

    assert core_fs.kwargs == []


@pytest.fixture
def url_to_fs(monkeypatch):
    """Serves a _FakeExtendedGCS from fsspec.core.url_to_fs; records the calls."""

    class _UrlToFs:
        fake = None
        calls = []

        def __call__(self, url, **kwargs):
            self.calls.append((url, kwargs))
            return self.fake, url.removeprefix("gs://").rstrip("/")

    resolver = _UrlToFs()
    monkeypatch.setattr(fsspec.core, "url_to_fs", resolver)
    return resolver


def test_warm_if_needed_reads_one_mib_of_each_object_then_settles(
    url_to_fs, monkeypatch
):
    monkeypatch.delenv("GCSFS_SUBSYSTEM_RCU_WARM_SETTLE_SECONDS", raising=False)
    url_to_fs.fake = _FakeExtendedGCS(
        {
            "b/case/data/big": (3 * 2**30, "STANDARD"),
            "b/case/data/small": (100, "STANDARD"),
            "b/case/data/": (0, "STANDARD"),
        }
    )
    sleeps = []

    rcu.warm_if_needed(_PREFIX, "standard_rcu_warm", sleep=sleeps.append)

    # A fresh instance: a handle opened before admission keeps reading regional
    # storage, so the timed reads must not reuse it.
    assert url_to_fs.calls == [(_PREFIX, {"skip_instance_cache": True})]
    assert sorted(url_to_fs.fake.reads) == [
        ("b/case/data/big", 0, 2**20),
        ("b/case/data/small", 0, 100),
    ]
    assert url_to_fs.fake.closed == ["grpc", "session"]
    # 120 s + 1.5 s per staged GiB.
    assert sleeps == [pytest.approx(124.5)]


def test_warm_if_needed_settle_can_be_overridden(url_to_fs, monkeypatch):
    monkeypatch.setenv("GCSFS_SUBSYSTEM_RCU_WARM_SETTLE_SECONDS", "5")
    url_to_fs.fake = _FakeExtendedGCS({"b/case/data/a": (2**30, "STANDARD")})
    sleeps = []

    rcu.warm_if_needed(_PREFIX, "hns_rcu_warm", sleep=sleeps.append)

    assert sleeps == [5.0]


def test_warm_if_needed_closes_its_instance_when_a_read_fails(url_to_fs):
    url_to_fs.fake = _FakeExtendedGCS(
        {"b/case/data/a": (10, "STANDARD")}, fail_reads=True
    )
    sleeps = []

    with pytest.raises(OSError, match="read of b/case/data/a failed"):
        rcu.warm_if_needed(_PREFIX, "standard_rcu_warm", sleep=sleeps.append)

    assert url_to_fs.fake.closed == ["grpc", "session"]
    assert sleeps == []


def test_warm_if_needed_fails_on_an_empty_prefix(url_to_fs):
    url_to_fs.fake = _FakeExtendedGCS({})

    with pytest.raises(RuntimeError, match="no objects"):
        rcu.warm_if_needed(_PREFIX, "standard_rcu_warm", sleep=lambda s: None)


@pytest.mark.parametrize(
    "bucket_type", ["standard_rcu_z", "hns_rcu_cold", "rapid_cache_warm"]
)
def test_warm_if_needed_leaves_other_types_unread(url_to_fs, bucket_type):
    sleeps = []

    rcu.warm_if_needed(_PREFIX, bucket_type, sleep=sleeps.append)

    assert (url_to_fs.calls, sleeps) == ([], [])
