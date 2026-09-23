import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading import rapid_cache


class _FakeCacheFS:
    def __init__(self, states=("CREATING", "RUNNING"), files=None, fail_disable=False):
        self.calls = []
        self._states = list(states)
        self._files = dict(files or {})
        self._fail_disable = fail_disable
        self.cat_calls = []

    def call(self, method, path, *, json=None, json_out=False):
        self.calls.append((method, path, json, json_out))
        if method == "POST" and path.endswith("/anywhereCaches"):
            return {"state": "CREATING", "zone": json["zone"]}
        if method == "GET" and "/anywhereCaches/" in path:
            state = self._states.pop(0) if len(self._states) > 1 else self._states[0]
            if isinstance(state, Exception):
                raise state
            return {"state": state}
        if method == "POST" and path.endswith("/disable"):
            if self._fail_disable:
                raise RuntimeError("disable failed")
            return {"state": "DISABLED"}
        raise AssertionError(f"unexpected call: {method} {path}")

    def find(self, prefix):
        return sorted(self._files.keys())

    def cat_file(self, path):
        self.cat_calls.append(path)
        key = path[len("gs://") :] if path.startswith("gs://") else path
        return self._files.get(path, self._files.get(key))


def test_is_rapid_cache_and_ingest_on_write():
    assert rapid_cache.is_rapid_cache_bucket_type("rapid_cache_cold") is True
    assert rapid_cache.is_rapid_cache_bucket_type("rapid_cache_warm") is True
    assert rapid_cache.is_rapid_cache_bucket_type("regional") is False
    assert rapid_cache.ingest_on_write_for("rapid_cache_cold") is False
    assert rapid_cache.ingest_on_write_for("rapid_cache_warm") is True


def test_create_posts_anywhere_cache_body():
    fs = _FakeCacheFS()
    rapid_cache.create(fs, "my-bucket", "us-central1-a", ingest_on_write=True)
    assert fs.calls == [
        (
            "POST",
            "b/my-bucket/anywhereCaches",
            {"zone": "us-central1-a", "ingestOnWrite": True},
            True,
        )
    ]


def test_wait_running_polls_until_running():
    fs = _FakeCacheFS(states=["CREATING", "CREATING", "RUNNING"])
    sleeps = []
    t = [0.0]

    def clock():
        return t[0]

    def sleep(dt):
        sleeps.append(dt)
        t[0] += dt

    resp = rapid_cache.wait_running(
        fs,
        "my-bucket",
        "us-central1-a",
        timeout=60,
        poll=5,
        sleep=sleep,
        clock=clock,
    )
    assert resp["state"] == "RUNNING"
    assert sleeps == [5, 5]


def test_wait_running_fails_fast_on_terminal_state():
    fs = _FakeCacheFS(states=["DISABLED"])
    with pytest.raises(RuntimeError, match="DISABLED"):
        rapid_cache.wait_running(
            fs,
            "my-bucket",
            "us-central1-a",
            timeout=60,
            poll=5,
            sleep=lambda _: None,
        )


def test_wait_running_times_out_with_bucket_and_zone():
    fs = _FakeCacheFS(states=["CREATING"])
    ticks = iter([0.0, 10.0, 25.0])
    with pytest.raises(TimeoutError, match="my-bucket.*us-central1-a"):
        rapid_cache.wait_running(
            fs,
            "my-bucket",
            "us-central1-a",
            timeout=20,
            poll=10,
            sleep=lambda _: None,
            clock=lambda: next(ticks),
        )


def test_disable_suppresses_errors():
    fs = _FakeCacheFS(fail_disable=True)
    rapid_cache.disable(fs, "my-bucket", "us-central1-a")
    assert fs.calls == [
        ("POST", "b/my-bucket/anywhereCaches/us-central1-a/disable", None, True)
    ]


def test_warm_if_needed_reads_all_objects_only_for_warm_gcs_prefix():
    fs = _FakeCacheFS(
        files={
            "my-bucket/data/": b"",
            "my-bucket/data/shard_00000.tar": b"abc",
            "my-bucket/data/shard_00001.tar": b"defg",
        }
    )
    assert (
        rapid_cache.warm_if_needed("gs://my-bucket/data/", "rapid_cache_cold", fs=fs)
        == 0
    )
    assert fs.cat_calls == []

    assert (
        rapid_cache.warm_if_needed("/tmp/local/data/", "rapid_cache_warm", fs=fs) == 0
    )
    assert fs.cat_calls == []

    total = rapid_cache.warm_if_needed(
        "gs://my-bucket/data/", "rapid_cache_warm", fs=fs
    )
    assert total == 7
    assert sorted(fs.cat_calls) == [
        "gs://my-bucket/data/shard_00000.tar",
        "gs://my-bucket/data/shard_00001.tar",
    ]


def test_warm_if_needed_raises_when_warm_prefix_has_no_objects():
    fs = _FakeCacheFS(files={})
    with pytest.raises(RuntimeError, match="no objects found to warm"):
        rapid_cache.warm_if_needed("gs://my-bucket/data/", "rapid_cache_warm", fs=fs)


def test_wait_running_tolerates_initial_file_not_found_before_running():
    fs = _FakeCacheFS(states=[FileNotFoundError("404 Not Found"), "CREATING", "RUNNING"])
    sleeps = []
    resp = rapid_cache.wait_running(
        fs,
        "my-bucket",
        "us-central1-a",
        timeout=60,
        poll=5,
        sleep=sleeps.append,
        clock=lambda: 0.0,
    )
    assert resp["state"] == "RUNNING"
    assert sleeps == [5, 5]


@pytest.mark.parametrize("bad_value", ["0", "-10", "not-an-int"])
def test_timeout_from_env_rejects_nonpositive_or_invalid_values(monkeypatch, bad_value):
    monkeypatch.setenv("GCSFS_SUBSYSTEM_RAPID_CACHE_TIMEOUT", bad_value)
    with pytest.raises(ValueError, match="GCSFS_SUBSYSTEM_RAPID_CACHE_TIMEOUT"):
        rapid_cache.timeout_from_env()


def test_warm_if_needed_constructs_gcsfs_with_skip_instance_cache(monkeypatch):
    import gcsfs

    constructed_kwargs = []
    invalidated = []
    fake_fs = _FakeCacheFS(files={"my-bucket/data/shard_00000.tar": b"abc"})
    fake_fs.invalidate_cache = lambda: invalidated.append(True)

    def fake_gcs_filesystem(**kwargs):
        constructed_kwargs.append(kwargs)
        return fake_fs

    monkeypatch.setattr(gcsfs, "GCSFileSystem", fake_gcs_filesystem)
    total = rapid_cache.warm_if_needed("gs://my-bucket/data/", "rapid_cache_warm")
    assert total == 3
    assert constructed_kwargs == [{"skip_instance_cache": True}]
    assert invalidated == [True]


@pytest.mark.parametrize("timeout,poll", [(0, 5), (-1, 5), (60, 0), (60, -2)])
def test_wait_running_rejects_nonpositive_timeout_or_poll(timeout, poll):
    fs = _FakeCacheFS()
    with pytest.raises(ValueError, match="must be > 0"):
        rapid_cache.wait_running(
            fs, "my-bucket", "us-central1-a", timeout=timeout, poll=poll
        )


