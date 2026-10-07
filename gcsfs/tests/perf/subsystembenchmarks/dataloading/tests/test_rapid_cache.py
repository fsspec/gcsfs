import io

import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading import rapid_cache


class _FakeCacheFS:
    def __init__(self, states=("CREATING", "RUNNING"), files=None, fail_disable=False):
        self.calls = []
        self._states = list(states)
        self._files = dict(files or {})
        self._fail_disable = fail_disable
        self.opened = []

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

    def open(self, path, mode="rb"):
        self.opened.append((path, mode))
        return io.BytesIO(self._files[path])


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
    sleeps = []
    assert (
        rapid_cache.warm_if_needed(
            "gs://my-bucket/data/", "rapid_cache_cold", fs=fs, sleep=sleeps.append
        )
        == 0
    )
    assert fs.opened == []
    assert sleeps == []

    assert (
        rapid_cache.warm_if_needed(
            "/tmp/local/data/", "rapid_cache_warm", fs=fs, sleep=sleeps.append
        )
        == 0
    )
    assert fs.opened == []
    assert sleeps == []

    total = rapid_cache.warm_if_needed(
        "gs://my-bucket/data/", "rapid_cache_warm", fs=fs, sleep=sleeps.append
    )
    assert total == 7
    assert sorted(fs.opened) == [
        ("my-bucket/data/shard_00000.tar", "rb"),
        ("my-bucket/data/shard_00000.tar", "rb"),
        ("my-bucket/data/shard_00001.tar", "rb"),
        ("my-bucket/data/shard_00001.tar", "rb"),
    ]
    assert sleeps == [
        rapid_cache.DEFAULT_WARMUP_SETTLE_SECONDS,
        rapid_cache.DEFAULT_WARMUP_SETTLE_SECONDS,
    ]


def test_warm_if_needed_raises_when_warm_prefix_has_no_objects():
    fs = _FakeCacheFS(files={})
    with pytest.raises(RuntimeError, match="no objects found to warm"):
        rapid_cache.warm_if_needed(
            "gs://my-bucket/data/", "rapid_cache_warm", fs=fs, sleep=lambda _: None
        )


def test_wait_running_tolerates_initial_file_not_found_before_running():
    fs = _FakeCacheFS(
        states=[FileNotFoundError("404 Not Found"), "CREATING", "RUNNING"]
    )
    sleeps = []
    t = [0.0]

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
        clock=lambda: t[0],
    )
    assert resp["state"] == "RUNNING"
    assert sleeps == [5, 5]


def test_wait_running_times_out_with_last_state_not_found():
    fs = _FakeCacheFS(states=[FileNotFoundError("404 Not Found")])
    ticks = iter([0.0, 10.0, 25.0])
    with pytest.raises(
        TimeoutError, match="my-bucket.*us-central1-a.*last state: 'NOT_FOUND'"
    ):
        rapid_cache.wait_running(
            fs,
            "my-bucket",
            "us-central1-a",
            timeout=20,
            poll=10,
            sleep=lambda _: None,
            clock=lambda: next(ticks),
        )


@pytest.mark.parametrize("timeout,poll", [(0, 5), (-1, 5), (60, 0), (60, -2)])
def test_wait_running_rejects_nonpositive_timeout_or_poll(timeout, poll):
    fs = _FakeCacheFS()
    with pytest.raises(ValueError, match="must be > 0"):
        rapid_cache.wait_running(
            fs, "my-bucket", "us-central1-a", timeout=timeout, poll=poll
        )


def test_warm_if_needed_lists_through_a_fresh_url_to_fs_instance(monkeypatch):
    import fsspec

    calls = []
    fs = _FakeCacheFS(files={"my-bucket/data/shard_00000.tar": b"abc"})

    def fake_url_to_fs(url, **kwargs):
        calls.append((url, kwargs))
        return fs, url

    monkeypatch.setattr(fsspec.core, "url_to_fs", fake_url_to_fs)
    total = rapid_cache.warm_if_needed(
        "gs://my-bucket/data/", "rapid_cache_warm", sleep=lambda _: None
    )
    assert total == 3
    assert calls == [("gs://my-bucket/data/", {"skip_instance_cache": True})]


def test_wait_running_tolerates_none_and_pending_states():
    responses = iter(
        [
            {"state": None},
            {},
            {"state": "PENDING"},
            {"state": "RUNNING", "zone": "us-central1-a"},
        ]
    )

    class _CustomStateFS:
        def call(self, method, path, **kwargs):
            return next(responses)

    sleeps = []
    resp = rapid_cache.wait_running(
        _CustomStateFS(),
        "my-bucket",
        "us-central1-a",
        timeout=60,
        poll=5,
        sleep=sleeps.append,
        clock=lambda: 0.0,
    )
    assert resp["state"] == "RUNNING"
    assert sleeps == [5, 5, 5]
