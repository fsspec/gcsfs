import io

import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset import gcsfs_opener


def torch_dataset_base():
    """Returns torch IterableDataset if installed, else object."""
    try:
        from torch.utils.data import IterableDataset
    except ImportError:
        return object
    return IterableDataset


class FakeFS:
    """Mock filesystem recording open and cat_file calls."""

    def __init__(self):
        self.calls = []

    def open(self, url, mode, **kwargs):
        self.calls.append(("open", url, mode, kwargs))
        return io.BytesIO(b"payload")

    def cat_file(self, url, **kwargs):
        self.calls.append(("cat_file", url, kwargs))
        return b"payload"


def test_default_mode_passes_no_cache_type_or_block_size():
    """Default read mode leaves cache_type unset to preserve adaptive prefetcher."""
    fs = FakeFS()
    stream = gcsfs_opener.open_url("gs://b/s.tar", read_mode="default", fs=fs)
    assert stream.read() == b"payload"
    _, url, mode, kwargs = fs.calls[0]
    assert (url, mode) == ("gs://b/s.tar", "rb")
    assert kwargs == {"concurrency": 4}  # No cache_type: prefetcher remains active.


def test_readahead_mode_sets_block_size_and_cache_type():
    fs = FakeFS()
    gcsfs_opener.open_url("gs://b/s.tar", read_mode="readahead_32mb", fs=fs)
    _, _, _, kwargs = fs.calls[0]
    assert kwargs == {
        "block_size": 32 * 2**20,
        "cache_type": "readahead",
        "concurrency": 4,
    }


def test_whole_object_mode_uses_cat_file():
    fs = FakeFS()
    stream = gcsfs_opener.open_url(
        "gs://b/s.tar", read_mode="whole_object", concurrency=16, fs=fs
    )
    assert fs.calls == [("cat_file", "gs://b/s.tar", {"concurrency": 16})]
    assert stream.read() == b"payload"


def test_read_buffer_coalesces_small_reads_before_they_reach_gcsfs():
    """Verifies client-side buffer coalesces small reader chunks into larger GCS reads."""

    class RecordingFS:
        def __init__(self):
            self.reads = []

        def open(self, url, mode, **kwargs):
            fs = self

            class Handle(io.RawIOBase):
                def __init__(self):
                    self.off = 0

                def readable(self):
                    return True

                def readinto(self, buf):
                    n = min(len(buf), 1 << 20)
                    fs.reads.append(n)
                    buf[:n] = b"\0" * n
                    self.off += n
                    return n

                def read(self, size=-1):
                    fs.reads.append(size)
                    return b"\0" * size

            return Handle()

    plain = RecordingFS()
    stream = gcsfs_opener.open_url("gs://b/s.tar", fs=plain)
    for _ in range(8):
        stream.read(10240)
    assert plain.reads == [10240] * 8

    buffered = RecordingFS()
    stream = gcsfs_opener.open_url("gs://b/s.tar", buffer_bytes=1 << 20, fs=buffered)
    for _ in range(8):
        stream.read(10240)
    assert buffered.reads == [1 << 20], "8 tar-sized reads must cost one gcsfs read"


def test_whole_object_rejects_a_read_buffer():
    """whole_object mode is already in memory and cannot accept a read buffer."""
    with pytest.raises(ValueError, match="whole_object"):
        gcsfs_opener.open_url(
            "gs://b/s.tar", read_mode="whole_object", buffer_bytes=1 << 20, fs=FakeFS()
        )


def test_read_buffer_travels_from_the_environment(monkeypatch):
    fs = FakeFS()
    monkeypatch.setattr(gcsfs_opener, "_fs", lambda: fs)
    monkeypatch.setenv(gcsfs_opener.READ_MODE_ENV, "default")
    monkeypatch.setenv(gcsfs_opener.READ_BUFFER_ENV, str(4 << 20))
    stream = gcsfs_opener.gopen_gcsfs("gs://b/s.tar", "rb", 8192)
    assert isinstance(stream, io.BufferedReader)


def test_unknown_read_mode_is_rejected():
    with pytest.raises(ValueError, match="unknown read_mode"):
        gcsfs_opener.open_url("gs://b/s.tar", read_mode="nope", fs=FakeFS())


def test_write_mode_is_rejected():
    with pytest.raises(ValueError, match="only supports"):
        gcsfs_opener.open_url("gs://b/s.tar", mode="wb", fs=FakeFS())


def test_webdataset_bufsize_is_discarded(monkeypatch):
    """Ignores WebDataset's default 8192 pipe buffer hint in favor of block_size."""
    fs = FakeFS()
    monkeypatch.setattr(gcsfs_opener, "_fs", lambda: fs)
    monkeypatch.setenv(gcsfs_opener.READ_MODE_ENV, "readahead_32mb")
    gcsfs_opener.gopen_gcsfs("gs://b/s.tar", "rb", 8192)
    _, _, _, kwargs = fs.calls[0]
    assert 8192 not in kwargs.values()
    assert kwargs["block_size"] == 32 * 2**20


class _WorkerProbe(torch_dataset_base()):
    """Iterable dataset that yields environment and registration state in workers."""

    def __iter__(self):
        yield {
            "registered": gcsfs_opener.is_registered(),
            "read_mode": gcsfs_opener.current_read_mode(),
            "concurrency": gcsfs_opener.current_read_concurrency(),
            "buffer_bytes": gcsfs_opener.current_read_buffer_bytes(),
        }


def test_a_spawned_dataloader_worker_gets_the_handler_and_the_read_environment(
    monkeypatch,
):
    pytest.importorskip("webdataset")
    torch = pytest.importorskip("torch")
    monkeypatch.setenv(gcsfs_opener.READ_MODE_ENV, "readahead_32mb")
    monkeypatch.setenv(gcsfs_opener.READ_CONCURRENCY_ENV, "16")
    monkeypatch.setenv(gcsfs_opener.READ_BUFFER_ENV, str(8 << 20))

    loader = torch.utils.data.DataLoader(
        _WorkerProbe(),
        num_workers=1,
        batch_size=None,
        worker_init_fn=gcsfs_opener.worker_init,
        multiprocessing_context="spawn",
    )
    seen = next(iter(loader))

    assert seen["registered"], "worker_init_fn did not reach the spawned worker"
    assert seen["read_mode"] == "readahead_32mb"
    assert int(seen["concurrency"]) == 16
    assert int(seen["buffer_bytes"]) == 8 << 20


def test_read_concurrency_travels_from_the_environment(monkeypatch):
    """Verifies read concurrency setting is read from the environment."""
    fs = FakeFS()
    monkeypatch.setattr(gcsfs_opener, "_fs", lambda: fs)
    monkeypatch.setenv(gcsfs_opener.READ_MODE_ENV, "default")
    monkeypatch.setenv(gcsfs_opener.READ_CONCURRENCY_ENV, "16")
    gcsfs_opener.gopen_gcsfs("gs://b/s.tar", "rb", 8192)
    assert fs.calls[0][3]["concurrency"] == 16
