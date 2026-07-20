import json
import uuid

import pytest

pytest.importorskip("pyarrow")

import fsspec  # noqa: E402

from gcsfs.tests.perf.subsystembenchmarks.dataloading import datagen  # noqa: E402


def _prefix():
    return f"memory://{uuid.uuid4().hex}/data/"


def test_pretok_parquet_shapes_and_rowgroups():
    import pyarrow.parquet as pq

    prefix = _prefix()
    man = datagen.ingest_dataset(
        prefix,
        fmt="pretok_parquet",
        seq_len=16,
        file_count=3,
        rows_per_file=100,
        row_group_size=25,
    )
    assert man["fmt"] == "pretok_parquet"
    assert man["file_count"] == 3
    assert man["sample_count"] == 300
    assert man["corpus_bytes"] > 0
    fs, root = fsspec.core.url_to_fs(prefix)
    files = sorted(p for p in fs.ls(root, detail=False) if p.endswith(".parquet"))
    assert len(files) == 3
    with fs.open(files[0], "rb") as f:
        pf = pq.ParquetFile(f)
    assert pf.num_row_groups == 4
    assert set(pf.schema_arrow.names) == {"tokens", "label"}


def test_text_parquet_has_text_column():
    import pyarrow.parquet as pq

    prefix = _prefix()
    datagen.ingest_dataset(
        prefix,
        fmt="text_parquet",
        seq_len=16,
        file_count=2,
        rows_per_file=10,
        row_group_size=10,
    )
    fs, root = fsspec.core.url_to_fs(prefix)
    f0 = sorted(p for p in fs.ls(root, detail=False) if p.endswith(".parquet"))[0]
    with fs.open(f0, "rb") as fh:
        pf = pq.ParquetFile(fh)
    assert set(pf.schema.names) == {"text", "label"}
    assert "string" in str(pf.schema_arrow.field("text").type)


def test_pretok_jsonl_lines_parse():
    prefix = _prefix()
    man = datagen.ingest_dataset(
        prefix,
        fmt="pretok_jsonl",
        seq_len=8,
        file_count=2,
        rows_per_file=5,
        row_group_size=0,
    )
    assert man["sample_count"] == 10
    fs, root = fsspec.core.url_to_fs(prefix)
    f0 = sorted(p for p in fs.ls(root, detail=False) if p.endswith(".jsonl"))[0]
    lines = fs.cat(f0).decode().strip().splitlines()
    assert len(lines) == 5
    obj = json.loads(lines[0])
    assert set(obj) == {"tokens", "label"} and len(obj["tokens"]) == 8


def test_unknown_format_raises():
    with pytest.raises(ValueError):
        datagen.ingest_dataset(
            _prefix(),
            fmt="bogus",
            seq_len=4,
            file_count=1,
            rows_per_file=1,
            row_group_size=1,
        )


def test_row_groups_are_streamed_not_buffered_whole():
    """Shards reach gigabytes and are written concurrently; a chunked writer caps RAM at one
    row group. The row-group layout the axis asks for must survive the chunking."""
    import pyarrow.parquet as pq

    prefix = _prefix()
    datagen.ingest_dataset(
        prefix,
        fmt="pretok_parquet",
        seq_len=8,
        file_count=1,
        rows_per_file=100,
        row_group_size=25,
    )
    fs, root = fsspec.core.url_to_fs(prefix)
    f0 = sorted(p for p in fs.ls(root, detail=False) if p.endswith(".parquet"))[0]
    with fs.open(f0, "rb") as fh:
        pf = pq.ParquetFile(fh)
    assert pf.num_row_groups == 4
    assert [pf.metadata.row_group(i).num_rows for i in range(4)] == [25, 25, 25, 25]


class _RecordingFile:
    def __init__(self):
        self.writes = []

    def write(self, data):
        self.writes.append(data)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class _RecordingFS:
    def __init__(self):
        self.file = _RecordingFile()

    def open(self, path, mode):
        return self.file


def test_jsonl_shard_is_streamed_not_buffered_whole():
    """Verify that pretok_jsonl writes chunk-sized batches in order without buffering entire shard."""
    fs = _RecordingFS()
    path = datagen._write_pretok_jsonl(
        fs, "root", 0, seq_len=4, rows=10, row_group_size=3
    )
    assert path == "root/shard_00000.jsonl"
    assert len(fs.file.writes) == 4
    lines = b"".join(fs.file.writes).decode().strip().splitlines()
    assert len(lines) == 10
    assert all(len(json.loads(line)["tokens"]) == 4 for line in lines)


def test_ingest_workers_scales_and_caps(monkeypatch):
    """Verify ingest worker count calculation and cap behavior."""
    monkeypatch.delenv("GCSFS_SUBSYSTEM_INGEST_THREADS", raising=False)
    assert datagen._ingest_workers(4) == 4
    assert datagen._ingest_workers(10_000) == datagen._MAX_INGEST_THREADS
    assert datagen._MAX_INGEST_THREADS >= 64
    monkeypatch.setenv("GCSFS_SUBSYSTEM_INGEST_THREADS", "128")
    assert datagen._ingest_workers(10_000) == 128
    assert datagen._ingest_workers(0) == 1


def test_shards_are_written_concurrently():
    """Verify shard writing occurs concurrently across thread pool workers."""
    import threading

    seen = set()
    real = datagen._WRITERS["pretok_parquet"]

    def spy(fs, root, idx, seq_len, rows, row_group_size):
        seen.add(threading.current_thread().name)
        return real(fs, root, idx, seq_len, rows, row_group_size)

    datagen._WRITERS["_spy"] = spy
    try:
        datagen.ingest_dataset(
            _prefix(),
            fmt="_spy",
            seq_len=4,
            file_count=8,
            rows_per_file=2,
            row_group_size=2,
        )
    finally:
        del datagen._WRITERS["_spy"]
    assert len(seen) > 1
