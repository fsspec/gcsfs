import json

import pytest

pytest.importorskip("pyarrow")

import fsspec  # noqa: E402

from gcsfs.tests.perf.subsystembenchmarks.dataloading import datagen  # noqa: E402


def _prefix(tmp_path):
    """On-disk prefix: shards are written by worker processes, which share no in-memory fs."""
    return f"{tmp_path}/data/"


def test_pretok_parquet_shapes_and_rowgroups(tmp_path):
    import pyarrow.parquet as pq

    prefix = _prefix(tmp_path)
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


def test_text_parquet_has_text_column(tmp_path):
    import pyarrow.parquet as pq

    prefix = _prefix(tmp_path)
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


def test_pretok_jsonl_lines_parse(tmp_path):
    prefix = _prefix(tmp_path)
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


def test_unknown_format_raises(tmp_path):
    with pytest.raises(ValueError):
        datagen.ingest_dataset(
            _prefix(tmp_path),
            fmt="bogus",
            seq_len=4,
            file_count=1,
            rows_per_file=1,
            row_group_size=1,
        )


def test_row_groups_are_streamed_not_buffered_whole(tmp_path):
    """Shards reach gigabytes and are written concurrently; a chunked writer caps RAM at one
    row group. The row-group layout the axis asks for must survive the chunking."""
    import pyarrow.parquet as pq

    prefix = _prefix(tmp_path)
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

    def open(self, path, mode, **kwargs):
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
    assert datagen.ingest_workers(4) == 4
    assert datagen.ingest_workers(10_000) == datagen._MAX_INGEST_THREADS
    assert datagen._MAX_INGEST_THREADS >= 64
    monkeypatch.setenv("GCSFS_SUBSYSTEM_INGEST_THREADS", "128")
    assert datagen.ingest_workers(10_000) == 128
    assert datagen.ingest_workers(0) == 1


def test_shards_are_written_across_worker_processes(tmp_path):
    """Shard synthesis is CPU-bound under the GIL, so the pool must be processes, not threads."""
    man = datagen.ingest_dataset(
        _prefix(tmp_path),
        fmt="pretok_parquet",
        seq_len=4,
        file_count=8,
        rows_per_file=2,
        row_group_size=2,
    )
    assert man["writer_process_count"] > 1


def test_corpus_bytes_are_deterministic_across_runs(tmp_path):
    """Shard content is seeded by index alone, so a rerun reproduces the corpus byte for byte."""
    kwargs = dict(
        fmt="text_parquet", seq_len=8, file_count=3, rows_per_file=10, row_group_size=5
    )
    first = datagen.ingest_dataset(_prefix(tmp_path / "a"), **kwargs)
    second = datagen.ingest_dataset(_prefix(tmp_path / "b"), **kwargs)
    assert first["corpus_bytes"] == second["corpus_bytes"]
    fs, _ = fsspec.core.url_to_fs(str(tmp_path))
    assert fs.cat(f"{tmp_path}/a/data/shard_00000.parquet") == fs.cat(
        f"{tmp_path}/b/data/shard_00000.parquet"
    )


def test_pretok_parquet_feature_columns(tmp_path):
    import pyarrow.parquet as pq

    prefix = _prefix(tmp_path)
    man = datagen.ingest_dataset(
        prefix,
        fmt="pretok_parquet",
        seq_len=4,
        file_count=2,
        rows_per_file=100,
        row_group_size=50,
        fmt_options=dict(feature_column_count=40, feature_value_bytes=8),
    )
    assert man["sample_count"] == 200
    fs, root = fsspec.core.url_to_fs(prefix)
    f0 = sorted(p for p in fs.ls(root, detail=False) if p.endswith(".parquet"))[0]
    with fs.open(f0, "rb") as fh:
        md = pq.ParquetFile(fh).metadata
    assert md.num_row_groups == 2
    # Leaf paths: the tokens list column's leaf is "tokens.list.element".
    names = [md.schema.column(i).path.split(".")[0] for i in range(md.num_columns)]
    assert names[:2] == ["tokens", "label"]
    assert names[2:] == datagen.feature_column_names(40)
    for i, width in enumerate(datagen.feature_column_widths(40, 8)):
        # Plain binary pages: 4-byte length + value per row, plus page headers.
        assert md.row_group(0).column(i + 2).total_uncompressed_size >= 50 * (4 + width)


def test_feature_column_widths_vary_around_the_mean():
    widths = datagen.feature_column_widths(1000, 12)
    assert min(widths) >= 1
    assert abs(sum(widths) / 1000 - 12) < 0.5
    assert len(set(widths)) > 10
    with pytest.raises(ValueError, match="feature_column_count"):
        datagen.feature_column_widths(0, 12)


def test_feature_projection_run_length_clusters_columns():
    def runs(indices):
        return 1 + sum(1 for a, b in zip(indices, indices[1:]) if b != a + 1)

    scattered = datagen.feature_projection(1134, 286, run_length=1)
    clustered = datagen.feature_projection(1134, 286, run_length=16)
    for cols in (scattered, clustered):
        assert len(cols) == len(set(cols)) == 286
        assert cols == sorted(cols)
    assert runs(scattered) > 150
    assert runs(clustered) <= 286 // 16 + 1
    assert scattered == datagen.feature_projection(1134, 286, run_length=1)
    # 0 or >= count selects every feature column.
    assert datagen.feature_projection(20, 0) == list(range(20))
    assert datagen.feature_projection(20, 25) == list(range(20))
