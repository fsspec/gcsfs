"""Per-case synthetic corpus dataset generator.

Dispatches uncompressed corpus generation by format (`pretok_parquet`, `text_parquet`,
`pretok_jsonl`). Shard writing is seeded deterministically and uploaded concurrently.

`pretok_parquet` can also carry extra binary feature columns, which turn it into a wide
table for column-projected reads. Feature names, sizes and the projection are pure
functions of the case parameters, so the read side picks the same columns without
reading data.
"""

import io
import json
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor

VOCAB_SIZE = 32000
_CHARS_PER_TOKEN = 6
_MAX_INGEST_THREADS = 64
# Feature columns: one seed for their sizes and projections (every shard shares one
# schema), and a fixed log-normal spread of per-column value sizes.
_FEATURE_SEED = 0
_FEATURE_SIZE_SIGMA = 1.0


def ingest_workers(file_count):
    """Write-pool worker count capped at min(max_threads, file_count), >= 1."""
    cap = int(os.environ.get("GCSFS_SUBSYSTEM_INGEST_THREADS", _MAX_INGEST_THREADS))
    return max(1, min(cap, file_count))


def _fs_and_root(prefix):
    import fsspec

    return fsspec.core.url_to_fs(prefix)


def _chunks(rows, row_group_size):
    """Yield row-group-sized chunks to stream shards without materializing full tables in RAM."""
    remaining = rows
    while remaining > 0:
        n = min(max(1, row_group_size), remaining)
        yield n
        remaining -= n


def _write_parquet_shard(fs, root, idx, rows, row_group_size, schema, make_chunk):
    import pyarrow.parquet as pq

    path = f"{root}/shard_{idx:05d}.parquet"
    with fs.open(path, "wb", finalize_on_close=True) as f:
        with pq.ParquetWriter(f, schema, compression="none") as writer:
            for n in _chunks(rows, row_group_size):
                writer.write_table(make_chunk(n))
    return path


def _write_pretok_parquet(
    fs,
    root,
    idx,
    seq_len,
    rows,
    row_group_size,
    *,
    feature_column_count=0,
    feature_value_bytes=0,
):
    """tokens + label, plus feature_column_count binary feature columns if set."""
    import numpy as np
    import pyarrow as pa

    rng = np.random.default_rng(idx)
    widths = (
        feature_column_widths(feature_column_count, feature_value_bytes)
        if feature_column_count
        else []
    )
    schema = pa.schema(
        [("tokens", pa.list_(pa.int64())), ("label", pa.int64())]
        + [(name, pa.binary()) for name in feature_column_names(feature_column_count)]
    )

    def make_chunk(n):
        tokens = rng.integers(0, VOCAB_SIZE, size=(n, seq_len), dtype=np.int64)
        arrays = [
            pa.array(list(tokens), type=pa.list_(pa.int64())),
            pa.array(rng.integers(0, 2, size=n, dtype=np.int64)),
        ]
        for width in widths:
            offsets = np.arange(0, (n + 1) * width, width, dtype=np.int32)
            data = pa.py_buffer(rng.bytes(n * width))
            arrays.append(
                pa.Array.from_buffers(
                    pa.binary(), n, [None, pa.py_buffer(offsets), data]
                )
            )
        return pa.Table.from_arrays(arrays, schema=schema)

    return _write_parquet_shard(fs, root, idx, rows, row_group_size, schema, make_chunk)


def _write_text_parquet(fs, root, idx, seq_len, rows, row_group_size):
    import numpy as np
    import pyarrow as pa

    rng = np.random.default_rng(idx)
    nchars = seq_len * _CHARS_PER_TOKEN
    alphabet = np.frombuffer(b"abcdefghijklmnopqrstuvwxyz ", dtype=np.uint8)
    schema = pa.schema([("text", pa.string()), ("label", pa.int64())])

    def make_chunk(n):
        texts = [
            alphabet[rng.integers(0, len(alphabet), size=nchars)]
            .tobytes()
            .decode("ascii")
            for _ in range(n)
        ]
        return pa.table(
            {
                "text": pa.array(texts, type=pa.string()),
                "label": pa.array(rng.integers(0, 2, size=n, dtype=np.int64)),
            },
            schema=schema,
        )

    return _write_parquet_shard(fs, root, idx, rows, row_group_size, schema, make_chunk)


def _write_pretok_jsonl(fs, root, idx, seq_len, rows, row_group_size):
    """Write pretok JSONL shards in streaming chunks to limit memory footprint."""
    import numpy as np

    rng = np.random.default_rng(idx)
    path = f"{root}/shard_{idx:05d}.jsonl"
    with fs.open(path, "wb", finalize_on_close=True) as f:
        for n in _chunks(rows, row_group_size):
            buf = io.StringIO()
            for _ in range(n):
                toks = rng.integers(
                    0, VOCAB_SIZE, size=seq_len, dtype=np.int64
                ).tolist()
                buf.write(
                    json.dumps({"tokens": toks, "label": int(rng.integers(0, 2))})
                )
                buf.write("\n")
            f.write(buf.getvalue().encode())
    return path


def feature_column_names(count):
    """Feature column names, zero-padded so they sort in schema order."""
    digits = len(str(max(count - 1, 0)))
    return [f"f{i:0{digits}d}" for i in range(count)]


def feature_column_widths(count, value_bytes):
    """Bytes per value of each feature column: log-normal sizes with mean value_bytes."""
    import numpy as np

    rng = np.random.default_rng(_FEATURE_SEED)
    sizes = rng.lognormal(0.0, _FEATURE_SIZE_SIGMA, count)
    sizes *= value_bytes / sizes.mean()
    return [max(1, int(round(s))) for s in sizes]


def feature_projection(count, projected, run_length=1):
    """Sorted indices of the feature columns a read selects.

    Columns are picked in non-overlapping blocks of ``run_length`` adjacent columns at
    random block positions: 1 scatters them, larger values cluster them. Neighbouring
    blocks can touch, and the last one is cut to reach ``projected``. Arrow merges
    adjacent column chunks into one read, so this sets how many small reads a file
    needs. ``projected`` of 0 (or >= count) selects every feature column.
    """
    import numpy as np

    if projected == 0 or projected >= count:
        return list(range(count))
    rng = np.random.default_rng(_FEATURE_SEED)
    blocks = rng.choice(count // run_length, -(-projected // run_length), replace=False)
    columns = [int(b) * run_length + i for b in blocks for i in range(run_length)]
    return sorted(columns[:projected])


_WRITERS = {
    "pretok_parquet": _write_pretok_parquet,
    "text_parquet": _write_text_parquet,
    "pretok_jsonl": _write_pretok_jsonl,
}

FORMATS = tuple(_WRITERS)


def _write_shard_task(args):
    """Writes one shard in a worker process, resolving its own filesystem handle."""
    prefix, fmt, idx, seq_len, rows_per_file, row_group_size, fmt_options = args
    fs, root = _fs_and_root(prefix)
    path = _WRITERS[fmt](
        fs, root.rstrip("/"), idx, seq_len, rows_per_file, row_group_size, **fmt_options
    )
    return {
        "bytes": int(fs.info(path)["size"]),
        "pid": os.getpid(),
    }


def ingest_dataset(
    prefix,
    *,
    fmt,
    seq_len,
    file_count,
    rows_per_file,
    row_group_size,
    fmt_options=None,
):
    """Write file_count shards of rows_per_file rows under prefix concurrently and return corpus manifest.

    ``fmt_options`` holds format-specific writer keywords (pretok_parquet's
    ``feature_column_count`` and ``feature_value_bytes``).
    """
    if fmt not in _WRITERS:
        raise ValueError(f"unknown fmt {fmt!r}; expected one of {sorted(_WRITERS)}")
    fmt_options = dict(fmt_options or {})
    fs, root = _fs_and_root(prefix)
    fs.makedirs(root.rstrip("/"), exist_ok=True)
    tasks = [
        (prefix, fmt, idx, seq_len, rows_per_file, row_group_size, fmt_options)
        for idx in range(file_count)
    ]

    with ProcessPoolExecutor(
        max_workers=ingest_workers(file_count),
        mp_context=multiprocessing.get_context("spawn"),
    ) as pool:
        results = list(pool.map(_write_shard_task, tasks))

    return {
        "fmt": fmt,
        "file_count": file_count,
        "rows_per_file": rows_per_file,
        "sample_count": file_count * rows_per_file,
        "corpus_bytes": sum(r["bytes"] for r in results),
        "writer_process_count": len({r["pid"] for r in results}),
    }
