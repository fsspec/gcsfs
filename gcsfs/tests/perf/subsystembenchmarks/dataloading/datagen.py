"""Per-case synthetic corpus dataset generator.

Dispatches uncompressed corpus generation by format (`pretok_parquet`, `text_parquet`, `pretok_jsonl`).
Shard writing is seeded deterministically and uploaded concurrently.
"""

import io
import json
import os
from concurrent.futures import ThreadPoolExecutor

VOCAB_SIZE = 32000
_CHARS_PER_TOKEN = 6  # ~6 chars per token matches token-row byte scale.
# Max ingest threads for parallel upload of latency-bound GCS shards (overridable via GCSFS_SUBSYSTEM_INGEST_THREADS).
_MAX_INGEST_THREADS = 64


def _ingest_workers(file_count):
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
    with fs.open(path, "wb") as f:
        with pq.ParquetWriter(f, schema, compression="none") as writer:
            for n in _chunks(rows, row_group_size):
                writer.write_table(make_chunk(n))
    return path


def _write_pretok_parquet(fs, root, idx, seq_len, rows, row_group_size):
    import numpy as np
    import pyarrow as pa

    rng = np.random.default_rng(idx)
    schema = pa.schema([("tokens", pa.list_(pa.int64())), ("label", pa.int64())])

    def make_chunk(n):
        tokens = rng.integers(0, VOCAB_SIZE, size=(n, seq_len), dtype=np.int64)
        return pa.table(
            {
                "tokens": pa.array(list(tokens)),
                "label": pa.array(rng.integers(0, 2, size=n, dtype=np.int64)),
            },
            schema=schema,
        )

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
    with fs.open(path, "wb") as f:
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


_WRITERS = {
    "pretok_parquet": _write_pretok_parquet,
    "text_parquet": _write_text_parquet,
    "pretok_jsonl": _write_pretok_jsonl,
}

FORMATS = tuple(_WRITERS)


def ingest_dataset(prefix, *, fmt, seq_len, file_count, rows_per_file, row_group_size):
    """Write file_count shards of rows_per_file rows under prefix concurrently and return corpus manifest."""
    if fmt not in _WRITERS:
        raise ValueError(f"unknown fmt {fmt!r}; expected one of {sorted(_WRITERS)}")
    fs, root = _fs_and_root(prefix)
    root = root.rstrip("/")
    fs.makedirs(root, exist_ok=True)
    writer = _WRITERS[fmt]

    def _write(idx):
        path = writer(fs, root, idx, seq_len, rows_per_file, row_group_size)
        return int(fs.info(path)["size"])

    with ThreadPoolExecutor(max_workers=_ingest_workers(file_count)) as pool:
        total_bytes = sum(pool.map(_write, range(file_count)))

    return {
        "fmt": fmt,
        "file_count": file_count,
        "rows_per_file": rows_per_file,
        "sample_count": file_count * rows_per_file,
        "corpus_bytes": total_bytes,
    }
