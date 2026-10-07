"""Routes WebDataset gs:// reads through gcsfs via environment configuration."""

import io
import os

READAHEAD_BLOCK_SIZE = 32 * 2**20

READ_MODES = ("default", "readahead_32mb", "whole_object")
READ_MODE_ENV = "GCSFS_SUBSYSTEM_WDS_READ_MODE"

# Default gcsfs concurrency passed explicitly per call.
DEFAULT_READ_CONCURRENCY = 4
READ_CONCURRENCY_ENV = "GCSFS_SUBSYSTEM_WDS_READ_CONCURRENCY"

# Client-side buffer size in bytes (0 for none).
DEFAULT_READ_BUFFER_BYTES = 0
READ_BUFFER_ENV = "GCSFS_SUBSYSTEM_WDS_READ_BUFFER"


def _fs():
    import gcsfs

    return gcsfs.GCSFileSystem()


def current_read_mode():
    return os.environ.get(READ_MODE_ENV, "default")


def current_read_concurrency():
    return int(os.environ.get(READ_CONCURRENCY_ENV, DEFAULT_READ_CONCURRENCY))


def current_read_buffer_bytes():
    return int(os.environ.get(READ_BUFFER_ENV, DEFAULT_READ_BUFFER_BYTES))


def open_url(
    url,
    mode="rb",
    read_mode="default",
    concurrency=DEFAULT_READ_CONCURRENCY,
    buffer_bytes=DEFAULT_READ_BUFFER_BYTES,
    fs=None,
):
    """Opens a gs:// URL through gcsfs using the selected read mode."""
    if mode != "rb":
        raise ValueError(f"gcsfs opener only supports mode 'rb', got {mode!r}")
    fs = fs if fs is not None else _fs()
    if read_mode == "whole_object":
        # Whole object is already in memory and cannot be buffered.
        if buffer_bytes:
            raise ValueError("whole_object reads cannot take a read buffer")
        return io.BytesIO(fs.cat_file(url, concurrency=concurrency))
    if read_mode == "readahead_32mb":
        # Explicit cache_type disables gcsfs adaptive prefetcher.
        return _buffered(
            fs.open(
                url,
                "rb",
                block_size=READAHEAD_BLOCK_SIZE,
                cache_type="readahead",
                concurrency=concurrency,
            ),
            buffer_bytes,
        )
    if read_mode == "default":
        # Omit cache_type to retain gcsfs adaptive prefetcher.
        return _buffered(fs.open(url, "rb", concurrency=concurrency), buffer_bytes)
    raise ValueError(f"unknown read_mode {read_mode!r}; expected one of {READ_MODES}")


def _buffered(handle, buffer_bytes):
    """Wraps handle in io.BufferedReader to coalesce small reads."""
    if not buffer_bytes:
        return handle
    return io.BufferedReader(handle, buffer_size=buffer_bytes)


def gopen_gcsfs(url, mode="rb", bufsize=8192, **kw):
    """WebDataset gopen handler for gs:// URLs (ignores pipe bufsize hint)."""
    return open_url(
        url,
        mode,
        read_mode=current_read_mode(),
        concurrency=current_read_concurrency(),
        buffer_bytes=current_read_buffer_bytes(),
    )


def register():
    import webdataset as wds

    wds.gopen_schemes["gs"] = gopen_gcsfs


def is_registered():
    """Returns True if gs:// is registered to gopen_gcsfs in this process."""
    import webdataset as wds

    return wds.gopen_schemes.get("gs") is gopen_gcsfs


def worker_init(worker_id):
    """DataLoader worker_init_fn to register the gs:// handler in spawned workers."""
    del worker_id
    register()
