"""Optional Rust (google-cloud-storage SDK) read backend for gcsfs.

This wraps the ``gcsfs-rust-backend`` PyO3 extension (see ``rust/gcsfs_rust``)
so gcsfs's async I/O path can opt into it for object reads. The extension is
an optional dependency: importing this module never raises, but calling
``read_range`` without the extension installed does.
"""

import asyncio
import logging

try:
    import gcsfs_rust_backend as _rust

    available = True
except ImportError:
    _rust = None
    available = False

# Older builds of the extension only expose the blocking entry point.
_has_async = available and hasattr(_rust, "read_range_async")

logger = logging.getLogger("gcsfs.rust_backend")


async def cat_file_range(bucket, object, start=None, end=None, generation=None):
    """Read a byte range of a GCS object using the Rust SDK backend.

    Prefers the extension's native awaitable, which lets the running asyncio
    loop drive the Rust future directly. Falls back to dispatching the
    blocking call to a worker thread.
    """
    if not available:
        raise ImportError(
            "The 'rust' read backend requires the optional 'gcsfs-rust-backend' "
            "package (see rust/gcsfs_rust in the gcsfs source tree)."
        )
    logger.debug("rust backend read: %s/%s %s-%s", bucket, object, start, end)
    if _has_async:
        return await _rust.read_range_async(bucket, object, start, end, generation)
    return await asyncio.to_thread(
        _rust.read_range, bucket, object, start, end, generation
    )
