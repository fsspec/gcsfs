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

logger = logging.getLogger("gcsfs.rust_backend")


async def cat_file_range(bucket, object, start=None, end=None, generation=None):
    """Read a byte range of a GCS object using the Rust SDK backend.

    Runs the (blocking) Rust call in a thread so it doesn't block the event
    loop that gcsfs's async filesystem relies on.
    """
    if not available:
        raise ImportError(
            "The 'rust' read backend requires the optional 'gcsfs-rust-backend' "
            "package (see rust/gcsfs_rust in the gcsfs source tree)."
        )
    logger.debug("rust backend read: %s/%s %s-%s", bucket, object, start, end)
    return await asyncio.to_thread(
        _rust.read_range, bucket, object, start, end, generation
    )
