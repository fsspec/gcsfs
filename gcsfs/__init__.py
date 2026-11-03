import logging
import os

try:
    from ._version import __version__  # noqa: F401
except ImportError:
    try:
        from importlib.metadata import PackageNotFoundError, version

        __version__ = version("gcsfs")
    except (ImportError, PackageNotFoundError):
        __version__ = "unknown"

logger = logging.getLogger(__name__)
from .core import GCSFileSystem
from .mapping import GCSMap

if os.getenv("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "true").lower() in ("true", "1"):
    try:
        from .extended_gcsfs import ExtendedGcsFileSystem as GCSFileSystem

        logger.info(
            "gcsfs experimental features enabled via GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT."
        )
    except ImportError as e:
        logger.warning(
            f"GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT is set, but failed to import experimental features: {e}"
        )
        # Fallback to core GCSFileSystem, do not register here

# Auto-initialize I/O tracing if GCSFS_IO_TRACE environment variable is set
if os.getenv("GCSFS_IO_TRACE") in ("1", "true", "True"):
    try:
        import atexit
        import shutil
        import tempfile
        import time
        from pathlib import Path
        from .iotracer import install_hooks_in_process, visualizer

        _trace_dir = os.environ.get("GCSFS_IO_TRACE_DIR")
        _root_process = False
        if not _trace_dir:
            _root_process = True
            _target_base = Path("/tmp")
            try:
                _usage = shutil.disk_usage(_target_base)
                if _usage.free < 2 * 1024 * 1024 * 1024:
                    _target_base = Path("./.gcsfs_traces")
                    _target_base.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
            _trace_dir = tempfile.mkdtemp(prefix="gcsfs_iotrace_", dir=str(_target_base))
            os.environ["GCSFS_IO_TRACE_DIR"] = _trace_dir
            os.environ["GCSFS_IO_TRACE_T0"] = f"{time.monotonic():.6f}"

        install_hooks_in_process()

        if _root_process:
            def _auto_render_trace():
                try:
                    visualizer.render_reports(_trace_dir, output_html="io_trace.html")
                except Exception:
                    pass

            atexit.register(_auto_render_trace)
    except Exception as _e:
        logger.warning(f"Failed to auto-initialize gcsfs.iotracer: {_e}")

__all__ = ["GCSFileSystem", "GCSMap"]

