from .core import GCSFileSystem
from .mapping import GCSMap
from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

__all__ = ["GCSFileSystem", "GCSMap"]
