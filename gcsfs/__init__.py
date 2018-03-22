from __future__ import absolute_import

from .core import GCSFileSystem
from .dask_link import register as register_dask
from .mapping import GCSMap
from ._version import get_versions

__version__ = get_versions()['version']
del get_versions
