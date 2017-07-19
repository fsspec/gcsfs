from __future__ import absolute_import

from .core import GCSFileSystem
from .dask_link import register as register_dask
from .mapping import GCSMap

__version__ = "0.0.2"
