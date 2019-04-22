
from collections import MutableMapping
import os

from .core import GCSFileSystem, split_path


class GCSMap(MutableMapping):
    """Wrap an GCSFileSystem as a mutable wrapping.

    The keys of the mapping become files under the given root, and the
    values (which must be bytes) the contents of those files.

    Parameters
    ----------
    root : string
        prefix for all the files (perhaps justa  bucket name
    gcs : GCSFileSystem
    check : bool (=True)
        performs a touch at the location, to check writeability.

    Examples
    --------
    >>> gcs = gcsfs.GCSFileSystem('myproject') # doctest: +SKIP
    >>> d = MapWrapping('mybucket/mapstore/', gcs=gcs) # doctest: +SKIP
    >>> d['loc1'] = b'Hello World' # doctest: +SKIP
    >>> list(d.keys()) # doctest: +SKIP
    ['loc1']
    >>> d['loc1'] # doctest: +SKIP
    b'Hello World'
    """

    def __init__(self, root, gcs=None, check=False, create=False):
        self.gcs = gcs or GCSFileSystem.current()
        self.root = root.rstrip('/').lstrip('/')
        bucket = split_path(root)[0]
        if create:
            self.gcs.mkdir(bucket)
        if check:
            if not self.gcs.exists(bucket):
                raise ValueError("Bucket %s does not exist. Create "
                                 "bucket with the ``create=True`` keyword" %
                                 bucket)
            self.gcs.touch(root+'/a')
            self.gcs.rm(root+'/a')

    def clear(self):
        """Remove all keys below root - empties out mapping
        """
        try:
            [self.gcs.rm(f) for f in self.gcs.walk(self.root)]
        except (IOError, OSError):
            # ignore non-existance of root
            pass

    def _key_to_str(self, key):
        """ Map to target path name"""
        if isinstance(key, (tuple, list)):
            key = str(tuple(key))
        else:
            key = str(key)
        return '/'.join([self.root, key])

    def __getitem__(self, key):
        key = self._key_to_str(key)
        try:
            return self.gcs.cat(key)
        except (IOError, OSError):
            raise KeyError(key)

    def __setitem__(self, key, value):
        key = self._key_to_str(key)
        with self.gcs.open(key, 'wb') as f:
            f.write(value)

    def keys(self):
        """ Contents of the mapping """
        return (x[len(self.root) + 1:] for x in self.gcs.walk(self.root))

    def __iter__(self):
        return self.keys()

    def __delitem__(self, key):
        self.gcs.rm(self._key_to_str(key))

    def __contains__(self, key):
        return self.gcs.exists(self._key_to_str(key))

    def __len__(self):
        return sum(1 for _ in self.keys())

    def __getstate__(self):
        return self.gcs, self.root

    def __setstate__(self, state):
        gcs, root = state
        self.gcs = gcs
        self.root = root
