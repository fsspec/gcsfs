from __future__ import print_function
import os
import logging
import decorator
import stat
import pandas as pd
from errno import ENOENT, EIO
from fuse import Operations, FuseOSError
from gcsfs import GCSFileSystem, core
from pwd import getpwnam
from grp import getgrnam

logger = logging.getLogger(__name__)

@decorator.decorator
def _tracemethod(f, self, *args, **kwargs):
   logger.debug("%s(args=%s, kwargs=%s)", f.__name__, args, kwargs)
   return f(self, *args, **kwargs)

def str_to_time(s):
    t = pd.to_datetime(s)
    return t.to_datetime64().view('int64') / 1e9


class GCSFS(Operations):

    def __init__(self, path='.', gcs=None, **fsargs):
        if gcs is None:
            self.gcs = GCSFileSystem(**fsargs)
        else:
            self.gcs = gcs
        self.cache = {}
        self.counter = 0
        self.root = path

    @_tracemethod
    def getattr(self, path, fh=None):
        try:
            info = self.gcs.info(''.join([self.root, path]))
        except FileNotFoundError:
            raise FuseOSError(ENOENT)
        data = {'st_uid': 1000, 'st_gid': 1000}
        perm = 0o777

        if info['storageClass'] == 'DIRECTORY' or 'bucket' in info['kind']:
            data['st_atime'] = 0
            data['st_ctime'] = 0
            data['st_mtime'] = 0
            data['st_mode'] = (stat.S_IFDIR | perm)
            data['st_size'] = 0
            data['st_blksize'] = 0
        else:
            data['st_atime'] = str_to_time(info['timeStorageClassUpdated'])
            data['st_ctime'] = str_to_time(info['timeCreated'])
            data['st_mtime'] = str_to_time(info['updated'])
            data['st_mode'] = (stat.S_IFREG | perm)
            data['st_size'] = info['size']
            data['st_blksize'] = 5 * 2**20
            data['st_nlink'] = 1

        return data

    @_tracemethod
    def readdir(self, path, fh):
        path = ''.join([self.root, path])
        files = self.gcs.ls(path)
        files = [os.path.basename(f.rstrip('/')) for f in files]
        return ['.', '..'] + files

    @_tracemethod
    def mkdir(self, path, mode):
        bucket, key = core.split_path(path)
        if not self.gcs.info(path):
            self.gcs.dirs['bucket'].append({
                        'bucket': bucket, 'kind': 'storage#object',
                        'size': 0, 'storageClass': 'DIRECTORY',
                        'name': path.rstrip('/') + '/'})

    @_tracemethod
    def rmdir(self, path):
        info = self.gcs.info(path)
        if info['storageClass': 'DIRECTORY']:
            self.gcs.rm(path, False)

    @_tracemethod
    def read(self, path, size, offset, fh):
        fn = ''.join([self.root, path])
        f = self.cache[fn]
        f.seek(offset)
        out = f.read(size)
        return out

    @_tracemethod
    def write(self, path, data, offset, fh):
        f = self.cache[fh]
        f.write(data)
        return len(data)

    @_tracemethod
    def create(self, path, flags):
        fn = ''.join([self.root, path])
        self.gcs.touch(fn)  # this makes sure directory entry exists - wasteful!
        # write (but ignore creation flags)
        f = self.gcs.open(fn, 'wb')
        self.cache[self.counter] = f
        self.counter += 1
        return self.counter - 1

    @_tracemethod
    def open(self, path, flags):
        fn = ''.join([self.root, path])
        if flags % 2 == 0:
            # read
            f = self.gcs.open(fn, 'rb')
        else:
            # write (but ignore creation flags)
            f = self.gcs.open(fn, 'wb')
        self.cache[self.counter] = f
        self.counter += 1
        return self.counter - 1

    @_tracemethod
    def truncate(self, path, length, fh=None):
        fn = ''.join([self.root, path])
        if length != 0:
            raise NotImplementedError
        # maybe should be no-op since open with write sets size to zero anyway
        self.gcs.touch(fn)

    @_tracemethod
    def unlink(self, path):
        fn = ''.join([self.root, path])
        try:
            self.gcs.rm(fn, False)
        except (IOError, FileNotFoundError):
            raise FuseOSError(EIO)

    @_tracemethod
    def release(self, path, fh):
        try:
            f = self.cache[fh]
            f.close()
            self.cache.pop(fh, None)  # should release any cache memory
        except Exception as e:
            logger.exception("exception on release")
        return 0

    @_tracemethod
    def chmod(self, path, mode):
        raise NotImplementedError
