from __future__ import print_function
import os
import stat
import pandas as pd
from errno import ENOENT, EIO
from fuse import Operations, FuseOSError
from gcsfs import GCSFileSystem, core
from pwd import getpwnam
from grp import getgrnam


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

    def readdir(self, path, fh):
        path = ''.join([self.root, path])
        files = self.gcs.ls(path)
        files = [f.rstrip('/').rsplit('/', 1)[1] for f in files]
        return ['.', '..'] + files

    def mkdir(self, path, mode):
        bucket, key = core.split_path(path)
        if not self.gcs.info(path):
            self.gcs.dirs['bucket'].append({
                        'bucket': bucket, 'kind': 'storage#object',
                        'size': 0, 'storageClass': 'DIRECTORY',
                        'name': path.rstrip('/') + '/'})

    def rmdir(self, path):
        info = self.gcs.info(path)
        if info['storageClass': 'DIRECTORY']:
            self.gcs.rm(path, False)

    def read(self, path, size, offset, fh):
        print('read', path, size, offset, fh)
        fn = ''.join([self.root, path])
        f = self.cache[fn]
        f.seek(offset)
        out = f.read(size)
        return out

    def write(self, path, data, offset, fh):
        print('write', path, offset, fh)
        f = self.cache[fh]
        f.write(data)
        return len(data)

    def create(self, path, flags):
        print('create', path, oct(flags))
        fn = ''.join([self.root, path])
        self.gcs.touch(fn)  # this makes sure directory entry exists - wasteful!
        # write (but ignore creation flags)
        f = self.gcs.open(fn, 'wb')
        self.cache[self.counter] = f
        self.counter += 1
        return self.counter - 1

    def open(self, path, flags):
        print('open', path, oct(flags))
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

    def truncate(self, path, length, fh=None):
        print('truncate', path, length, fh)
        fn = ''.join([self.root, path])
        if length != 0:
            raise NotImplementedError
        # maybe should be no-op since open with write sets size to zero anyway
        self.gcs.touch(fn)

    def unlink(self, path):
        print('delete', path)
        fn = ''.join([self.root, path])
        try:
            self.gcs.rm(fn, False)
        except (IOError, FileNotFoundError):
            raise FuseOSError(EIO)

    def release(self, path, fh):
        print('close', path, fh)
        try:
            f = self.cache[fh]
            f.close()
            self.cache.pop(fh, None)  # should release any cache memory
        except Exception as e:
            print(e)
        return 0

    def chmod(self, path, mode):
        raise NotImplementedError
