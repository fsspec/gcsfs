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


class SmallChunkCacher:
    def __init__(self, cutoff=10000, maxmem=50 * 2 ** 20):
        self.cache = {}
        self.cutoff = cutoff
        self.maxmem = maxmem
        self.mem = 0

    def read(self, fn, offset, size, f):
        if size > self.cutoff:
            # big reads are likely sequential
            f.seek(offset)
            return f.read(size)
        if fn not in self.cache:
            self.cache[fn] = []
        c = self.cache[fn]
        for chunk in c:
            if chunk['start'] < offset and chunk['end'] > offset + size:
                print('cache hit')
                start = offset - chunk['start']
                return chunk['data'][start:start + size]
        print('cache miss')
        f.seek(offset)
        out = f.read(size)
        c.append({'start': f.start, 'end': f.end, 'data': f.cache})
        self.mem += len(f.cache)
        return out

    def close(self, fn):
        self.cache.pop(fn, None)


class GCSFS(Operations):

    def __init__(self, path='.', gcs=None, **fsargs):
        if gcs is None:
            self.gcs = GCSFileSystem(**fsargs)
        else:
            self.gcs = gcs
        self.cache = {}
        self.chunk_cacher = SmallChunkCacher()
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
        print("List", path, fh, flush=True)
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
        fn = ''.join([self.root, path])
        print('read #{} ({}) offset: {}, size: {}'.format(
            fh, fn, offset,size))
        f = self.cache[fh]
        out = self.chunk_cacher.read(fn, offset, size, f)
        return out

    def write(self, path, data, offset, fh):
        fn = ''.join([self.root, path])
        print('write #{} ({}) offset'.format(fh, fn, offset))
        f = self.cache[fh]
        f.write(data)
        return len(data)

    def create(self, path, flags):
        fn = ''.join([self.root, path])
        print('create', fn, oct(flags), end=' ')
        self.gcs.touch(fn)  # this makes sure directory entry exists - wasteful!
        # write (but ignore creation flags)
        f = self.gcs.open(fn, 'wb')
        self.cache[self.counter] = f
        print('-> fh #', self.counter)
        self.counter += 1
        return self.counter - 1

    def open(self, path, flags):
        fn = ''.join([self.root, path])
        print('open', fn, oct(flags), end=' ')
        if flags % 2 == 0:
            # read
            f = self.gcs.open(fn, 'rb')
        else:
            # write (but ignore creation flags)
            f = self.gcs.open(fn, 'wb')
        self.cache[self.counter] = f
        print('-> fh #', self.counter)
        self.counter += 1
        return self.counter - 1

    def truncate(self, path, length, fh=None):
        fn = ''.join([self.root, path])
        print('truncate #{} ({}) to {}'.format(fh, fn, length))
        if length != 0:
            raise NotImplementedError
        # maybe should be no-op since open with write sets size to zero anyway
        self.gcs.touch(fn)

    def unlink(self, path):
        fn = ''.join([self.root, path])
        print('delete', fn)
        try:
            self.gcs.rm(fn, False)
        except (IOError, FileNotFoundError):
            raise FuseOSError(EIO)

    def release(self, path, fh):
        fn = ''.join([self.root, path])
        print('close #{} ({})'.format(fh, fn))
        try:
            f = self.cache[fh]
            f.close()
            self.cache.pop(fh, None)  # should release any cache memory
        except Exception as e:
            print(e)
        return 0

    def chmod(self, path, mode):
        raise NotImplementedError
