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
import time

logger = logging.getLogger(__name__)


@decorator.decorator
def _tracemethod(f, self, *args, **kwargs):
    logger.debug("%s(args=%s, kwargs=%s)", f.__name__, args, kwargs)
    return f(self, *args, **kwargs)


def str_to_time(s):
    t = pd.to_datetime(s)
    return t.to_datetime64().view('int64') / 1e9


class SmallChunkCacher:
    def __init__(self, gcs, cutoff=10000, maxmem=50 * 2 ** 20,
                 nfiles=10):
        self.gcs = gcs
        self.file_cache = {}
        self.block_cache = {}
        self.cutoff = cutoff
        self.maxmem = maxmem
        self.nfiles = nfiles
        self.mem = 0

    def read(self, fn, offset, size):
        f = self.file_cache[fn]
        if size > self.cutoff:
            # big reads are likely sequential
            f.seek(offset)
            return f.read(size)
        if fn not in self.block_cache:
            self.block_cache[fn] = []
        c = self.block_cache[fn]
        for chunk in c:
            if chunk['start'] < offset and chunk['end'] > offset + size:
                logger.info('cache hit')
                start = offset - chunk['start']
                return chunk['data'][start:start + size]
        logger.info('cache miss')
        f.seek(offset)
        out = f.read(size)
        c.append({'start': f.start, 'end': f.end, 'data': f.cache})
        self.mem += len(f.cache)
        return out

    def open(self, fn):
        if fn not in self.file_cache:
            self.file_cache[fn] = self.gcs.open(fn, 'rb')
            logger.info('{} inserted into cache'.format(fn))
        else:
            logger.info('{} found in cache'.format(fn))
        return self.file_cache[fn]

    def close(self, fn):
        self.block_cache.pop(fn, None)
        self.file_cache.pop(fn, None)


class GCSFS(Operations):

    def __init__(self, path='.', gcs=None, **fsargs):
        if gcs is None:
            self.gcs = GCSFileSystem(**fsargs)
        else:
            self.gcs = gcs
        self.cache = SmallChunkCacher(self.gcs)
        self.write_cache = {}
        self.counter = 0
        self.root = path

    @_tracemethod
    def getattr(self, path, fh=None):
        try:
            info = self.gcs.info(''.join([self.root, path]))
        except FileNotFoundError:
            raise FuseOSError(ENOENT)
        logger.info(str(list(self.gcs._listing_cache)))
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
        logger.info("List {}, {}".format(path, fh))
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
        logger.info('read #{} ({}) offset: {}, size: {}'.format(
            fh, fn, offset,size))
        out = self.cache.read(fn, offset, size)
        return out

    @_tracemethod
    def write(self, path, data, offset, fh):
        fn = ''.join([self.root, path])
        logger.info('write #{} ({}) offset'.format(fh, fn, offset))
        f = self.write_cache[fh]
        f.write(data)
        return len(data)

    @_tracemethod
    def create(self, path, flags):
        fn = ''.join([self.root, path])
        logger.info('create {} {}'.format(fn, oct(flags)))
        self.gcs.touch(fn)  # this makes sure directory entry exists - wasteful!
        # write (but ignore creation flags)
        f = self.gcs.open(fn, 'wb')
        self.write_cache[self.counter] = f
        print('-> fh #', self.counter)
        self.counter += 1
        return self.counter - 1

    @_tracemethod
    def open(self, path, flags):
        fn = ''.join([self.root, path])
        logger.info('open {} {}'.format(fn, oct(flags)))
        if flags % 2 == 0:
            # read
            f = self.cache.open(fn)
        else:
            # write (but ignore creation flags)
            f = self.gcs.open(fn, 'wb')
            self.write_cache[self.counter] = f
        logger.info('-> fh #{}'.format(self.counter))
        self.counter += 1
        return self.counter - 1

    @_tracemethod
    def truncate(self, path, length, fh=None):
        fn = ''.join([self.root, path])
        logger.info('truncate #{} ({}) to {}'.format(fh, fn, length))
        if length != 0:
            raise NotImplementedError
        # maybe should be no-op since open with write sets size to zero anyway
        self.gcs.touch(fn)

    @_tracemethod
    def unlink(self, path):
        fn = ''.join([self.root, path])
        logger.info('delete', fn)
        try:
            self.gcs.rm(fn, False)
        except (IOError, FileNotFoundError):
            raise FuseOSError(EIO)

    @_tracemethod
    def release(self, path, fh):
        fn = ''.join([self.root, path])
        logger.info('close #{} ({})'.format(fh, fn))
        try:
            if fh in self.write_cache:
                # write mode
                f = self.write_cache[fh]
                f.close()
                self.write_cache.pop(fh, None)
        except Exception as e:
            logger.exception("exception on release:" + str(e))
        return 0

    @_tracemethod
    def chmod(self, path, mode):
        raise NotImplementedError
