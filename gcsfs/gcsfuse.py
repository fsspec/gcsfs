from __future__ import print_function
from collections import OrderedDict, MutableMapping
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
from threading import Lock


@decorator.decorator
def _tracemethod(f, self, *args, **kwargs):
    logger.debug("%s(args=%s, kwargs=%s)", f.__name__, args, kwargs)
    out = f(self, *args, **kwargs)
    return out


logger = logging.getLogger(__name__)


@decorator.decorator
def _tracemethod(f, self, *args, **kwargs):
    logger.debug("%s(args=%s, kwargs=%s)", f.__name__, args, kwargs)
    return f(self, *args, **kwargs)


def str_to_time(s):
    t = pd.to_datetime(s)
    return t.to_datetime64().view('int64') / 1e9


class LRUDict(MutableMapping):
    """A dict that discards least-recently-used items"""

    DEFAULT_SIZE = 128

    def __init__(self, *args, **kwargs):
        """Same arguments as OrderedDict with one additions:

        size: maximum number of entries
        """
        self.size = kwargs.pop('size', self.DEFAULT_SIZE)
        self.data = OrderedDict(*args, **kwargs)
        self.purge()

    def purge(self):
        """Removes expired or overflowing entries."""
        # pop until maximum capacity is reached
        extra = max(0, len(self.data) - self.size)
        for _ in range(extra):
            self.data.popitem(last=False)

    def __getitem__(self, key):
        if key not in self.data:
            raise KeyError(key)
        self.data.move_to_end(key)
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value
        self.data.move_to_end(key)
        self.purge()

    def __delitem__(self, key):
        del self.data[key]

    def __iter__(self):
        return iter(list(self.data))

    def __len__(self):
        return len(self.data)


class SmallChunkCacher:
    """
    Cache open GCSFiles, and data chunks from small reads

    Parameters
    ----------
    gcs : instance of GCSFileSystem
    cutoff : int
        Will store/fetch data from cache for calls to read() with values smaller
        than this.
    nfile : int
        Number of files to store in LRU cache.
    """

    def __init__(self, gcs, cutoff=10000, nfiles=3):
        self.gcs = gcs
        self.cache = LRUDict(size=nfiles)
        self.cutoff = cutoff
        self.nfiles = nfiles

    def read(self, fn, offset, size):
        """Reach block from file

        If size is less than cutoff, see if the relevant data is in the cache;
        either return data from there, or call read() on underlying file object
        and store the resultant block in the cache.
        """
        if fn not in self.cache:
            self.open(fn)
        f, chunks = self.cache[fn]
        for chunk in chunks:
            if chunk['start'] < offset and chunk['end'] > offset + size:
                logger.info('cache hit')
                start = offset - chunk['start']
                return chunk['data'][start:start + size]
        if size > self.cutoff:
            # big reads are likely sequential
            with f.lock:
                f.seek(offset)
                return f.read(size)
        logger.info('cache miss')
        with f.lock:
            bs = f.blocksize
            f.blocksize = 2 * 2 ** 20
            f.seek(offset)
            out = f.read(size)
            chunks.append({'start': f.start, 'end': f.end, 'data': f.cache})
            f.blocksize = bs

        return out

    def open(self, fn):
        """Create cache entry, or return existing open file

        May result in the eviction of LRU file object and its data blocks.
        """
        if fn not in self.cache:
            f = self.gcs.open(fn, 'rb')
            chunk = f.read(5 * 2**20)
            self.cache[fn] = f, [{'start': 0, 'end': 5 * 2**20, 'data': chunk}]
            f.lock = Lock()
            logger.info('{} inserted into cache'.format(fn))
        else:
            logger.info('{} found in cache'.format(fn))
        return self.cache[fn][0]


class GCSFS(Operations):

    def __init__(self, path='.', gcs=None, nfiles=10, **fsargs):
        if gcs is None:
            # minimum block size: still read on 5MB boundaries.
            self.gcs = GCSFileSystem(block_size=30 * 2 ** 20,
                                     cache_timeout=6000, **fsargs)
        else:
            self.gcs = gcs
        self.cache = SmallChunkCacher(self.gcs, nfiles=nfiles)
        self.write_cache = {}
        self.counter = 0
        self.root = path

    @_tracemethod
    def getattr(self, path, fh=None):
        path = ''.join([self.root, path])
        try:
            info = self.gcs.info(path)
        except FileNotFoundError:
            parent = path.rsplit('/', 1)[0]
            if path in self.gcs.ls(parent):
                info = True
            else:
                raise FuseOSError(ENOENT)
        data = {'st_uid': 1000, 'st_gid': 1000}
        perm = 0o777

        if (info is True or info['storageClass'] == 'DIRECTORY'
                or 'bucket' in info['kind']):
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
        path = ''.join([self.root, path])
        logger.info("Mkdir {}".format(path))
        parent, name = path.rsplit('/', 1)
        prefixes = self.gcs._listing_cache[parent + '/'][1]['prefixes']
        if name not in prefixes:
            prefixes.append(name)
        return 0

    @_tracemethod
    def rmdir(self, path):
        info = self.gcs.info(path)
        if info['storageClass': 'DIRECTORY']:
            self.gcs.rm(path, False)

    @_tracemethod
    def read(self, path, size, offset, fh):
        fn = ''.join([self.root, path])
        logger.info('read #{} ({}) offset: {}, size: {}'.format(
            fh, fn, offset, size))
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
        logger.info('-> fh #{}'.format(self.counter))
        self.counter += 1
        return self.counter - 1

    @_tracemethod
    def open(self, path, flags):
        fn = ''.join([self.root, path])
        logger.info('open {} {}'.format(fn, oct(flags)))
        if flags % 2 == 0:
            # read
            self.cache.open(fn)
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
