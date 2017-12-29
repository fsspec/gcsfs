import errno
import logging
import stat
import sys

from distributed.utils import log_errors
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import gcsfs
import pandas as pd


class GCS(LoggingMixIn, Operations):
    def __init__(self, fs, bucket):
        print('hello')
        self.bucket = bucket.rstrip('/')
        self.files = dict()
        self.fd = 0
        self.fs = fs

    def _path(self, path):
        print('path:', path)
        return self.bucket + '/' + path.lstrip('/')

    def create(self, path, mode):
        file = self.fs.open(self._path(path), mode=mode)
        fd = self.fd
        self.fd += 1
        self.files[fd] = file
        return fd

    def open(self, path, flags):
        file = self.fs.open(self._path(path))
        fd = self.fd
        self.fd += 1
        self.files[fd] = file
        return fd

    def read(self, path, length, offset, fh):
        file = self.files[fh]
        file.seek(offset)
        data = file.read(length)
        return data

    def write(self, path, buf, offset, fh):
        file = self.files[fh]
        file.seek(offset)
        return file.write(fh, buf)

    def flush(self, path, fh):
        return self.file[fh].flush()

    def release(self, path, fh):
        self.file[fh].close()
        del self.file[fh]

    def fsync(self, path, fdatasync, fh):
        pass

    # Filesystem methods
    # ==================

    def getattr(self, path, fh=None):
        if path == '/':
            return {'st_mode': stat.S_IFDIR | 0o666,
                    'st_blksize': 0,
                    'st_size': 0}
        path = self._path(path)
        print('getattr', path)
        with log_errors():
            try:
                info = self.fs.info(path)
            except FileNotFoundError:
                raise FuseOSError(errno.ENOENT)
            return info_stat(info)

    def readdir(self, path, fh):
        dirents = ['.', '..']
        paths = self.fs.ls(self._path(path), detail=False)
        print(paths)

        for p in dirents + list(paths):
            yield p

    def rmdir(self, path):
        return self.fs.rmdir(self._path(path))

    def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)

    def mkdir(self, path, mode):
        return self.fs.mkdir(path)

    def statfs(self, path):
        return {'f_bsize': 5122, 'f_blocks': 4096, 'f_bavail': 2048}

    def unlink(self, path):
        return self.fs.rm(self._path(path))

    def rename(self, old, new):
        return self.fs.mv(self._path(old), self._path(new))

    # def utimens(self, path, times=None):
    #     return os.utime(self._full_path(path), times)


def info_stat(info):
    out = {'st_size': info['size']}
    permissions = 0o666  # TODO: need to find real permission levels
    if info['storageClass'] == 'DIRECTORY':
        out['st_mode'] = stat.S_IFDIR | permissions
        out['st_blksize'] = 0
    else:
        out['st_mode'] = stat.S_IFREG | permissions
        out['st_blksize'] = 5122
        out['st_nlink'] = 1
        out['st_ctime'] = int(pd.Timestamp(info['timeCreated']).value / 1e9)
        out['st_mtime'] = int(pd.Timestamp(info['updated']).value / 1e9)
        out['st_atime'] = int(pd.Timestamp(info['timeStorageClassUpdated']).value / 1e9)
    return out


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('usage: %s <bucket> <mountpoint>' % sys.argv[0])
        sys.exit(1)

    fs = gcsfs.GCSFileSystem()
    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(GCS(fs, sys.argv[1]), sys.argv[2], foreground=True)
