import logging

from .core import GCSFileSystem

logger = logging.getLogger('gcsfs')


def register():
    """
    Make GCS filesystem available to dask parallel I/O.
    URLs can look like ``gs://myproject@mybucket/mypath.*.csv``

    Returns
    -------

    """
    global DaskGCSFileSystem
    import dask.bytes.core

    class DaskGCSFileSystem(GCSFileSystem):

        sep = '/'

        def __init__(self, username=None, password=None, project=None,
                     path=None, host=None, **kwargs):
            if project is not None:
                if username is not None:
                    raise ValueError('GCS project defined twice: %s'
                                     % ((username, project),))
                username = project
            if username is not None:
                GCSFileSystem.__init__(self, project=username, **kwargs)
            else:
                GCSFileSystem.__init__(self, **kwargs)

        def open(self, path, mode='rb', **kwargs):
            bucket = kwargs.pop('host', '')
            gcs_path = bucket + path
            return GCSFileSystem.open(self, gcs_path, mode=mode)

        def glob(self, path, **kwargs):
            bucket = kwargs.pop('host', '')
            gcs_path = bucket + path
            return GCSFileSystem.glob(self, gcs_path)

        def mkdirs(self, path):
            pass  # no need to pre-make paths on GCS

        def ukey(self, path):
            return self.info(path)['etag']

        def size(self, path):
            return self.info(path)['size']

        def _get_pyarrow_filesystem(self):
            """Make a version of the FS instance which will be acceptable to pyarrow

            This just copies everything, but adds the pyarrow FileSystem as a
            superclass, so that in arrow functions it passes isinstance checks.
            """
            import pyarrow as pa

            class PyarrowWrappedGCSFS(DaskGCSFileSystem,
                                      pa.filesystem.DaskFileSystem):

                def __init__(self, fs):
                    self.fs = fs
                    self.name = str(fs.__class__)
                    self.__dict__.update(fs.__dict__)

                def isdir(self, path):
                    try:
                        contents = self.fs.ls(path)
                        if len(contents) == 1 and contents[0] == path:
                            return False
                        else:
                            return True
                    except OSError:
                        return False

                def isfile(self, path):
                    try:
                        contents = self.fs.ls(path)
                        return len(contents) == 1 and contents[0] == path
                    except OSError:
                        return False

                def walk(self, path, refresh=False, maxdepth=5):
                    """
                    Directory tree generator, like os.walk

                    Generator version of what is in s3fs, which yields a flattened list of
                    files
                    """
                    full_dirs = []
                    dirs = []
                    files = []

                    for info in self.ls(path, True):
                        # each info name must be at least [path]/part , but here
                        # we check also for names like [path]/part/
                        name = info['name']
                        if name.endswith('/'):
                            tail = '/'.join(name.rsplit('/', 2)[-2:])
                        else:
                            tail = name.rsplit('/', 1)[1]
                        if info['storageClass'] == 'DIRECTORY':
                            full_dirs.append(name)
                            dirs.append(tail)
                        else:
                            files.append(tail)
                    yield path, dirs, files

                    for d in full_dirs:
                        if maxdepth is None or maxdepth > 1:
                            for res in self.walk(d):
                                yield res

            return PyarrowWrappedGCSFS(self)

    dask.bytes.core._filesystems['gcs'] = DaskGCSFileSystem
    dask.bytes.core._filesystems['gs'] = DaskGCSFileSystem


try:
    register()
except ImportError as e:
    logger.debug(e)
