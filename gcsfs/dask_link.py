from .core import GCSFileSystem


def register():
    """
    Make GCS filesystem available to dask parallel I/O.
    URLs can look like ``gs://myproject@mybucket/mypath.*.csv``

    Returns
    -------

    """
    global DaskGCSFileSystem
    import dask.bytes.core

    class DaskGCSFileSystem(GCSFileSystem, dask.bytes.core.FileSystem):

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

    dask.bytes.core._filesystems['gcs'] = DaskGCSFileSystem
    dask.bytes.core._filesystems['gs'] = DaskGCSFileSystem

try:
    register()
except ImportError as e:
    print(e)
