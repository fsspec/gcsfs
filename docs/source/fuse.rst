GCSFS and FUSE
==============

Warning, this functionality is **experimental**

FUSE_ is a mechanism to mount user-level filesystems in unix-like
systems (linux, osx, etc.). GCSFS is able to use FUSE to present remote
data/keys as if they were a directory on your local file-system. This
allows for standard shell command manipulation, and loading of data
by libraries that can only handle local file-paths (e.g., netCDF/HDF5).

.. _FUSE: https://github.com/libfuse/libfuse

Requirements
-------------

In addition to a standard installation of GCSFS, you also need:

   - libfuse as a system install. The way to install this will depend
     on your OS. Examples include ``sudo apt-get install fuse``,
     ``sudo yum install fuse`` and download from osxfuse_.

   - fusepy_, which can be installed via conda or pip

   - pandas, which can also be installed via conda or pip (this library is
     used only for its timestring parsing.

.. _osxfuse: https://osxfuse.github.io/
.. _fusepy: https://github.com/terencehonles/fusepy

Usage
-----

FUSE functionality is available via the ``fsspec.fuse`` module. See the
docstrings for further details.

.. code-block:: python

    gcs = gcsfs.GCSFileSystem(..)
    from fsspec.fuse import run
    run(gcs, "bucket/path", "local/path", foreground=True, threads=False)

Caveats
-------

This functionality is experimental. The command usage may change, and you should
expect exceptions.

Furthermore:

   - although mutation operations tentatively work, you should not at the moment
     depend on gcsfuse as a reliable system that won't loose your data.

   - permissions on GCS are complicated, so all files will be shown as fully-open
     0o777, regardless of state. If a read fails, you likely don't have the right
     permissions.
