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

Installation of GCSFS will make the command ``gcsfuse``. Use the flag ``--help``
to display the usage message.

You must provide a path on GCS to regard as the root directory for your data, and
a `mount point`, a location on your local file-system in which you want the remote
data to appear. For example, lets consider that the bucket ``mybucket`` contains a
key ``path/key``,
(full path ``mybucket/path/key``). If the remote root is set to ``mybucket/path``
and the mount point is ``~/gcs`` then after
mounting, listing the contents of ``~/gcs`` will show a file called ``key``.

.. code-block::bash

   $ gcsfuse mybucket/path ~/fuse
   $ ls ~/fuse
   key

Starting the process in foreground mode will give some debug information on which
bytes in which keys are being read.

To stop the process, either use ^C (if in foreground mode), explicitly terminate
the process, or use the command ``umount`` with the mount point (in this example
``umount ~/gcs``).

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
