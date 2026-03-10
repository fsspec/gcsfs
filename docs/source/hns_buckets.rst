Hierarchical Namespace (HNS)
==============================================

To train, checkpoint, and serve AI models at peak efficiency, Google Cloud Storage (GCS) offers **Hierarchical Namespace (HNS)**.

``gcsfs`` provides full support for all data and metadata operations on HNS buckets.

What is a Hierarchical Namespace (HNS)?
---------------------------------------

Historically, GCS buckets have utilized a **flat namespace**. In a flat
namespace, directories do not exist as distinct physical entities; they are
simulated by 0-byte objects ending in a slash (``/``) or by filtering object
prefixes during list operations.

A `Hierarchical Namespace (HNS) <https://cloud.google.com/storage/docs/hns-overview>`_ introduces true, logical directories as first-class resources to GCS.

Under the Hood: The ``ExtendedFileSystem``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``gcsfs`` utilizes the ``ExtendedFileSystem`` class under the hood (implemented in `gcsfs/extended_gcsfs.py <https://github.com/fsspec/gcsfs/blob/main/gcsfs/extended_gcsfs.py>`_).

Importantly, ``ExtendedFileSystem`` is designed to be fully backward-compatible. Before executing directory operations, it automatically identifies the underlying bucket type. If it detects a standard flat-namespace bucket, it routes the request back to standard object-level operations, ensuring your existing buckets continue to work without issue.

The fundamental architectural shift is that ``ExtendedFileSystem`` actively routes directory-level operations to the **GCS Folders grpc API** instead of relying solely on the Objects API.

.. list-table:: Operation Semantics: Flat Namespace vs. HNS
   :widths: 15 40 45
   :header-rows: 1

   * - Operation
     - Flat Namespace (Standard ``gcsfs``)
     - HNS Namespace (``ExtendedFileSystem``)
   * - **``mkdir``**
     - Only used for creating buckets, since GCS Flat namespace doesn't have real directories.
     - Calls the native GCS Folders API, creating physical GCS Folder resource instead of simulating with 0 byte object or object prefix.
   * - **``rmdir``**
     - Primarily used to delete buckets, as directories do not exist as distinct physical entities.
     - Used to delete empty folders natively via the GCS Folders API, in addition to deleting buckets.
   * - **``rm``**
     - Paginates through and individually issues delete requests for every object matching the prefix.
     - Deletes the folder resource and its contents via different delete requests corresponding to folder or file.
   * - **``rename`` / ``mv``**
     - Issues a ``Copy`` request for each object under the prefix, followed by ``Delete``. Non-atomic, ``O(N)``.
     - Triggers a single native metadata-only rename on the folder. **Atomic** and more performant, ``O(1)``, helpful in Checkpointing.
   * - **``info``**
     - Infers directory existence by checking for child objects, returning mocked 0-byte metadata.
     - Uses ``get_folder_metadata`` to explicitly query the Folders API, returning accurate metadata (creation time, resource IDs).

Important Differences to Keep in Mind
-------------------------------------

While ``gcsfs`` aims to abstract the differences via the ``fsspec`` API, you should be aware of standard HNS limitations imposed by the Google Cloud Storage API:

1. **Implicit directories:** In standard GCS, you can create an object ``a/b/c.txt`` without the directories ``a/`` or ``a/b/`` physically existing. In HNS, the parent folder resources must exist (or be created) before the object can be written. ``gcsfs`` handles parent folder creation natively under the hood.
2. **``mkdir`` behavior:** Previously, in a flat namespace, calling ``mkdir`` on a path could only ensure the underlying bucket exists. With HNS enabled, calling ``mkdir`` will create an actual folder resource in GCS. Furthermore, if you want to create nested folders (eg: bucket/a/b/c/d) pass ``create_parents=True``, it will physically create all intermediate folder resources along the specified path.
3. **No mixing or toggling:** You cannot toggle HNS on an existing flat-namespace bucket. You must create a new HNS bucket and migrate your data.
4. **Object naming:** Object names in HNS cannot end with a slash (``/``) unless they are true folder resources.

For more details on managing these buckets, refer to the official documentation for `Hierarchical Namespace <https://cloud.google.com/storage/docs/hns-overview>`_.

Disabling HNS Support
------------------------------

You can disable these features by explicitly setting an environment variable of the same name.

**Code Example**

.. code-block:: bash

    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT=false

**Note:** *The choice of which filesystem class to use is made at import time based on the GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT environment variable, and cannot be controlled via constructor arguments passed to GCSFileSystem (but you can still import each class explicitly, if you desire).*
