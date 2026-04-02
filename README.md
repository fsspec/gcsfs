
# gcsfs

[|Build Status|](https://github.com/fsspec/gcsfs/actions)
[|Docs|](https://gcsfs.readthedocs.io/en/latest/?badge=latest)

**GCSFS** is a Python library that provides a familiar, file-system-like interface to [Google Cloud Storage (GCS)](https://docs.cloud.google.com/storage/docs/introduction). Built on top of [fsspec](https://github.com/fsspec), it allows you to interact with cloud buckets as if they were local directories, making it a favorite for data scientists and engineers.

-----

## Getting Started

### Installation

Install via pip or conda:

```bash
# Using pip
pip install gcsfs

# OR using conda
conda install -c conda-forge gcsfs
```

### Basic Usage

```python
import gcsfs

# Initialize the filesystem
fs = gcsfs.GCSFileSystem(project='my-google-project')

# List files in a bucket
files = fs.ls('my-bucket')

# Read a file directly into a string/bytes
with fs.open('my-bucket/data.txt', 'rb') as f:
    content = f.read()
```

-----

## Specialized Bucket Support

GCSFS now automatically supports advanced Google Cloud Storage features through its `ExtendedFileSystem` implementation.

### 1\. Hierarchical Namespace (HNS)

Hierarchical Namespace (HNS) replaces the traditional "flat" GCS structure with true logical directories.

  * **Atomic Renames:** Moving or renaming a directory is an `O(1)` metadata operation. No more slow "copy-then-delete" for large folders.
  * **High Performance:** Offers up to 8x higher initial Queries Per Second (QPS) for read/write operations.
  * **AI/ML Ready:** Ideal for heavy checkpointing and managing millions of small files.

### 2\. Rapid Buckets (Zonal Storage)

Rapid Buckets are zonal storage resources designed for ultra-low latency and maximum throughput.

  * **Zonal Co-location:** Place your data in the same zone as your GPU/TPU clusters to minimize network lag.
  * **True Appends:** Unlike standard GCS objects, you can append data to existing objects in Rapid buckets without a full rewrite.
  * **Streaming I/O:** Optimized for high-speed model loading and real-time logging.

-----

## Integration & Auth

GCSFS plays nicely with the rest of the Python data ecosystem.

### Authentication Modes

  * **Default:** Uses your local gcloud credentials or environment service accounts.
  * **Cloud:** Explicitly use Google Metadata service (`token='cloud'`).
  * **Anonymous:** Access public data without a login (`token='anon'`).
  * **Service Account:** Pass the path to your JSON key file (`token='path/to/key.json'`).

> [\!TIP]
> **Note on Async:** GCSFS is built on `aiohttp`. If you are building high-concurrency applications, you can use the asynchronous API by passing `asynchronous=True` to the `GCSFileSystem` constructor.

-----

## Support

Work on this repository is supported in part by:

**"Anaconda, Inc. - Advancing AI through open source."**
