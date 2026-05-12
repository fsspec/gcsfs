Retries
=======

``gcsfs`` implements retry logic to handle transient errors and improve the reliability of operations against Google Cloud Storage.

Default Retry Implementation (Standard Buckets)
-----------------------------------------------

For standard buckets, ``gcsfs`` uses a custom retry decorator (``retry_request``) for most HTTP requests. Since most high-level operations utilize this decorator internally, they benefit from the retry logic.

- **Applicable Methods:**
    - ``ls`` / ``_ls``: Listing objects and prefixes.
    - ``info`` / ``_info``: Retrieving object metadata.
    - ``cat`` / ``_cat_file``: Reading object contents.
    - ``get`` / ``_get_file``: Downloading objects.
    - ``put`` / ``_put_file``: Uploading objects (including resumable uploads).
    - ``mkdir`` / ``_mkdir``: Creating buckets.
    - ``rm`` / ``_rm_file``: Deleting objects.
    - ``mv`` / ``_mv_file``: Moving/renaming objects.
    - ``cp`` / ``_cp_file``: Copying objects.
- **Number of Retries:** The default number of retries is **6**. This is defined as a class attribute ``retries = 6`` in ``GCSFileSystem``.
- **Timeouts:** Individual requests use ``requests_timeout`` (if configured in ``GCSFileSystem.__init__``) as their timeout. There is no total deadline for the retry loop in ``retry_request``; it will attempt up to the specified number of retries (default 6) irrespective of the total time taken.
- **Backoff Strategy:** Exponential backoff with jitter. The wait time between retries is calculated as ``min(random.random() + 2 ** (retry - 1), 32)``.
- **Retriable Errors:**
    - ``requests.exceptions.ChunkedEncodingError``
    - ``requests.exceptions.ConnectionError``
    - ``requests.exceptions.ReadTimeout``
    - ``requests.exceptions.Timeout``
    - ``requests.exceptions.ProxyError``
    - ``requests.exceptions.SSLError``
    - ``requests.exceptions.ContentDecodingError``
    - ``google.auth.exceptions.RefreshError``
    - ``aiohttp.client_exceptions.ClientError``
    - ``ChecksumError``
    - HTTP status codes: 500-504, 408, 429.
    - HTTP status code 401 with "Invalid Credentials" message (auth expiration).

Hierarchical Namespace (HNS) Buckets
------------------------------------

For HNS buckets, ``ExtendedGcsFileSystem`` utilizes the specialized Storage Control client (``StorageControlAsyncClient``) for control plane operations (e.g., ``mkdir``, ``rename``, ``get_storage_layout``).

- These calls utilize retry configuration based on ``google.api_core.retry.AsyncRetry``.
- **Applicable Methods:**
    - ``get_storage_layout``: Used to determine bucket type.
    - ``create_folder``: Used for ``mkdir``.
    - ``get_folder``: Used for directory metadata and existence checks.
    - ``list_folders``: Used for directory listings (``ls``).
    - ``rename_folder``: Used for moving/renaming directories (``mv``).
    - ``delete_folder``: Used for deleting directories (``rmdir``, ``rm -r``).
- **Retriable Errors:**
    - ``google.api_core.exceptions.DeadlineExceeded``
    - ``google.api_core.exceptions.ServiceUnavailable``
    - ``google.api_core.exceptions.InternalServerError``
    - ``google.api_core.exceptions.TooManyRequests``
    - ``google.api_core.exceptions.ResourceExhausted``
    - ``google.api_core.exceptions.Unknown``
    - ``google.api_core.exceptions.Unauthenticated`` (when "Invalid Credentials" is in the message).

- **Configuration:**
  The retry behavior can be customized via the following parameters passed to the FileSystem instance:

  - ``retry_timeout`` (float): The total deadline for the retry loop in seconds. Default: ``60.0``.
  - ``retry_initial`` (float): The initial delay between retries in seconds. Default: ``1.0``.
  - ``retry_maximum`` (float): The maximum delay between retries in seconds. Default: ``60.0``.
  - ``retry_multiplier`` (float): The multiplier applied to the delay after each retry. Default: ``2.0``.

  Per-attempt timeout is controlled by an internal ``STORAGE_CONTROL_RPC_TIMEOUT`` constant, currently set to ``30.0s``.

Configuring Retries via fsspec
------------------------------

Since ``gcsfs`` integrates with the ``fsspec`` configuration system, these retry parameters can be set using ``fsspec`` `configuration files or environment variables <https://filesystem-spec.readthedocs.io/en/latest/features.html#configuration>`_

These settings will be automatically picked up by any ``GCSFileSystem`` instance when experimental HNS support is enabled (which is the default).

Rapid Storage (Zonal Buckets)
-----------------------------

For Zonal buckets, ``ZonalFile`` utilizes the specialized gRPC clients (``AsyncMultiRangeDownloader`` for reads and ``AsyncAppendableObjectWriter`` for writes).

- Similar to HNS buckets, control plane operations for Zonal buckets (such as ``get_storage_layout`` or folder operations) utilize the same Storage Control retry mechanism described in the **Storage Control API** section above.
- File read/write operations (data plane) for Zonal buckets utilize the underlying Google Cloud Python SDK's default retry behavior for gRPC streams. Standard ``gcsfs`` retry logic (``retry_request``) is not applied to these data plane calls.
- **AsyncMultiRangeDownloader (MRD) Retries (Reads):**
    - **Applicable Methods:**
        - ``open``: Establishes the initial gRPC stream.
        - ``download_ranges``: Fetches multiple byte ranges in a single request.
    - **Retriable Errors:** ``InternalServerError``, ``ServiceUnavailable``, ``DeadlineExceeded``, ``TooManyRequests`` (429), and ``Aborted`` (allowing the download to resume from the last successful byte offset without re-transferring data).
    - **Backoff Strategy:** Exponential backoff with ``initial=1.0s``, ``maximum=60.0s``, and ``multiplier=2.0``.
    - **Overall Timeout (Deadline):** 120.0s.
- **AsyncAppendableObjectWriter (AAOW) Retries (Writes):**
    - **Applicable Methods:**
        - ``open``: Establishes the initial bidirectional gRPC stream.
        - ``append``: Streams data to the object.
    - **Methods without Automatic Retries:** ``flush`` and ``finalize`` do not have automatic retry logic in the underlying client.
    - **Retriable Errors:** ``InternalServerError``, ``ServiceUnavailable``, ``DeadlineExceeded``, ``TooManyRequests`` (429), and ``BidiWriteObjectRedirectedError`` (handled by re-opening the stream and resuming from the last persisted offset).
    - **Backoff Strategy:** Exponential backoff with ``initial=1.0s``, ``maximum=60.0s``, and ``multiplier=2.0``.
    - **Overall Timeout (Deadline):** 120.0s.
