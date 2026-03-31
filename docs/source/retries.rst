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

For HNS buckets, ``ExtendedGcsFileSystem`` utilizes the specialized Storage Control client (``StorageControlAsyncClient``) for folder-level operations (e.g., ``mkdir``, ``rename``).

- These calls utilize the underlying Google Cloud Python SDK's default retry behavior. Standard ``gcsfs`` retry logic (``retry_request``) is not applied to these control plane calls.
- **Applicable Methods:**
    - ``get_storage_layout``: Used to determine bucket type.
    - ``create_folder``: Used for ``mkdir``.
    - ``get_folder``: Used for directory metadata and existence checks.
    - ``list_folders``: Used for directory listings (``ls``).
    - ``rename_folder``: Used for moving/renaming directories (``mv``).
- **Non-Retried Methods:** Methods like ``delete_folder`` (used for ``rmdir``) are not retried by default.
- **Retriable Errors:**
    - ``google.api_core.exceptions.DeadlineExceeded``
    - ``google.api_core.exceptions.InternalServerError``
    - ``google.api_core.exceptions.ResourceExhausted``
    - ``google.api_core.exceptions.ServiceUnavailable``
    - ``google.api_core.exceptions.Unknown``
- **Backoff Strategy:** Exponential backoff with ``initial=1.0s``, ``maximum=60.0s``, and ``multiplier=2.0``.
- **Overall Timeout (Deadline):** 60.0s

Rapid Storage (Zonal Buckets)
-----------------------------

For Zonal buckets, ``ZonalFile`` utilizes the specialized gRPC clients (``AsyncMultiRangeDownloader`` for reads and ``AsyncAppendableObjectWriter`` for writes).

- Similar to HNS buckets, control plane operations for Zonal buckets (such as ``get_storage_layout`` or folder operations) utilize the same ``StorageControlAsyncClient`` retry mechanism described in the HNS section above.
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
