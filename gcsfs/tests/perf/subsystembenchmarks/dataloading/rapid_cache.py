"""Per-case GCS Rapid Cache (Anywhere Cache) lifecycle and warmup helpers."""

import logging
import os
import time

RAPID_CACHE_BUCKET_TYPES = ("rapid_cache_cold", "rapid_cache_warm")
DEFAULT_TIMEOUT_SECONDS = 1800
DEFAULT_POLL_SECONDS = 10
_PENDING_STATES = ("CREATING", "PROVISIONING", "")


def is_rapid_cache_bucket_type(bucket_type):
    """Return True if bucket_type provisions a per-case Rapid Cache."""
    return bucket_type in RAPID_CACHE_BUCKET_TYPES


def ingest_on_write_for(bucket_type):
    """Return True if the Rapid Cache should enable ingestOnWrite."""
    if bucket_type == "rapid_cache_warm":
        return True
    if bucket_type == "rapid_cache_cold":
        return False
    raise ValueError(f"not a Rapid Cache bucket_type: {bucket_type!r}")


def timeout_from_env():
    """Return Rapid Cache creation timeout in seconds from env or default."""
    raw = os.environ.get(
        "GCSFS_SUBSYSTEM_RAPID_CACHE_TIMEOUT", str(DEFAULT_TIMEOUT_SECONDS)
    )
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"GCSFS_SUBSYSTEM_RAPID_CACHE_TIMEOUT must be a positive integer, got {raw!r}"
        ) from exc
    if value <= 0:
        raise ValueError(
            f"GCSFS_SUBSYSTEM_RAPID_CACHE_TIMEOUT must be > 0, got {value}"
        )
    return value


def create(fs, bucket, zone, *, ingest_on_write):
    """Initiate Rapid Cache creation on bucket in zone via GCS JSON API."""
    return fs.call(
        "POST",
        f"b/{bucket}/anywhereCaches",
        json={"zone": zone, "ingestOnWrite": bool(ingest_on_write)},
        json_out=True,
    )


def wait_running(
    fs,
    bucket,
    zone,
    *,
    timeout=DEFAULT_TIMEOUT_SECONDS,
    poll=DEFAULT_POLL_SECONDS,
    sleep=time.sleep,
    clock=time.monotonic,
):
    """Poll GET anywhereCaches/{zone} until state == 'RUNNING'."""
    if timeout <= 0:
        raise ValueError(f"timeout must be > 0, got {timeout}")
    if poll <= 0:
        raise ValueError(f"poll must be > 0, got {poll}")
    deadline = clock() + timeout
    last_state = "UNKNOWN"
    while True:
        try:
            resp = (
                fs.call("GET", f"b/{bucket}/anywhereCaches/{zone}", json_out=True)
                or {}
            )
            state = str(resp.get("state", "")).upper()
        except FileNotFoundError:
            resp = {}
            state = "CREATING"
            last_state = "NOT_FOUND"
        else:
            last_state = state or last_state
        if state == "RUNNING":
            return resp
        if state not in _PENDING_STATES:
            raise RuntimeError(
                f"Rapid Cache for bucket {bucket!r} in zone {zone!r} entered "
                f"unexpected state {state!r}: {resp}"
            )
        if clock() >= deadline:
            raise TimeoutError(
                f"Timed out after {timeout}s waiting for Rapid Cache on bucket "
                f"{bucket!r} in zone {zone!r} to reach RUNNING (last state: {last_state!r})"
            )
        sleep(poll)


def disable(fs, bucket, zone):
    """Best-effort disable of the Rapid Cache on bucket in zone."""
    try:
        fs.call("POST", f"b/{bucket}/anywhereCaches/{zone}/disable", json_out=True)
    except Exception as exc:
        logging.warning(
            "could not disable Rapid Cache on bucket %s (%s): %s", bucket, zone, exc
        )


def warm_if_needed(prefix, bucket_type, *, fs=None):
    """Read every object under prefix once (untimed) when bucket_type is rapid_cache_warm."""
    if bucket_type != "rapid_cache_warm" or not str(prefix).startswith("gs://"):
        return 0
    if fs is None:
        import gcsfs

        fs = gcsfs.GCSFileSystem(skip_instance_cache=True)
    protocols = getattr(fs, "protocol", ("gs", "gcs"))
    if isinstance(protocols, str):
        protocols = (protocols,)
    uses_gs_protocol = "gs" in protocols or "gcs" in protocols
    find_target = prefix if uses_gs_protocol else str(prefix)[len("gs://") :]
    objects = sorted(
        obj
        for obj in fs.find(find_target)
        if not str(obj).endswith("/")
        and not (hasattr(fs, "isdir") and fs.isdir(obj))
    )
    if not objects:
        raise RuntimeError(f"no objects found to warm under {prefix!r}")

    import concurrent.futures

    def _warm_one(obj):
        if uses_gs_protocol:
            url = obj if str(obj).startswith("gs://") else f"gs://{obj}"
        else:
            url = obj
        if hasattr(fs, "open"):
            total = 0
            with fs.open(url, "rb") as f:
                while chunk := f.read(16 * 1024 * 1024):
                    total += len(chunk)
            return total
        return len(fs.cat_file(url))

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(16, len(objects))
    ) as pool:
        total_bytes = sum(pool.map(_warm_one, objects))
    if hasattr(fs, "invalidate_cache"):
        fs.invalidate_cache()
    return total_bytes

