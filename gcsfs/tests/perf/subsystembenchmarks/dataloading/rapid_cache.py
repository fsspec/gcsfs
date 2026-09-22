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
    return int(
        os.environ.get(
            "GCSFS_SUBSYSTEM_RAPID_CACHE_TIMEOUT", str(DEFAULT_TIMEOUT_SECONDS)
        )
    )


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
    deadline = clock() + timeout
    last_state = "UNKNOWN"
    while True:
        resp = fs.call("GET", f"b/{bucket}/anywhereCaches/{zone}", json_out=True) or {}
        state = str(resp.get("state", "")).upper()
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

        fs = gcsfs.GCSFileSystem()
    objects = sorted(fs.find(prefix))
    if not objects:
        raise RuntimeError(f"no objects found to warm under {prefix!r}")
    total_bytes = 0
    for obj in objects:
        url = obj if str(obj).startswith("gs://") else f"gs://{obj}"
        total_bytes += len(fs.cat_file(url))
    return total_bytes
