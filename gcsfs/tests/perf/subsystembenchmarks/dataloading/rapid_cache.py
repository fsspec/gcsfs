"""Per-case GCS Rapid Cache (Anywhere Cache) lifecycle and warmup helpers."""

import concurrent.futures
import logging
import time

RAPID_CACHE_BUCKET_TYPES = ("rapid_cache_cold", "rapid_cache_warm")
DEFAULT_TIMEOUT_SECONDS = 3600
DEFAULT_POLL_SECONDS = 10
DEFAULT_WARMUP_PASSES = 2
DEFAULT_WARMUP_SETTLE_SECONDS = 65
_PENDING_STATES = ("CREATING", "PROVISIONING", "PENDING", "")


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
            raw = fs.call("GET", f"b/{bucket}/anywhereCaches/{zone}", json_out=True)
            resp = raw if isinstance(raw, dict) else {}
            raw_state = resp.get("state")
            state = str(raw_state if raw_state is not None else "").strip().upper()
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


def warm_if_needed(
    prefix,
    bucket_type,
    *,
    fs=None,
    passes=DEFAULT_WARMUP_PASSES,
    settle_seconds=DEFAULT_WARMUP_SETTLE_SECONDS,
    sleep=time.sleep,
):
    """Read every object under prefix (untimed) and settle when bucket_type is rapid_cache_warm.

    Under high concurrent write load (e.g. 64 workers closing ~95 MiB shards
    simultaneously), ``ingestOnWrite=True`` only finishes admitting ~75% of the
    corpus immediately after ``ingest()`` returns. Running multiple warmup
    passes with a post-pass settle delay ensures asynchronous admit-on-miss
    completes for every shard and separates warmup reads from the timed Cloud
    Monitoring minute bucket. A fresh filesystem instance keeps the warmup's
    listings out of the instance the timed reads use.
    """
    if bucket_type != "rapid_cache_warm" or not str(prefix).startswith("gs://"):
        return 0
    path = prefix
    if fs is None:
        import fsspec

        fs, path = fsspec.core.url_to_fs(prefix, skip_instance_cache=True)
    objects = sorted(obj for obj in fs.find(path) if not obj.endswith("/"))
    if not objects:
        raise RuntimeError(f"no objects found to warm under {prefix!r}")

    def _warm_one(path):
        total = 0
        with fs.open(path, "rb") as f:
            while chunk := f.read(16 * 1024 * 1024):
                total += len(chunk)
        return total

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(16, len(objects))
    ) as pool:
        for _ in range(passes):
            total = sum(pool.map(_warm_one, objects))
            sleep(settle_seconds)
    return total
