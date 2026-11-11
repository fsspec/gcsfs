"""Rapid Cache Ultra (RCU) bucket types.

Each case creates its own bucket (HNS for ``hns_rcu_*``) with an RCU cache in the
run's zone and disables the cache when it ends (bucket.case_bucket). The type's
suffix is the cache state reads start from:

- ``*_rcu_z``: objects staged as RAPID (zonal) by gcsfs's appendable writer.
- ``*_rcu_warm``: objects staged as STANDARD, then admitted by one read each.
- ``*_rcu_cold``: objects staged as STANDARD and never read before timing.
"""

import concurrent.futures
import contextlib
import logging
import os
import time

import fsspec

from gcsfs.retry import HttpError

RCU_BUCKET_TYPES = (
    "standard_rcu_z",
    "standard_rcu_warm",
    "standard_rcu_cold",
    "hns_rcu_z",
    "hns_rcu_warm",
    "hns_rcu_cold",
)
_ZB_HNS_FLAG = "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"
_WARM_READ_BYTES = 2**20


def rcu_state(bucket_type):
    """``"z"``, ``"warm"`` or ``"cold"`` for RCU bucket types, else None."""
    if bucket_type not in RCU_BUCKET_TYPES:
        return None
    return bucket_type.rsplit("_", 1)[1]


def create(fs, bucket, zone):
    """Create the RCU cache (admit-on-first-miss) of this case's bucket in zone.

    gcsfs retries transient errors, so a retried create whose first attempt went
    through reports that the cache already exists, which is fine on the case's bucket.
    """
    try:
        fs.call(
            "POST",
            f"b/{bucket}/rapidCaches",
            json={
                "zone": zone,
                "cacheType": "rapid-cache-ultra",
                "admissionPolicy": "admit-on-first-miss",
            },
            json_out=True,
        )
    except HttpError as exc:
        if exc.code != 409:
            raise


def disable(fs, bucket, zone):
    """Best-effort disable of the RCU cache of bucket in zone."""
    try:
        fs.call("POST", f"b/{bucket}/rapidCaches/{zone}/disable", json_out=True)
    except Exception as exc:
        logging.warning(
            "could not disable the RCU cache on bucket %s (%s): %s", bucket, zone, exc
        )


def _close(fs):
    """Close a fresh gcsfs instance's gRPC clients and HTTP session.

    Logs errors instead of raising them, since it runs in finally blocks.
    """
    if hasattr(fs, "_close_resources"):
        try:
            fsspec.asyn.sync(fs.loop, fs._close_resources)
        except Exception as exc:
            logging.warning("could not close the gcsfs gRPC clients: %s", exc)
    try:
        fs.close_session(fs.loop, fs._session)
    except Exception as exc:
        logging.warning("could not close the gcsfs HTTP session: %s", exc)


def wait_for_rcu_layout(
    bucket, bucket_type, project, *, sleep=time.sleep, clock=time.monotonic
):
    """Wait up to 10 minutes until bucket reports a running RCU cache.

    gcsfs caches a bucket's layout per instance and detects RCU only while the cache
    is RUNNING, so a writer that looked the layout up too early would stage STANDARD
    objects for ``*_rcu_z``. Polls GetStorageLayout every 10 s on a fresh instance,
    then checks the bucket's namespace matches the type (HNS for ``hns_rcu_*``).
    """
    from gcsfs.extended_gcsfs import ExtendedGcsFileSystem

    fs = ExtendedGcsFileSystem(project=project, skip_instance_cache=True)
    try:
        deadline = clock() + 600
        while True:
            fs.invalidate_storage_layout_cache(bucket)
            layout = fs._sync_lookup_bucket_layout(bucket)
            if layout.is_rcu:
                break
            if clock() >= deadline:
                raise TimeoutError(
                    f"bucket {bucket!r} reported no running Rapid Cache Ultra cache "
                    f"after 600 s (last layout: {layout})"
                )
            sleep(10)
    finally:
        _close(fs)
    want_hns = bucket_type.startswith("hns_")
    if layout.is_hierarchical != want_hns:
        raise ValueError(
            f"{bucket_type} needs an {'HNS' if want_hns else 'flat'} RCU bucket, "
            f"but {bucket!r} is {'HNS' if layout.is_hierarchical else 'flat'}"
        )


@contextlib.contextmanager
def http_staging_env(bucket_type):
    """For ``*_rcu_warm`` and ``*_rcu_cold``, make processes spawned in the block stage
    STANDARD objects.

    gcsfs picks ``gcsfs.GCSFileSystem`` (which fsspec's ``gs://`` resolves to) at
    import time: with GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT=false it is the HTTP-only
    ``gcsfs.core.GCSFileSystem``, which uploads STANDARD objects to RCU buckets. The
    harness stages in spawned processes, which copy the environment when they start,
    so its writers need no changes. A no-op for every other bucket type.
    """
    if rcu_state(bucket_type) not in ("warm", "cold"):
        yield
        return
    previous = os.environ.get(_ZB_HNS_FLAG)
    os.environ[_ZB_HNS_FLAG] = "false"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(_ZB_HNS_FLAG, None)
        else:
            os.environ[_ZB_HNS_FLAG] = previous


def assert_storage_class(prefix, bucket_type):
    """Fail unless every object under prefix has the storage class the type stages.

    RAPID for ``*_rcu_z``, STANDARD for warm and cold. Lists metadata only, on a
    fresh HTTP-only instance, so cold objects stay unread.
    """
    state = rcu_state(bucket_type)
    if state is None or not str(prefix).startswith("gs://"):
        return
    from gcsfs.core import GCSFileSystem

    want = "RAPID" if state == "z" else "STANDARD"
    fs = GCSFileSystem(skip_instance_cache=True)
    try:
        listing = fs.find(prefix, detail=True)
    finally:
        _close(fs)
    objects = {name: info for name, info in listing.items() if not name.endswith("/")}
    if not objects:
        raise AssertionError(f"no objects found under {prefix} to check for {want}")
    wrong = sorted(
        f"{name} ({info.get('storageClass')})"
        for name, info in objects.items()
        if info.get("storageClass") != want
    )
    if wrong:
        raise AssertionError(
            f"{len(wrong)} objects under {prefix} are not {want} as {bucket_type} "
            f"needs: {', '.join(wrong[:5])}"
        )


def warm_if_needed(prefix, bucket_type, *, sleep=time.sleep):
    """For ``*_rcu_warm``, get every object under prefix admitted to the cache.

    The cache admits a whole object on its first in-zone read (admit-on-first-miss),
    so this reads the first MiB of each object. It reads through a fresh instance and
    closes it: a read stream opened before admission keeps reading regional storage,
    so the timed reads must not reuse it. Admission has no completion signal, so it
    then sleeps 120 s + 1.5 s per GiB under prefix (a lone 1 GiB object took 77 s, a
    100 GiB one 211 s), or GCSFS_SUBSYSTEM_RCU_WARM_SETTLE_SECONDS if set. The 120 s
    floor also keeps the warm-up reads out of the case's Cloud Monitoring window
    when they are reported a minute late.
    """
    if rcu_state(bucket_type) != "warm" or not str(prefix).startswith("gs://"):
        return
    fs, path = fsspec.core.url_to_fs(prefix, skip_instance_cache=True)
    try:
        sizes = {
            name: info["size"]
            for name, info in fs.find(path, detail=True).items()
            if not name.endswith("/")
        }
        if not sizes:
            raise RuntimeError(f"no objects found to warm under {prefix!r}")
        with concurrent.futures.ThreadPoolExecutor(min(32, len(sizes))) as pool:
            reads = [
                pool.submit(fs.cat_file, name, 0, min(size, _WARM_READ_BYTES))
                for name, size in sizes.items()
            ]
            for read in reads:
                read.result()
    finally:
        _close(fs)
    override = os.environ.get("GCSFS_SUBSYSTEM_RCU_WARM_SETTLE_SECONDS")
    settle = float(override) if override else 120 + 1.5 * sum(sizes.values()) / 2**30
    logging.info(
        "read %d objects under %s; settling %.0f s", len(sizes), prefix, settle
    )
    sleep(settle)
