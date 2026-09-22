"""Per-case benchmark bucket lifecycle.

Provisions an isolated bucket per case via gcsfs so Cloud Monitoring metrics (sampled on a 60s
bucket-level grid) accurately partition per case window. Teardown deletes the bucket after use.
"""

import contextlib
import logging
import os
import re
import uuid
from dataclasses import dataclass

from gcsfs.tests.perf.subsystembenchmarks.dataloading import rapid_cache

BUCKET_TYPES = ("regional", "zonal", "hns", *rapid_cache.RAPID_CACHE_BUCKET_TYPES)
_MAX_BUCKET_NAME = 63
_SUFFIX_LEN = 8


def bucket_name_of(prefix):
    """Extract GCS bucket name from a gs:// prefix, or "" if non-GCS."""
    if not prefix.startswith("gs://"):
        return ""
    return prefix[len("gs://") :].split("/", 1)[0]


@dataclass(frozen=True)
class BucketSpec:
    """How to create this run's per-case buckets (exported by run.py from its CLI args)."""

    prefix: str
    bucket_type: str
    project: str
    location: str
    zone: str = ""

    @classmethod
    def from_env(cls):
        spec = cls(
            prefix=os.environ.get("GCSFS_SUBSYSTEM_BUCKET_PREFIX", ""),
            bucket_type=os.environ.get("GCSFS_SUBSYSTEM_BUCKET_TYPE", "regional"),
            project=os.environ.get("GCSFS_SUBSYSTEM_PROJECT", ""),
            location=os.environ.get("GCSFS_SUBSYSTEM_LOCATION", ""),
            zone=os.environ.get("GCSFS_SUBSYSTEM_ZONE", ""),
        )
        spec.validate()
        return spec

    def validate(self):
        missing = [
            name
            for name, value in (
                ("GCSFS_SUBSYSTEM_BUCKET_PREFIX", self.prefix),
                ("GCSFS_SUBSYSTEM_PROJECT", self.project),
                ("GCSFS_SUBSYSTEM_LOCATION", self.location),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                f"{', '.join(missing)} unset -- run.py exports these from its CLI args; "
                "the read benchmarks create a bucket per case and cannot run without them"
            )
        if self.bucket_type not in BUCKET_TYPES:
            raise ValueError(
                f"unknown bucket_type {self.bucket_type!r}; expected one of {BUCKET_TYPES}"
            )
        if (
            self.bucket_type in ("zonal", *rapid_cache.RAPID_CACHE_BUCKET_TYPES)
            and not self.zone
        ):
            raise ValueError(
                f"{self.bucket_type} buckets need GCSFS_SUBSYSTEM_ZONE (the placement zone)"
            )
        if self.prefix != self.prefix.lower():
            # Bucket prefix must be lowercase (GCS bucket names cannot contain uppercase).
            raise ValueError(
                f"GCSFS_SUBSYSTEM_BUCKET_PREFIX {self.prefix!r} must be lowercase "
                "(GCS bucket names cannot contain uppercase letters)"
            )


def bucket_kwargs(spec):
    """buckets.insert body for this bucket type (gcsfs.mkdir forwards these verbatim)."""
    if spec.bucket_type in ("regional", *rapid_cache.RAPID_CACHE_BUCKET_TYPES):
        return {}
    body = {
        # HNS requires uniform bucket-level access.
        "iam_configuration": {"uniformBucketLevelAccess": {"enabled": True}},
        "hierarchicalNamespace": {"enabled": True},
    }
    if spec.bucket_type == "zonal":
        body["storageClass"] = "RAPID"
        body["customPlacementConfig"] = {"dataLocations": [spec.zone]}
    return body


def case_bucket_name(prefix, case_id):
    """`<prefix>-<case-slug>-<rand>`, always a legal (<=63 char, lowercase) bucket name."""
    slug = re.sub(r"[^a-z0-9-]+", "-", case_id.lower()).strip("-")
    suffix = uuid.uuid4().hex[:_SUFFIX_LEN]
    head = f"{prefix}-{slug}"[: _MAX_BUCKET_NAME - _SUFFIX_LEN - 1].rstrip("-")
    return f"{head}-{suffix}"


def _delete(fs, name, spec=None):
    """Best-effort teardown; cloudbuild sweeps the prefix at the end as the safety net."""
    if spec is not None and rapid_cache.is_rapid_cache_bucket_type(spec.bucket_type):
        rapid_cache.disable(fs, name, spec.zone)
    try:
        fs.rm(f"{name}/", recursive=True)
    except FileNotFoundError:
        pass
    except Exception as exc:
        logging.warning("could not empty benchmark bucket %s: %s", name, exc)
    with contextlib.suppress(Exception):
        fs.rmdir(name)


@contextlib.contextmanager
def case_bucket(spec, case_id, *, fs=None):
    """Create this case's bucket, yield its corpus prefix, delete it on the way out."""
    if fs is None:
        import gcsfs

        fs = gcsfs.GCSFileSystem(project=spec.project)
    name = case_bucket_name(spec.prefix, case_id)
    fs.mkdir(name, location=spec.location, **bucket_kwargs(spec))
    try:
        if rapid_cache.is_rapid_cache_bucket_type(spec.bucket_type):
            rapid_cache.create(
                fs,
                name,
                spec.zone,
                ingest_on_write=rapid_cache.ingest_on_write_for(spec.bucket_type),
            )
            rapid_cache.wait_running(
                fs,
                name,
                spec.zone,
                timeout=rapid_cache.timeout_from_env(),
            )
        yield f"gs://{name}/data/"
    finally:
        _delete(fs, name, spec=spec)


def local_case_bucket(root):
    """Local-directory stand-in for case_bucket for runs without GCS."""

    @contextlib.contextmanager
    def ctx(case_id):
        yield os.path.join(str(root).rstrip("/"), case_id) + "/data/"

    return ctx
