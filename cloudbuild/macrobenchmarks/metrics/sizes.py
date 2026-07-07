"""GCS ``du`` sizes for the read-amplification denominators (best-effort)."""

import os
import tempfile

from metrics import schema


def _split_gs_path(gs_path: str):
    """(bucket, prefix) for ``gs://bucket[/prefix]`` (scheme optional)."""
    rest = gs_path[len("gs://") :] if gs_path.startswith("gs://") else gs_path
    bucket_name, _, prefix = rest.partition("/")
    return bucket_name, prefix


def gcs_du(storage_client, gs_path: str):
    """Sum blob sizes under ``gs_path`` (gs://bucket[/prefix]); None if empty."""
    bucket_name, prefix = _split_gs_path(gs_path)
    total = 0
    found = False
    for blob in storage_client.list_blobs(bucket_name, prefix=prefix):
        total += blob.size or 0
        found = True
    return total if found else None


def _hf_num_rows(local_dir: str, filename: str) -> int:
    """Row count of one local shard, format inferred from its extension.

    Delegates to ``datasets`` to stay format-agnostic (parquet, arrow, json,
    csv, webdataset, ...); streaming avoids caching the decoded shard to disk.
    """
    # Force offline: local packaged builders need no Hub access, and a
    # sandboxed metrics step must never block on a Hub round-trip.
    os.environ.setdefault("HF_HUB_OFFLINE", "1")
    os.environ.setdefault("HF_DATASETS_OFFLINE", "1")
    import datasets

    ds = datasets.load_dataset(
        path=local_dir,
        data_files=filename,
        split="train",
        streaming=True,
        cache_dir=local_dir,
    )
    return sum(1 for _ in ds)


def _download_and_count(storage_client, blob) -> int:
    """Download one shard to a temp dir and count its rows via ``datasets``."""
    with tempfile.TemporaryDirectory() as d:
        name = blob.name.rsplit("/", 1)[-1]
        blob.download_to_filename(os.path.join(d, name))
        return _hf_num_rows(d, name)


def _largest_shard(blobs):
    """Largest object among ``blobs`` (None if none have a size).

    Used as the representative shard for per-sample bytes: a small remainder
    shard would over-weight fixed per-file overhead and skew the ratio.
    """
    sized = [b for b in blobs if b.size]
    return max(sized, key=lambda b: b.size) if sized else None


def dataset_du_and_sample_count(
    storage_client, gs_path: str, *, count_shard=_download_and_count
):
    """``(du_bytes, estimated_total_rows)`` from a single bucket listing.

    ``du`` is the summed object size; the row count is ``du / per_sample_bytes``,
    with per-sample bytes measured from one downloaded/parsed shard (the
    largest). Counting is best-effort: on failure ``du`` is still returned and
    only the count is dropped. Both are None when the path holds no objects.
    """
    bucket_name, prefix = _split_gs_path(gs_path)
    blobs = list(storage_client.list_blobs(bucket_name, prefix=prefix))
    if not blobs:
        return None, None
    du = sum(b.size or 0 for b in blobs)
    count = None
    shard = _largest_shard(blobs)
    if du and shard is not None:
        try:
            rows = count_shard(storage_client, shard)
        except Exception as e:  # noqa: BLE001 - best-effort metric
            print(f"Warning: dataset sample count failed, ratio N/A: {e}")
            rows = None
        if rows:
            count = round(du / (shard.size / rows))
    return du, count


def restored_checkpoint_location(restore_rows: list):
    """checkpoint_location of the earliest-ending restore (the resume), or None."""
    candidates = [
        r
        for r in restore_rows
        if r.get("checkpoint_location") and r.get("end_time") is not None
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda r: r["end_time"])["checkpoint_location"]


def size_rows(
    storage_client,
    *,
    dataset_bucket,
    restored_location,
    count_shard=_download_and_count,
) -> list:
    """SystemMetric rows for the dataset-bucket size and restored-checkpoint size."""
    rows = []
    if dataset_bucket:
        du, count = dataset_du_and_sample_count(
            storage_client, f"gs://{dataset_bucket}", count_shard=count_shard
        )
        if du is not None:
            rows.append(
                schema.SystemMetric(
                    pod_name=dataset_bucket,
                    metric="dataset_size_bytes",
                    peak=du,
                    mean=None,
                )
            )
            if count is not None:
                rows.append(
                    schema.SystemMetric(
                        pod_name=dataset_bucket,
                        metric="dataset_sample_count",
                        peak=count,
                        mean=None,
                    )
                )
    if restored_location:
        size = gcs_du(storage_client, restored_location)
        if size is not None:
            rows.append(
                schema.SystemMetric(
                    pod_name=restored_location,
                    metric="checkpoint_restored_bytes",
                    peak=size,
                    mean=None,
                )
            )
    return rows
