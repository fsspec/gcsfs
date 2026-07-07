from types import SimpleNamespace

from metrics import sizes


class _FakeStorage:
    """list_blobs(bucket, prefix) over a canned {bucket: [(name, size), ...]}."""

    def __init__(self, by_bucket):
        self.by_bucket = by_bucket

    def list_blobs(self, bucket_name, prefix=""):
        for name, size in self.by_bucket.get(bucket_name, []):
            if name.startswith(prefix):
                yield SimpleNamespace(name=name, size=size)


def test_gcs_du_sums_under_prefix():
    client = _FakeStorage(
        {"b": [("checkpoints/a", 100), ("checkpoints/c", 50), ("other", 7)]}
    )
    assert sizes.gcs_du(client, "gs://b/checkpoints") == 150
    assert sizes.gcs_du(client, "gs://b") == 157


def test_gcs_du_none_when_no_match():
    client = _FakeStorage({"b": []})
    assert sizes.gcs_du(client, "gs://b/missing") is None


def test_restored_checkpoint_location_picks_earliest_end():
    rows = [
        {"checkpoint_location": "gs://c/step200", "end_time": 200.0},
        {"checkpoint_location": "gs://c/step100", "end_time": 100.0},
        {"checkpoint_location": None, "end_time": 5.0},
    ]
    assert sizes.restored_checkpoint_location(rows) == "gs://c/step100"
    assert sizes.restored_checkpoint_location([]) is None


def test_dataset_du_and_sample_count_uses_largest_shard():
    # du and count come from a single listing. The largest object is the
    # representative shard (its bytes/row best matches the du-weighted average);
    # a tiny sidecar must not be chosen.
    client = _FakeStorage(
        {
            "ds": [
                ("train/_meta.json", 10),
                ("train/a.parquet", 990),
                ("train/big.parquet", 2000),  # largest
            ]
        }
    )

    def count_shard(storage_client, blob):
        assert blob.name == "train/big.parquet"  # largest, not the sidecar
        return 200  # 2000 bytes / 200 rows -> 10 bytes/sample

    du, count = sizes.dataset_du_and_sample_count(
        client, "gs://ds", count_shard=count_shard
    )
    assert du == 3000  # 10 + 990 + 2000
    assert count == 300  # du 3000 / 10 bytes-per-sample


def test_dataset_du_and_sample_count_none_when_no_objects():
    client = _FakeStorage({"ds": []})
    assert sizes.dataset_du_and_sample_count(
        client, "gs://ds", count_shard=lambda c, b: 5
    ) == (None, None)


def test_dataset_du_and_sample_count_du_survives_count_failure():
    # Counting a shard (download + parse) is best-effort: on failure du is still
    # returned so dataset_size_bytes is preserved; only the count is dropped.
    client = _FakeStorage({"ds": [("a.parquet", 1000)]})

    def boom(storage_client, blob):
        raise RuntimeError("unparseable shard")

    du, count = sizes.dataset_du_and_sample_count(client, "gs://ds", count_shard=boom)
    assert du == 1000
    assert count is None


def test_dataset_du_and_sample_count_no_count_when_shard_uncountable():
    client = _FakeStorage({"ds": [("a.parquet", 100)]})
    du, count = sizes.dataset_du_and_sample_count(
        client, "gs://ds", count_shard=lambda c, b: 0
    )
    assert du == 100
    assert count is None


def test_size_rows_emits_dataset_size_sample_count_and_checkpoint():
    client = _FakeStorage(
        {
            "ds": [("train/0.parquet", 1000)],
            "ckpt": [("checkpoints/step100/shard0", 400)],
        }
    )
    rows = sizes.size_rows(
        client,
        dataset_bucket="ds",
        restored_location="gs://ckpt/checkpoints/step100",
        count_shard=lambda c, b: 100,  # 1000 bytes / 100 rows -> 10 bytes/sample
    )
    by_metric = {r.metric: r for r in rows}
    assert by_metric["dataset_size_bytes"].peak == 1000
    assert by_metric["dataset_sample_count"].peak == 100  # du 1000 / 10 per sample
    assert by_metric["checkpoint_restored_bytes"].peak == 400


def test_size_rows_skips_missing_inputs():
    client = _FakeStorage({"ds": []})  # dataset du -> None
    rows = sizes.size_rows(client, dataset_bucket="ds", restored_location=None)
    assert rows == []
