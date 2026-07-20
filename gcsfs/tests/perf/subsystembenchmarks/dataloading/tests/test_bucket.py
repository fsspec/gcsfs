import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading import bucket


def _spec(**over):
    kw = dict(
        prefix="pfx-regional-abcd1234",
        bucket_type="regional",
        project="proj",
        location="us-central1",
    )
    kw.update(over)
    return bucket.BucketSpec(**kw)


class _FakeFS:
    def __init__(self, fail_rm=False):
        self.made, self.removed, self.rmdirs = [], [], []
        self.fail_rm = fail_rm

    def mkdir(self, name, **kwargs):
        self.made.append((name, kwargs))

    def rm(self, path, recursive=False):
        if self.fail_rm:
            raise FileNotFoundError(path)
        self.removed.append(path)

    def rmdir(self, name):
        self.rmdirs.append(name)


def test_regional_body_is_plain():
    assert bucket.bucket_kwargs(_spec()) == {}


def test_hns_body_enables_hns_and_ubla():
    body = bucket.bucket_kwargs(_spec(bucket_type="hns"))
    assert body["hierarchicalNamespace"] == {"enabled": True}
    assert body["iam_configuration"]["uniformBucketLevelAccess"]["enabled"] is True
    assert "customPlacementConfig" not in body


def test_zonal_body_pins_placement_and_rapid():
    body = bucket.bucket_kwargs(_spec(bucket_type="zonal", zone="us-central1-a"))
    assert body["storageClass"] == "RAPID"
    assert body["customPlacementConfig"] == {"dataLocations": ["us-central1-a"]}
    assert body["hierarchicalNamespace"] == {"enabled": True}


def test_zonal_requires_a_zone():
    with pytest.raises(ValueError, match="GCSFS_SUBSYSTEM_ZONE"):
        _spec(bucket_type="zonal").validate()


def test_missing_env_names_the_missing_vars():
    with pytest.raises(ValueError, match="GCSFS_SUBSYSTEM_BUCKET_PREFIX"):
        _spec(prefix="").validate()
    with pytest.raises(ValueError, match="GCSFS_SUBSYSTEM_LOCATION"):
        _spec(location="").validate()


def test_uppercase_prefix_is_rejected():
    with pytest.raises(ValueError, match="must be lowercase"):
        _spec(prefix="Pfx-Region").validate()


def test_from_env_reads_what_run_py_exports(monkeypatch):
    monkeypatch.setenv("GCSFS_SUBSYSTEM_BUCKET_PREFIX", "pfx")
    monkeypatch.setenv("GCSFS_SUBSYSTEM_BUCKET_TYPE", "hns")
    monkeypatch.setenv("GCSFS_SUBSYSTEM_PROJECT", "p")
    monkeypatch.setenv("GCSFS_SUBSYSTEM_LOCATION", "us-central1")
    spec = bucket.BucketSpec.from_env()
    assert (spec.prefix, spec.bucket_type, spec.project) == ("pfx", "hns", "p")


def test_case_bucket_name_is_unique_and_legal():
    a = bucket.case_bucket_name("pfx", "read-hf-ptpq-stream-seq")
    b = bucket.case_bucket_name("pfx", "read-hf-ptpq-stream-seq")
    assert a != b
    assert a.startswith("pfx-read-hf-ptpq-stream-seq-")
    assert 3 <= len(a) <= 63


def test_case_bucket_name_truncates_to_the_gcs_limit():
    name = bucket.case_bucket_name("a" * 40, "read-hf-" + "x" * 60)
    assert len(name) <= 63
    assert not name.startswith("-") and "--" not in name.strip("-")


def test_case_bucket_creates_then_deletes():
    fs = _FakeFS()
    with bucket.case_bucket(_spec(bucket_type="hns"), "read-hf-x", fs=fs) as name:
        assert fs.made == [
            (
                name,
                {
                    "location": "us-central1",
                    **bucket.bucket_kwargs(_spec(bucket_type="hns")),
                },
            )
        ]
        assert fs.removed == []
    assert fs.removed == [f"{name}/"]
    assert fs.rmdirs == [name]


def test_case_bucket_deletes_even_when_the_case_raises():
    fs = _FakeFS()
    with pytest.raises(RuntimeError, match="boom"):
        with bucket.case_bucket(_spec(), "read-hf-x", fs=fs):
            raise RuntimeError("boom")
    assert fs.rmdirs and fs.rmdirs[0].startswith("pfx-regional-abcd1234-read-hf-x-")


def test_empty_bucket_still_gets_dropped():
    fs = _FakeFS(fail_rm=True)
    with bucket.case_bucket(_spec(), "read-hf-x", fs=fs) as name:
        pass
    assert fs.rmdirs == [name]
