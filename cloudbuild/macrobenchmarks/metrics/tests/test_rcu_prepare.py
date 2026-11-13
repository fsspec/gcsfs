"""Tests for rcu/prepare.py, the Rapid Cache Ultra staging, check and warm-up
commands that the rcu-stage and rcu-prepare-reads steps run in an in-cluster
Job. A dict-backed fake stands in for gcsfs.GCSFileSystem.
"""

import io
from types import SimpleNamespace

import pytest
from rcu import prepare

MiB = 1 << 20
RCU = SimpleNamespace(is_rcu=True)
NOT_RCU = SimpleNamespace(is_rcu=False)


class FakeGCS:
    """Objects are named "bucket/key", as gcsfs names them, and paths may
    carry the gs:// scheme. find() matches the path itself or anything below
    it, like gcsfs. layouts maps a bucket name to its bucket layout."""

    def __init__(self, objects, layouts=None):
        self.objects = dict(objects)
        self.layouts = dict(layouts or {})
        self.reads = []

    @staticmethod
    def _name(path):
        return path.removeprefix("gs://").rstrip("/")

    def find(self, path, detail=False):
        root = self._name(path)
        infos = {
            name: {
                "name": name,
                "size": len(data),
                "type": "file",
                "storageClass": "STANDARD",
            }
            for name, data in self.objects.items()
            if name == root or name.startswith(root + "/")
        }
        return infos if detail else sorted(infos)

    def cat_file(self, path, start=None, end=None):
        self.reads.append((self._name(path), start, end))
        return self.objects[self._name(path)][start:end]

    def open(self, path, mode="rb"):
        name = self._name(path)
        if mode == "rb":
            return io.BytesIO(self.objects[name])
        objects = self.objects

        class Writer(io.BytesIO):
            def close(self):
                objects[name] = self.getvalue()
                super().close()

        return Writer()

    def split_path(self, path):
        bucket, _, key = self._name(path).partition("/")
        return bucket, key, None

    def _sync_lookup_bucket_layout(self, bucket):
        return self.layouts[bucket]


@pytest.fixture
def fake_gcs(monkeypatch):
    def install(objects, layouts=None):
        fs = FakeGCS(objects, layouts)

        def make_fs(**kwargs):
            fs.init_kwargs = kwargs
            return fs

        monkeypatch.setattr(prepare.gcsfs, "GCSFileSystem", make_fs)
        return fs

    return install


def _under(fs, bucket):
    return {k: v for k, v in fs.objects.items() if k.startswith(bucket + "/")}


def test_stage_rapid_copies_a_bucket_root_dataset(fake_gcs):
    fs = fake_gcs({"src/a.parquet": b"a" * 10, "src/sub/b.parquet": b"b" * 20})

    prepare.main(["stage-rapid", "gs://src", "gs://dst/"])

    assert fs.init_kwargs == {"rcu_rapid_write": True}
    assert _under(fs, "dst") == {
        "dst/a.parquet": b"a" * 10,
        "dst/sub/b.parquet": b"b" * 20,
    }


def test_stage_rapid_copies_only_the_dataset_prefix(fake_gcs):
    fs = fake_gcs(
        {
            # Folder placeholders (zero-byte, name ends in /) are not copied.
            "src/data/": b"",
            "src/data/a.parquet": b"a",
            "src/data/sub/": b"",
            "src/data/sub/b.parquet": b"bb",
            "src/data2/c.parquet": b"c",
        }
    )

    prepare.main(["stage-rapid", "gs://src/data", "gs://dst/data"])

    assert fs.init_kwargs == {"rcu_rapid_write": True}
    assert _under(fs, "dst") == {
        "dst/data/a.parquet": b"a",
        "dst/data/sub/b.parquet": b"bb",
    }


def test_warm_reads_at_most_one_mib_of_every_object(fake_gcs):
    fs = fake_gcs(
        {
            "data/a.parquet": b"x" * 10,
            "data/sub/b.parquet": b"y" * (3 * MiB),
            "data/empty": b"",
            "ckpt/seed/run/model.ckpt": b"z" * (MiB + 1),
            "ckpt/other/unrelated": b"w" * 5,
        },
        layouts={"data": RCU, "ckpt": RCU},
    )

    prepare.main(["warm", "gs://data/", "gs://ckpt/seed/run/model.ckpt"])

    assert fs.init_kwargs == {}
    assert sorted(fs.reads) == [
        ("ckpt/seed/run/model.ckpt", 0, MiB),
        ("data/a.parquet", 0, 10),
        ("data/sub/b.parquet", 0, MiB),
    ]


def test_warm_fails_before_reading_when_a_bucket_is_not_rcu(fake_gcs):
    fs = fake_gcs({"data/a.parquet": b"x"}, layouts={"data": NOT_RCU})

    with pytest.raises(SystemExit, match="data"):
        prepare.main(["warm", "gs://data/"])

    assert fs.reads == []


def test_check_passes_when_every_bucket_is_rcu(fake_gcs):
    # The fake raises KeyError for any other bucket name.
    fs = fake_gcs({}, layouts={"data": RCU, "ckpt": RCU})

    prepare.main(["check", "gs://data/parquet/", "gs://ckpt"])

    assert fs.init_kwargs == {}


def test_check_fails_when_a_bucket_is_not_rcu(fake_gcs):
    fake_gcs({}, layouts={"data": RCU, "ckpt": NOT_RCU})

    with pytest.raises(SystemExit, match="ckpt"):
        prepare.main(["check", "gs://data/parquet/", "gs://ckpt"])


def test_check_fails_when_gcsfs_has_no_bucket_layout_lookup(fake_gcs, monkeypatch):
    fake_gcs({})
    monkeypatch.delattr(FakeGCS, "_sync_lookup_bucket_layout")

    with pytest.raises(SystemExit, match="no Rapid Cache Ultra support"):
        prepare.main(["check", "gs://data/parquet/", "gs://ckpt"])
