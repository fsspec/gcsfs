"""Tests for rcu/prepare.py, the Rapid Cache Ultra staging and warm-up
commands that the rcu-stage and rcu-prepare-reads steps run in an in-cluster
Job. A dict-backed fake stands in for gcsfs.GCSFileSystem.
"""

import io

import pytest
from rcu import prepare

MiB = 1 << 20


class FakeGCS:
    """Objects are named "bucket/key", as gcsfs names them, and paths may
    carry the gs:// scheme. find() matches the path itself or anything below
    it, like gcsfs."""

    def __init__(self, objects):
        self.objects = dict(objects)
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


@pytest.fixture
def fake_gcs(monkeypatch):
    def install(objects):
        fs = FakeGCS(objects)
        monkeypatch.setattr(prepare.gcsfs, "GCSFileSystem", lambda **kwargs: fs)
        return fs

    return install


def _under(fs, bucket):
    return {k: v for k, v in fs.objects.items() if k.startswith(bucket + "/")}


def test_stage_rapid_copies_a_bucket_root_dataset(fake_gcs):
    fs = fake_gcs({"src/a.parquet": b"a" * 10, "src/sub/b.parquet": b"b" * 20})

    prepare.main(["stage-rapid", "gs://src", "gs://dst/"])

    assert _under(fs, "dst") == {
        "dst/a.parquet": b"a" * 10,
        "dst/sub/b.parquet": b"b" * 20,
    }


def test_stage_rapid_copies_only_the_dataset_prefix(fake_gcs):
    fs = fake_gcs(
        {
            "src/data/a.parquet": b"a",
            "src/data/sub/b.parquet": b"bb",
            "src/data2/c.parquet": b"c",
        }
    )

    prepare.main(["stage-rapid", "gs://src/data", "gs://dst/data"])

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
        }
    )

    prepare.main(["warm", "gs://data/", "gs://ckpt/seed/run/model.ckpt"])

    assert sorted(fs.reads) == [
        ("ckpt/seed/run/model.ckpt", 0, MiB),
        ("data/a.parquet", 0, 10),
        ("data/sub/b.parquet", 0, MiB),
    ]
