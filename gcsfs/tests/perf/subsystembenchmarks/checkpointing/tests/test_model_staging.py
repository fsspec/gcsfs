import json
import os

import pytest

from gcsfs.tests.perf.subsystembenchmarks.checkpointing import model_staging


class _FakeFS:
    def __init__(self, entries, files):
        self._entries = entries
        self._files = files
        self.ls_calls = []
        self.get_calls = []

    def ls(self, prefix, detail=False):
        self.ls_calls.append((prefix, detail))
        return list(self._entries)

    def get(self, rpaths, lpaths):
        self.get_calls.append((list(rpaths), list(lpaths)))
        for rpath, lpath in zip(rpaths, lpaths):
            with open(lpath, "w") as f:
                f.write(self._files[rpath])


def test_model_dir_resolves_gcs_to_tmp_and_passes_through_local():
    assert (
        model_staging.model_dir("gs://bucket/path/Llama-3.1-8B/") == "/tmp/Llama-3.1-8B"
    )
    assert model_staging.model_dir("gs://bucket/gpt2") == "/tmp/gpt2"
    assert model_staging.model_dir("/local/models/gpt2") == "/local/models/gpt2"
    assert model_staging.model_dir("gpt2") == "gpt2"


@pytest.mark.parametrize(
    "bad_id",
    ["gs://", "gs:///", "gs://bucket/.", "gs://bucket/..", "gs://bucket/sub/.."],
)
def test_model_dir_rejects_traversal_and_empty_names(bad_id):
    with pytest.raises(ValueError, match="Invalid GCS model_id"):
        model_staging.model_dir(bad_id)


def test_stage_model_downloads_atomically_via_temp_dir_and_replace(
    tmp_path, monkeypatch
):
    target = tmp_path / "Llama-3.1-8B"
    monkeypatch.setattr(model_staging, "model_dir", lambda mid: str(target))
    entries = [
        {"name": "bucket/Llama-3.1-8B/", "type": "directory"},
        {"name": "bucket/Llama-3.1-8B/config.json", "type": "file"},
        {"name": "bucket/Llama-3.1-8B/model.safetensors", "type": "file"},
    ]
    fake_fs = _FakeFS(
        entries,
        {
            "bucket/Llama-3.1-8B/config.json": "{}",
            "bucket/Llama-3.1-8B/model.safetensors": "weights",
        },
    )
    monkeypatch.setattr(model_staging.gcsfs, "GCSFileSystem", lambda: fake_fs)

    replace_calls = []
    real_replace = os.replace

    def spy_replace(src, dst):
        replace_calls.append((src, dst))
        return real_replace(src, dst)

    monkeypatch.setattr(model_staging.os, "replace", spy_replace)

    out = model_staging.stage_model("gs://bucket/Llama-3.1-8B")
    assert out == str(target)
    assert fake_fs.ls_calls == [("bucket/Llama-3.1-8B", True)]
    assert len(fake_fs.get_calls) == 1
    assert len(replace_calls) == 1
    src, dst = replace_calls[0]
    assert os.path.basename(src).startswith(".Llama-3.1-8B.")
    assert dst == str(target)
    assert not os.path.exists(src)
    assert (target / "config.json").read_text() == "{}"
    assert (target / "model.safetensors").read_text() == "weights"

    # Already-staged directory is a no-op.
    assert model_staging.stage_model("gs://bucket/Llama-3.1-8B") == str(target)
    assert len(fake_fs.ls_calls) == 1


@pytest.mark.parametrize("bad_key", ["bucket/model/..", "bucket/model/."])
def test_stage_model_rejects_dot_and_dotdot_remote_keys(tmp_path, monkeypatch, bad_key):
    target = tmp_path / "model"
    monkeypatch.setattr(model_staging, "model_dir", lambda mid: str(target))
    fake_fs = _FakeFS([{"name": bad_key, "type": "file"}], {})
    monkeypatch.setattr(model_staging.gcsfs, "GCSFileSystem", lambda: fake_fs)

    with pytest.raises(ValueError, match="Invalid remote object key"):
        model_staging.stage_model("gs://bucket/model")

    assert not target.exists()
    assert list(tmp_path.iterdir()) == []


def test_stage_model_rejects_commonpath_escape(tmp_path, monkeypatch):
    target = tmp_path / "model"
    monkeypatch.setattr(model_staging, "model_dir", lambda mid: str(target))
    fake_fs = _FakeFS([{"name": "bucket/model/config.json", "type": "file"}], {})
    monkeypatch.setattr(model_staging.gcsfs, "GCSFileSystem", lambda: fake_fs)
    outside = str(tmp_path / "escaped")
    real_realpath = os.path.realpath

    def fake_realpath(p):
        if p.endswith("config.json"):
            return outside
        return real_realpath(p)

    monkeypatch.setattr(model_staging.os.path, "realpath", fake_realpath)

    with pytest.raises(ValueError, match="Refusing to write outside"):
        model_staging.stage_model("gs://bucket/model")

    assert list(tmp_path.iterdir()) == []


def test_stage_model_restages_incomplete_sharded_safetensors(tmp_path, monkeypatch):
    target = tmp_path / "sharded-model"
    target.mkdir()
    # Incomplete sharded files: -of- shard without index.json.
    (target / "config.json").write_text("{}")
    (target / "model-00001-of-00002.safetensors").write_text("partial")

    monkeypatch.setattr(model_staging, "model_dir", lambda mid: str(target))
    index = {
        "weight_map": {
            "a": "model-00001-of-00002.safetensors",
            "b": "model-00002-of-00002.safetensors",
        }
    }
    entries = [
        {"name": "b/sharded-model/config.json", "type": "file"},
        {"name": "b/sharded-model/model.safetensors.index.json", "type": "file"},
        {"name": "b/sharded-model/model-00001-of-00002.safetensors", "type": "file"},
        {"name": "b/sharded-model/model-00002-of-00002.safetensors", "type": "file"},
    ]
    fake_fs = _FakeFS(
        entries,
        {
            "b/sharded-model/config.json": "{}",
            "b/sharded-model/model.safetensors.index.json": json.dumps(index),
            "b/sharded-model/model-00001-of-00002.safetensors": "s1",
            "b/sharded-model/model-00002-of-00002.safetensors": "s2",
        },
    )
    monkeypatch.setattr(model_staging.gcsfs, "GCSFileSystem", lambda: fake_fs)

    assert model_staging.stage_model("gs://b/sharded-model") == str(target)
    assert len(fake_fs.get_calls) == 1

    # Remove one shard referenced by index.json -> triggers re-stage again.
    (target / "model-00002-of-00002.safetensors").unlink()
    assert model_staging.stage_model("gs://b/sharded-model") == str(target)
    assert len(fake_fs.get_calls) == 2


def test_stage_model_raises_when_missing_safetensors(tmp_path, monkeypatch):
    local = tmp_path / "empty-local"
    local.mkdir()
    with pytest.raises(FileNotFoundError, match="no safetensors"):
        model_staging.stage_model(str(local))

    target = tmp_path / "empty-gcs"
    monkeypatch.setattr(model_staging, "model_dir", lambda mid: str(target))
    fake_fs = _FakeFS(
        [{"name": "b/empty/config.json", "type": "file"}], {"b/empty/config.json": "{}"}
    )
    monkeypatch.setattr(model_staging.gcsfs, "GCSFileSystem", lambda: fake_fs)
    with pytest.raises(FileNotFoundError, match="no safetensors"):
        model_staging.stage_model("gs://b/empty")
    assert list(tmp_path.iterdir()) == [local]


def test_stage_gcs_model_passes_through_non_gcs_and_stages_gcs(monkeypatch):
    called = []
    monkeypatch.setattr(
        model_staging, "stage_model", lambda mid: called.append(mid) or "/tmp/gpt2"
    )

    assert model_staging.stage_gcs_model("gpt2") == "gpt2"
    assert model_staging.stage_gcs_model("/local/model") == "/local/model"
    assert model_staging.stage_gcs_model("") == ""
    assert called == []

    assert model_staging.stage_gcs_model("gs://bucket/gpt2") == "/tmp/gpt2"
    assert called == ["gs://bucket/gpt2"]
