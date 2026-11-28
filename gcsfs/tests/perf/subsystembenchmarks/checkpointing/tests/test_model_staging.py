import json
import os
import threading
import time

import pytest

from gcsfs.tests.perf.subsystembenchmarks.checkpointing import model_staging


class _FakeFS:
    def __init__(self, entries, files, get_delay=0.0):
        self._entries = entries
        self._files = files
        self._get_delay = get_delay
        self.ls_calls = []
        self.get_calls = []

    def ls(self, prefix, detail=False):
        self.ls_calls.append((prefix, detail))
        return list(self._entries)

    def get(self, rpaths, lpaths):
        self.get_calls.append((list(rpaths), list(lpaths)))
        time.sleep(self._get_delay)
        for rpath, lpath in zip(rpaths, lpaths):
            with open(lpath, "w") as f:
                f.write(self._files[rpath])


def _model_fs(prefix, get_delay=0.0):
    names = {"config.json": "{}", "model.safetensors": "weights"}
    entries = [{"name": f"{prefix}/", "type": "directory"}] + [
        {"name": f"{prefix}/{n}", "type": "file"} for n in names
    ]
    return _FakeFS(
        entries, {f"{prefix}/{n}": v for n, v in names.items()}, get_delay=get_delay
    )


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    path = tmp_path / "cache"
    path.mkdir()
    monkeypatch.setenv(model_staging.MODEL_CACHE_DIR_ENV, str(path))
    return path


def _leftovers(cache_dir):
    """Cache entries other than staged models and their lock files."""
    return sorted(p.name for p in cache_dir.iterdir() if p.name.startswith("."))


def test_model_dir_keys_gcs_uris_by_bucket_and_path(cache_dir):
    a = model_staging.model_dir("gs://bucket-a/models/Llama-3.1-8B/")
    assert os.path.dirname(a) == str(cache_dir)
    assert os.path.basename(a).startswith("Llama-3.1-8B-")
    assert model_staging.model_dir("gs://bucket-a/models/Llama-3.1-8B") == a
    assert model_staging.model_dir("gs://bucket-b/models/Llama-3.1-8B") != a
    assert model_staging.model_dir("gs://bucket-a/other/Llama-3.1-8B") != a
    assert model_staging.model_dir("/local/models/gpt2") == "/local/models/gpt2"
    assert model_staging.model_dir("gpt2") == "gpt2"


def test_cache_root_defaults_to_the_temp_dir(monkeypatch, tmp_path):
    monkeypatch.delenv(model_staging.MODEL_CACHE_DIR_ENV, raising=False)
    monkeypatch.setattr(model_staging.tempfile, "gettempdir", lambda: str(tmp_path))
    assert os.path.dirname(model_staging.model_dir("gs://b/gpt2")) == str(tmp_path)


@pytest.mark.parametrize(
    "bad_id",
    [
        "gs://",
        "gs:///",
        "gs:///gpt2",
        "gs://bucket/.",
        "gs://bucket/..",
        "gs://bucket/sub/..",
        "gs://bucket//gpt2",
    ],
)
def test_model_dir_rejects_traversal_and_empty_segments(bad_id):
    with pytest.raises(ValueError, match="Invalid GCS model_id"):
        model_staging.model_dir(bad_id)


def test_stage_model_downloads_atomically_via_temp_dir_and_replace(
    cache_dir, monkeypatch
):
    fake_fs = _model_fs("bucket/Llama-3.1-8B")
    monkeypatch.setattr(model_staging.gcsfs, "GCSFileSystem", lambda: fake_fs)
    replace_calls = []
    real_replace = os.replace

    def spy_replace(src, dst):
        replace_calls.append((src, dst))
        return real_replace(src, dst)

    monkeypatch.setattr(model_staging.os, "replace", spy_replace)

    target = model_staging.stage_model("gs://bucket/Llama-3.1-8B")
    assert target == model_staging.model_dir("gs://bucket/Llama-3.1-8B")
    assert fake_fs.ls_calls == [("bucket/Llama-3.1-8B", True)]
    assert len(fake_fs.get_calls) == 1
    [(src, dst)] = replace_calls
    assert os.path.dirname(src) == str(cache_dir)
    assert dst == target
    with open(os.path.join(target, "model.safetensors")) as f:
        assert f.read() == "weights"
    assert _leftovers(cache_dir) == []

    # Already staged: no GCS access.
    assert model_staging.stage_model("gs://bucket/Llama-3.1-8B") == target
    assert len(fake_fs.ls_calls) == 1


def test_concurrent_stagers_download_once(cache_dir, monkeypatch):
    fake_fs = _model_fs("b/gpt2", get_delay=0.2)
    monkeypatch.setattr(model_staging.gcsfs, "GCSFileSystem", lambda: fake_fs)
    results, errors = [], []

    def stage():
        try:
            results.append(model_staging.stage_model("gs://b/gpt2"))
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)

    threads = [threading.Thread(target=stage) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert results == [model_staging.model_dir("gs://b/gpt2")] * 4
    assert len(fake_fs.get_calls) == 1
    assert _leftovers(cache_dir) == []


@pytest.mark.parametrize("bad_key", ["bucket/model/..", "bucket/model/."])
def test_stage_model_rejects_dot_and_dotdot_remote_keys(
    cache_dir, monkeypatch, bad_key
):
    fake_fs = _FakeFS([{"name": bad_key, "type": "file"}], {})
    monkeypatch.setattr(model_staging.gcsfs, "GCSFileSystem", lambda: fake_fs)

    with pytest.raises(ValueError, match="Invalid remote object key"):
        model_staging.stage_model("gs://bucket/model")

    assert fake_fs.get_calls == []
    assert not os.path.exists(model_staging.model_dir("gs://bucket/model"))
    assert _leftovers(cache_dir) == []


def test_stage_model_rejects_commonpath_escape(cache_dir, monkeypatch):
    fake_fs = _FakeFS([{"name": "bucket/model/config.json", "type": "file"}], {})
    monkeypatch.setattr(model_staging.gcsfs, "GCSFileSystem", lambda: fake_fs)
    outside = str(cache_dir.parent / "escaped")
    real_realpath = os.path.realpath
    monkeypatch.setattr(
        model_staging.os.path,
        "realpath",
        lambda p: outside if p.endswith("config.json") else real_realpath(p),
    )

    with pytest.raises(ValueError, match="Refusing to write outside"):
        model_staging.stage_model("gs://bucket/model")

    assert fake_fs.get_calls == []
    assert _leftovers(cache_dir) == []


def test_stage_model_restages_incomplete_sharded_safetensors(cache_dir, monkeypatch):
    target = model_staging.model_dir("gs://b/sharded-model")
    os.makedirs(target)
    # An interrupted copy: a -of- shard without index.json.
    for name, text in (
        ("config.json", "{}"),
        ("model-00001-of-00002.safetensors", "partial"),
    ):
        with open(os.path.join(target, name), "w") as f:
            f.write(text)

    index = {
        "weight_map": {
            "a": "model-00001-of-00002.safetensors",
            "b": "model-00002-of-00002.safetensors",
        }
    }
    files = {
        "b/sharded-model/config.json": "{}",
        "b/sharded-model/model.safetensors.index.json": json.dumps(index),
        "b/sharded-model/model-00001-of-00002.safetensors": "s1",
        "b/sharded-model/model-00002-of-00002.safetensors": "s2",
    }
    fake_fs = _FakeFS([{"name": n, "type": "file"} for n in files], files)
    monkeypatch.setattr(model_staging.gcsfs, "GCSFileSystem", lambda: fake_fs)

    assert model_staging.stage_model("gs://b/sharded-model") == target
    assert len(fake_fs.get_calls) == 1

    # A shard listed by index.json is missing -> staged again.
    os.unlink(os.path.join(target, "model-00002-of-00002.safetensors"))
    assert model_staging.stage_model("gs://b/sharded-model") == target
    assert len(fake_fs.get_calls) == 2
    assert _leftovers(cache_dir) == []


def test_stage_model_raises_when_missing_safetensors(tmp_path, cache_dir, monkeypatch):
    local = tmp_path / "empty-local"
    local.mkdir()
    with pytest.raises(FileNotFoundError, match="no safetensors"):
        model_staging.stage_model(str(local))

    fake_fs = _FakeFS(
        [{"name": "b/empty/config.json", "type": "file"}], {"b/empty/config.json": "{}"}
    )
    monkeypatch.setattr(model_staging.gcsfs, "GCSFileSystem", lambda: fake_fs)
    with pytest.raises(FileNotFoundError, match="no safetensors"):
        model_staging.stage_model("gs://b/empty")
    assert not os.path.exists(model_staging.model_dir("gs://b/empty"))
    assert _leftovers(cache_dir) == []


@pytest.mark.parametrize(
    "index",
    [
        "not json",
        json.dumps([]),
        json.dumps({"weight_map": {}}),
        json.dumps({"weight_map": {"a": "../model.safetensors"}}),
        json.dumps({"weight_map": {"a": 1}}),
    ],
)
def test_safetensors_files_rejects_bad_index(tmp_path, index):
    (tmp_path / "model.safetensors").write_text("w")
    (tmp_path / "model.safetensors.index.json").write_text(index)
    assert model_staging.safetensors_files(str(tmp_path)) == []


def test_safetensors_files_lists_unsharded_files_without_hidden_copies(tmp_path):
    (tmp_path / "model.safetensors").write_text("w")
    (tmp_path / ".canonical.safetensors").write_text("c")
    assert model_staging.safetensors_files(str(tmp_path)) == [
        str(tmp_path / "model.safetensors")
    ]


def test_stage_gcs_model_passes_through_non_gcs_and_stages_gcs(monkeypatch):
    called = []
    monkeypatch.setattr(
        model_staging, "stage_model", lambda mid: called.append(mid) or "/cache/gpt2"
    )

    assert model_staging.stage_gcs_model("gpt2") == "gpt2"
    assert model_staging.stage_gcs_model("/local/model") == "/local/model"
    assert model_staging.stage_gcs_model("") == ""
    assert called == []

    assert model_staging.stage_gcs_model("gs://bucket/gpt2") == "/cache/gpt2"
    assert called == ["gs://bucket/gpt2"]
