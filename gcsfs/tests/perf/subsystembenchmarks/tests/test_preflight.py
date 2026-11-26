import os
from unittest.mock import MagicMock

import pytest

from gcsfs.tests.perf.subsystembenchmarks import run
from gcsfs.tests.perf.subsystembenchmarks._common import preflight


def test_preflight_reports_missing_model_read():
    fake_fs = MagicMock()
    fake_fs.ls.side_effect = PermissionError("403 Forbidden")

    model_id = "gs://private-models/Llama-3.1-8B"
    with pytest.raises(preflight.PreflightError) as exc:
        preflight.check(
            "my-project",
            "us-central1",
            "ssb-pf",
            {model_id},
            fs=fake_fs,
        )
    assert model_id in str(exc.value)
    fake_fs.mkdir.assert_called_once()
    fake_fs.rmdir.assert_called_once()


def test_preflight_reports_bucket_create_failure():
    fake_fs = MagicMock()
    fake_fs.mkdir.side_effect = PermissionError("storage.buckets.create denied")

    with pytest.raises(preflight.PreflightError, match="create.*ssb-pf-pf-"):
        preflight.check(
            "my-project",
            "us-central1",
            "ssb-pf",
            set(),
            fs=fake_fs,
        )


def _model_listing(*entries):
    return [{"name": name, "type": kind} for name, kind in entries]


def test_preflight_creates_and_deletes_bucket_and_reads_models():
    fake_fs = MagicMock()
    fake_fs.ls.return_value = _model_listing(
        ("bucket/Llama-3.1-8B", "directory"),
        ("bucket/Llama-3.1-8B/LICENSE", "file"),
        ("bucket/Llama-3.1-8B/config.json", "file"),
    )

    preflight.check(
        "my-project",
        "us-central1",
        "ssb-test",
        {"gs://bucket/Llama-3.1-8B"},
        fs=fake_fs,
    )
    created_name = fake_fs.mkdir.call_args.args[0]
    assert created_name.startswith("ssb-test-pf-")
    assert fake_fs.mkdir.call_args.kwargs == {"location": "us-central1"}
    fake_fs.rmdir.assert_called_once_with(created_name)
    fake_fs.ls.assert_called_once_with("gs://bucket/Llama-3.1-8B", detail=True)
    fake_fs.cat_file.assert_called_once_with(
        "bucket/Llama-3.1-8B/config.json", start=0, end=1
    )


def test_preflight_forwards_bucket_kwargs_to_mkdir():
    fake_fs = MagicMock()
    kwargs = {
        "iam_configuration": {"uniformBucketLevelAccess": {"enabled": True}},
        "storageClass": "RAPID",
    }
    preflight.check(
        "my-project", "us-central1", "ssb-z", set(), bucket_kwargs=kwargs, fs=fake_fs
    )
    assert fake_fs.mkdir.call_args.kwargs == {"location": "us-central1", **kwargs}


def test_preflight_rejects_model_prefix_without_files():
    fake_fs = MagicMock()
    fake_fs.ls.return_value = _model_listing(("bucket/model", "directory"))
    with pytest.raises(preflight.PreflightError, match="has no files"):
        preflight.check(
            "p",
            "us-central1",
            "ssb",
            {"gs://bucket/model"},
            check_bucket=False,
            fs=fake_fs,
        )
    fake_fs.cat_file.assert_not_called()


def test_preflight_reports_unreadable_model_object():
    fake_fs = MagicMock()
    fake_fs.ls.return_value = _model_listing(("bucket/model/weights.bin", "file"))
    fake_fs.cat_file.side_effect = PermissionError("403 storage.objects.get denied")
    with pytest.raises(preflight.PreflightError, match="bucket/model/weights.bin"):
        preflight.check(
            "p",
            "us-central1",
            "ssb",
            {"gs://bucket/model"},
            check_bucket=False,
            fs=fake_fs,
        )


def test_preflight_uses_gcsfs_even_when_storage_emulator_host_set(monkeypatch):
    """run.py exports STORAGE_EMULATOR_HOST=https://storage.googleapis.com, which makes
    google.cloud.storage.Client() default to AnonymousCredentials. preflight must use
    gcsfs.GCSFileSystem(project=project) instead."""
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", "https://storage.googleapis.com")
    fake_fs = MagicMock()
    created_with = []

    def fake_ctor(*, project=None):
        created_with.append(project)
        return fake_fs

    import gcsfs

    monkeypatch.setattr(gcsfs, "GCSFileSystem", fake_ctor)
    preflight.check("my-project", "europe-west1", "ssb-gpu", set())
    assert created_with == ["my-project"]
    fake_fs.mkdir.assert_called_once()
    fake_fs.rmdir.assert_called_once()


def test_config_model_ids_includes_pinned_variants():
    ssb_root = os.path.dirname(os.path.dirname(__file__))
    save_dir = os.path.join(ssb_root, "checkpointing", "pytorch_save")
    ids = preflight.config_model_ids(save_dir)
    assert ids == {
        "gs://gcs-aiml-huggingface-model-weights/Llama-3.1-8B",
        "gs://gcs-aiml-huggingface-model-weights/OLMoE-1B-7B-0924",
    }

    overridden = preflight.config_model_ids(
        save_dir, model_id_override="gs://custom-bkt/MyModel"
    )
    assert overridden == {"gs://custom-bkt/MyModel"}

    hf_override = preflight.config_model_ids(save_dir, model_id_override="gpt2")
    assert hf_override == set()

    hf_dir = os.path.join(ssb_root, "dataloading", "huggingface_datasets")
    assert preflight.config_model_ids(hf_dir) == set()


def test_run_py_preflight_flag(monkeypatch, tmp_path):
    from unittest import mock

    calls = []
    monkeypatch.setattr(
        preflight,
        "check",
        lambda project, location, bucket_prefix, model_ids, **kw: calls.append(
            ("preflight", project, location, bucket_prefix, model_ids, kw)
        ),
    )
    monkeypatch.setattr(
        run.cli,
        "run_suite",
        lambda *a, **kw: calls.append(("run_suite",)) or (0, None),
    )
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GCSFS_SUBSYSTEM_NODES", None)
        with pytest.raises(SystemExit) as exc:
            run.main(
                [
                    "--group=checkpointing/pytorch_save",
                    "--bucket-prefix=ssb-pf",
                    "--project=proj",
                    "--location=us-central1",
                    "--preflight",
                    f"--results-dir={tmp_path}",
                ]
            )
    assert exc.value.code == 0
    assert calls[0][0] == "preflight"
    assert calls[0][1:4] == ("proj", "us-central1", "ssb-pf")
    assert "gs://gcs-aiml-huggingface-model-weights/Llama-3.1-8B" in calls[0][4]
    assert calls[0][5] == {"bucket_kwargs": {}}
    assert calls[1] == ("run_suite",)


def test_run_py_preflight_uses_the_run_bucket_type(monkeypatch, tmp_path):
    from unittest import mock

    seen = {}
    monkeypatch.setattr(
        preflight, "check", lambda *a, **kw: seen.update(kw.get("bucket_kwargs"))
    )
    monkeypatch.setattr(run.cli, "run_suite", lambda *a, **kw: (0, None))
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GCSFS_SUBSYSTEM_NODES", None)
        with pytest.raises(SystemExit):
            run.main(
                [
                    "--group=dataloading/webdataset",
                    "--bucket-prefix=ssb-pf",
                    "--project=proj",
                    "--location=us-central1",
                    "--bucket-type=zonal",
                    "--zone=us-central1-a",
                    "--preflight",
                    f"--results-dir={tmp_path}",
                ]
            )
    assert seen["storageClass"] == "RAPID"
    assert seen["customPlacementConfig"] == {"dataLocations": ["us-central1-a"]}


def test_run_py_preflight_skips_follower_nodes(monkeypatch, tmp_path):
    from unittest import mock

    from gcsfs.tests.perf.subsystembenchmarks._common import nodes

    calls = []
    monkeypatch.setattr(
        preflight,
        "check",
        lambda *a, **kw: calls.append("preflight"),
    )
    monkeypatch.setattr(
        run.cli,
        "run_suite",
        lambda *a, **kw: calls.append("run_suite") or (0, None),
    )
    with mock.patch.dict(
        os.environ,
        {nodes.NODE_RANK_ENV: "1", nodes.LEADER_ADDR_ENV: "pod-0.svc"},
        clear=False,
    ):
        with pytest.raises(SystemExit) as exc:
            run.main(
                [
                    "--group=checkpointing/pytorch_save",
                    "--nodes=2",
                    "--devices-per-node=8",
                    "--bucket-prefix=ssb-pf",
                    "--project=proj",
                    "--location=us-central1",
                    "--preflight",
                    f"--results-dir={tmp_path}",
                ]
            )
    assert exc.value.code == 0
    assert calls == ["run_suite"]
