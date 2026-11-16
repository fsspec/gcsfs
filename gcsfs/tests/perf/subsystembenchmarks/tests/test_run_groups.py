import os
import subprocess
import sys
from unittest import mock

import pytest

from gcsfs.tests.perf.subsystembenchmarks import run

_REQUIRED = [
    "--bucket-prefix=p",
    "--project=pr",
    "--location=us-central1",
]


def test_parse_args_accepts_a_discovered_group():
    args = run.parse_args(["--group=dataloading/huggingface_datasets"] + _REQUIRED)
    assert args.group == "dataloading/huggingface_datasets"


def test_setup_environment_sets_storage_emulator_host(monkeypatch):
    monkeypatch.delenv("STORAGE_EMULATOR_HOST", raising=False)
    args = run.parse_args(["--group=dataloading/huggingface_datasets"] + _REQUIRED)
    run._setup_environment(args)
    assert os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"


def test_setup_environment_exports_sweep_axes(monkeypatch):
    monkeypatch.delenv("GCSFS_SUBSYSTEM_SWEEP_AXES", raising=False)
    args = run.parse_args(
        [
            "--group=dataloading/huggingface_datasets",
            "--sweep-axes=workers prefetch",
        ]
        + _REQUIRED
    )
    run._setup_environment(args)
    assert os.environ["GCSFS_SUBSYSTEM_SWEEP_AXES"] == "workers prefetch"


def test_parse_args_rejects_negative_amplification_wait(capsys):
    with pytest.raises(SystemExit):
        run.parse_args(
            [
                "--group=dataloading/huggingface_datasets",
                "--amplification-wait=-1",
            ]
            + _REQUIRED
        )
    assert "--amplification-wait must be >= 0" in capsys.readouterr().err


def test_required_amplification_rejects_missing_buckets():
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.amplification import (
        EnrichmentResult,
    )

    result = EnrichmentResult(eligible=2, enriched=1, missing_buckets=("bucket-b",))
    with pytest.raises(RuntimeError, match="bucket-b"):
        run.require_complete_amplification(result)


def test_amplification_retry_waits_once_for_missing_buckets(monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import amplification

    results = iter(
        [
            amplification.EnrichmentResult(1, 0, ("bucket-a",)),
            amplification.EnrichmentResult(1, 1, ()),
        ]
    )
    monkeypatch.setattr(amplification, "enrich_csv", lambda *a, **k: next(results))
    sleeps = []

    result = run.enrich_amplification_with_retry(
        "results.csv", "project", object(), retry_wait=30, sleep=sleeps.append
    )

    assert result.missing_buckets == ()
    assert sleeps == [30]


def test_csv_with_renamed_amplification_columns_is_eligible(tmp_path):
    csv_path = tmp_path / "results.csv"
    csv_path.write_text(
        "benchmark_case_id,gcs_bucket_name,"
        "measurement_window_start_unix_seconds,"
        "measurement_window_end_unix_seconds,dataset_size_bytes\n"
        "case-a,bucket-a,1000,1060,500\n"
    )

    assert run._csv_has_amplification_inputs(csv_path)


def test_build_pytest_args_includes_run_benchmarks():
    from gcsfs.tests.perf.subsystembenchmarks._common.cli import build_pytest_args

    args = build_pytest_args("/path/to/suite", "/path/to/results.json")
    assert "--run-benchmarks" in args


def test_build_pytest_args_overrides_case_timeout():
    from gcsfs.tests.perf.subsystembenchmarks._common.cli import (
        CASE_TIMEOUT_SECONDS,
        build_pytest_args,
    )

    args = build_pytest_args("/path/to/suite", "/path/to/results.json")
    assert CASE_TIMEOUT_SECONDS == 7200
    assert f"--timeout={CASE_TIMEOUT_SECONDS}" in args


def test_webdataset_group_is_discoverable():
    """Verifies dataloading/webdataset is discovered from its requirements.txt."""
    assert "dataloading/webdataset" in run.discover_groups()


def test_ray_data_group_is_discoverable():
    """Verifies dataloading/ray_data is discovered from its requirements.txt."""
    assert "dataloading/ray_data" in run.discover_groups()


def test_ray_checkpointing_group_is_discoverable():
    """Verifies checkpointing/ray_pytorch is discovered from its requirements.txt."""
    assert "checkpointing/ray_pytorch" in run.discover_groups()


def test_pytorch_checkpointing_group_is_discoverable():
    """Verifies checkpointing/pytorch is discovered from its requirements.txt."""
    assert "checkpointing/pytorch" in run.discover_groups()


@pytest.mark.parametrize("bucket_type", ["rapid_cache_cold", "rapid_cache_warm"])
def test_parse_args_requires_zone_for_rapid_cache_bucket_types(capsys, bucket_type):
    with pytest.raises(SystemExit):
        run.parse_args(
            [
                "--group=dataloading/webdataset",
                f"--bucket-type={bucket_type}",
            ]
            + _REQUIRED
        )
    assert "--zone is required" in capsys.readouterr().err


@pytest.mark.parametrize("bucket_type", ["rapid_cache_cold", "rapid_cache_warm"])
def test_parse_args_accepts_rapid_cache_with_zone(monkeypatch, bucket_type):
    for key in ("GCSFS_SUBSYSTEM_BUCKET_TYPE", "GCSFS_SUBSYSTEM_ZONE"):
        monkeypatch.delenv(key, raising=False)
    args = run.parse_args(
        [
            "--group=dataloading/webdataset",
            f"--bucket-type={bucket_type}",
            "--zone=us-central1-a",
        ]
        + _REQUIRED
    )
    run._setup_environment(args)
    assert os.environ["GCSFS_SUBSYSTEM_BUCKET_TYPE"] == bucket_type
    assert os.environ["GCSFS_SUBSYSTEM_ZONE"] == "us-central1-a"


_RCU_BUCKET_TYPES = [
    "standard_rcu_z",
    "standard_rcu_warm",
    "standard_rcu_cold",
    "hns_rcu_z",
    "hns_rcu_warm",
    "hns_rcu_cold",
]


@pytest.mark.parametrize("bucket_type", _RCU_BUCKET_TYPES)
def test_parse_args_requires_zone_for_rcu_bucket_types(capsys, bucket_type):
    argv = ["--group=dataloading/webdataset", f"--bucket-type={bucket_type}"]
    with pytest.raises(SystemExit):
        run.parse_args(argv + _REQUIRED)
    assert "--zone is required" in capsys.readouterr().err
    args = run.parse_args(argv + _REQUIRED + ["--zone=us-central1-c"])
    assert args.zone == "us-central1-c"


@pytest.mark.parametrize(
    "bucket_type, rcu_rapid_write",
    [
        ("standard_rcu_z", "true"),
        ("hns_rcu_z", "true"),
        ("standard_rcu_warm", None),
        ("hns_rcu_cold", None),
        ("zonal", None),
        ("regional", None),
    ],
)
def test_setup_environment_turns_on_rcu_rapid_write_for_rcu_z(
    bucket_type, rcu_rapid_write
):
    """*_rcu_z stages and saves RAPID objects, which gcsfs writes to RCU buckets
    only with rcu_rapid_write, so every process of the run gets the env var."""
    args = run.parse_args(
        [
            "--group=dataloading/webdataset",
            f"--bucket-type={bucket_type}",
            "--zone=us-central1-a",
        ]
        + _REQUIRED
    )
    with mock.patch.dict(os.environ):
        os.environ.pop("FSSPEC_GCS_RCU_RAPID_WRITE", None)
        run._setup_environment(args)
        assert os.environ.get("FSSPEC_GCS_RCU_RAPID_WRITE") == rcu_rapid_write

        os.environ["FSSPEC_GCS_RCU_RAPID_WRITE"] = "true"
        run._setup_environment(args)
        assert os.environ.get("FSSPEC_GCS_RCU_RAPID_WRITE") == rcu_rapid_write


@pytest.mark.parametrize(
    "bucket_type, expected",
    [
        ("standard_rcu_z", True),
        ("standard_rcu_cold", False),
    ],
)
def test_setup_environment_propagates_rcu_rapid_write_to_spawned_child_process(
    bucket_type, expected
):
    """Spawned child processes initialize GCSFileSystem().rcu_rapid_write from _setup_environment."""
    args = run.parse_args(
        [
            "--group=dataloading/webdataset",
            f"--bucket-type={bucket_type}",
            "--zone=us-central1-a",
        ]
        + _REQUIRED
    )
    with mock.patch.dict(os.environ):
        os.environ["FSSPEC_GCS_RCU_RAPID_WRITE"] = "true"
        run._setup_environment(args)
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import gcsfs; "
                    "fs = gcsfs.GCSFileSystem(token='anon', skip_instance_cache=True); "
                    "print(fs.rcu_rapid_write)"
                ),
            ],
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            check=True,
        )

    assert proc.stdout.strip() == str(expected)


def test_cloudbuild_disables_leaked_rapid_caches_before_deleting_buckets():
    repo_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
    )
    cb_path = os.path.join(
        repo_root,
        "cloudbuild",
        "subsystembenchmarks",
        "subsystembenchmarks-cloudbuild.yaml",
    )
    with open(cb_path) as f:
        cb_yaml = f.read()

    assert cb_yaml.count("/anywhereCaches/") >= 2
    assert cb_yaml.index("TOKEN=$$(gcloud auth print-access-token") < cb_yaml.index(
        "gcloud storage buckets list"
    )
    assert 'gcloud storage rm --recursive "gs://$$CLEAN_NAME" < /dev/null' in cb_yaml
    assert (
        'gcloud storage buckets delete "gs://$$CLEAN_NAME" --quiet < /dev/null'
        in cb_yaml
    )
    # The steps run on the cloud-sdk image, which ships no system python3:
    # caches must be listed with gcloud, not parsed with `python3 -c`.
    assert cb_yaml.count("gcloud storage buckets anywhere-caches list") >= 2
    assert "python3 -c" not in cb_yaml
    assert (
        "^${_INFRA_PREFIX}-(ci|regional|zonal|hns|rapid_cache_cold|rapid_cache_warm"
        "|standard_rcu_z|standard_rcu_warm|standard_rcu_cold|hns_rcu_z|hns_rcu_warm"
        "|hns_rcu_cold)-[0-9a-f]{8}-" in cb_yaml
    )


_RUN_ENV_KEYS = (
    "GCSFS_SUBSYSTEM_BUCKET_PREFIX",
    "GCSFS_SUBSYSTEM_BUCKET_TYPE",
    "GCSFS_SUBSYSTEM_PROJECT",
    "GCSFS_SUBSYSTEM_LOCATION",
    "GCSFS_SUBSYSTEM_ZONE",
    "GCSFS_SUBSYSTEM_SWEEP_AXES",
    "GCSFS_SUBSYSTEM_MODEL_ID",
    "GCSFS_SUBSYSTEM_GROUP",
    "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT",
    "STORAGE_EMULATOR_HOST",
)


def _run_main_capturing_results_dir(monkeypatch, extra_args):
    # delenv registers each key for restoration, so main()'s os.environ writes
    # do not leak into other tests.
    for key in _RUN_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    seen = {}

    def fake_run_suite(suite_dir, results_dir, filter_expr=None):
        seen["results_dir"] = results_dir
        return 0, None

    monkeypatch.setattr(run.cli, "run_suite", fake_run_suite)
    with pytest.raises(SystemExit):
        run.main(["--group=dataloading/huggingface_datasets"] + _REQUIRED + extra_args)
    return seen["results_dir"]


def test_main_uses_and_creates_results_dir(monkeypatch, tmp_path):
    target = tmp_path / "nested" / "case"
    results_dir = _run_main_capturing_results_dir(
        monkeypatch, [f"--results-dir={target}"]
    )
    assert results_dir == str(target)
    assert target.is_dir()


def test_main_defaults_to_timestamped_run_dir(monkeypatch):
    results_dir = _run_main_capturing_results_dir(monkeypatch, [])
    try:
        assert os.path.dirname(results_dir) == os.path.join(
            os.path.dirname(run.__file__), "__run__"
        )
    finally:
        os.rmdir(results_dir)
