import json
import os
import re
import subprocess
import sys
import time
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


def test_setup_environment_clears_stale_model_id_when_flag_empty(monkeypatch):
    monkeypatch.setenv("GCSFS_SUBSYSTEM_MODEL_ID", "gs://stale/model")
    explicit = run.parse_args(
        ["--group=dataloading/huggingface_datasets", "--model-id=gs://custom/model"]
        + _REQUIRED
    )
    run._setup_environment(explicit)
    assert os.environ["GCSFS_SUBSYSTEM_MODEL_ID"] == "gs://custom/model"

    empty = run.parse_args(
        ["--group=dataloading/huggingface_datasets", "--model-id="] + _REQUIRED
    )
    run._setup_environment(empty)
    assert "GCSFS_SUBSYSTEM_MODEL_ID" not in os.environ


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


def test_required_amplification_rejects_a_scrape_that_enriched_nothing():
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.amplification import (
        EnrichmentResult,
    )

    result = EnrichmentResult(
        eligible=2, enriched=0, missing_buckets=("bucket-a", "bucket-b")
    )
    with pytest.raises(RuntimeError, match="bucket-a, bucket-b"):
        run.require_any_amplification(result)


@pytest.mark.parametrize(
    "eligible,enriched,missing",
    [(2, 1, ("bucket-b",)), (2, 2, ()), (0, 0, ())],
    ids=["one-dropped", "all-enriched", "nothing-eligible"],
)
def test_required_amplification_allows_partial_or_no_eligible_rows(
    eligible, enriched, missing
):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.amplification import (
        EnrichmentResult,
    )

    run.require_any_amplification(EnrichmentResult(eligible, enriched, missing))


def test_required_amplification_rejects_scrape_errors_even_with_enriched_rows():
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.amplification import (
        EnrichmentResult,
    )

    result = EnrichmentResult(
        eligible=3,
        enriched=2,
        missing_buckets=("bucket-err",),
        failed_buckets=("bucket-err",),
    )
    with pytest.raises(RuntimeError, match="errored for buckets: bucket-err"):
        run.require_any_amplification(result)


def test_amplification_wait_default_matches_the_scrape_module():
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import amplification

    args = run.parse_args(["--group=dataloading/huggingface_datasets"] + _REQUIRED)
    assert args.amplification_wait == amplification.DEFAULT_SCRAPE_WAIT_SECONDS


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


def test_scrape_amplification_skips_write_only_csv(tmp_path, monkeypatch):
    json_path = tmp_path / "results.json"
    with open(json_path, "w") as f:
        json.dump(
            {
                "benchmarks": [
                    {
                        "name": "checkpoint_write-x",
                        "group": "checkpoint_write",
                        "stats": {
                            "min": 1.0,
                            "max": 1.0,
                            "mean": 1.0,
                            "median": 1.0,
                            "stddev": 0.0,
                            "data": [1.0],
                        },
                        "extra_info": {
                            "gcs_bucket_name": "bucket-a",
                            "measurement_window_start_unix_seconds": 1000,
                            "measurement_window_end_unix_seconds": 1060,
                            "checkpoint_physical_size_bytes": 500,
                            "workload_scenario": "checkpoint_write",
                            "checkpoint_write_throughput_mean_bytes_per_second": 250.0,
                        },
                    }
                ]
            },
            f,
        )
    csv_path = run.report.generate_csv(str(json_path), str(tmp_path))
    args = run.parse_args(
        [
            "--group=checkpointing/pytorch_save",
            "--amplification-wait=600",
            "--require-amplification",
        ]
        + _REQUIRED
    )
    monkeypatch.setattr(
        time,
        "sleep",
        lambda s: (_ for _ in ()).throw(AssertionError(f"unexpected sleep({s})")),
    )

    assert run._scrape_amplification(csv_path, args) is None


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


def test_pytorch_checkpointing_groups_are_discoverable():
    """Verifies pytorch_{save,load,reshard} are discovered and pytorch is not."""
    groups = run.discover_groups()
    assert "checkpointing/pytorch_save" in groups
    assert "checkpointing/pytorch_load" in groups
    assert "checkpointing/pytorch_reshard" in groups
    assert "checkpointing/pytorch" not in groups


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
    "GCSFS_SUBSYSTEM_DEVICE",
    "GCSFS_SUBSYSTEM_GROUP",
    "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT",
    "STORAGE_EMULATOR_HOST",
)


def test_parse_args_device_sets_env(monkeypatch, capsys):
    for key in (*_RUN_ENV_KEYS, "FSSPEC_GCS_RCU_RAPID_WRITE"):
        monkeypatch.delenv(key, raising=False)

    default_args = run.parse_args(["--group=checkpointing/pytorch_load"] + _REQUIRED)
    assert default_args.device == "cpu"
    run._setup_environment(default_args)
    assert os.environ["GCSFS_SUBSYSTEM_DEVICE"] == "cpu"

    cuda_args = run.parse_args(
        ["--group=checkpointing/pytorch_load", "--device=cuda"] + _REQUIRED
    )
    assert cuda_args.device == "cuda"
    run._setup_environment(cuda_args)
    assert os.environ["GCSFS_SUBSYSTEM_DEVICE"] == "cuda"

    with pytest.raises(SystemExit):
        run.parse_args(
            ["--group=checkpointing/pytorch_load", "--device=tpu"] + _REQUIRED
        )
    assert "--device" in capsys.readouterr().err


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


def test_ci_workflow_subsystembenchmark_requirements_exist():
    repo_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
    )
    ci_path = os.path.join(repo_root, ".github", "workflows", "ci.yml")
    with open(ci_path) as f:
        ci_yaml = f.read()

    req_paths = re.findall(
        r"gcsfs/tests/perf/subsystembenchmarks/\S*requirements\S*\.txt", ci_yaml
    )
    assert req_paths
    missing = [
        rel_path
        for rel_path in req_paths
        if not os.path.isfile(os.path.join(repo_root, rel_path))
    ]
    assert missing == []


@pytest.mark.parametrize(
    "bucket_type",
    ["standard_rcu_warm", "standard_rcu_cold", "hns_rcu_warm", "hns_rcu_cold"],
)
def test_parse_args_rejects_write_only_group_on_rcu_warm_and_cold(capsys, bucket_type):
    with pytest.raises(SystemExit):
        run.parse_args(
            [
                "--group=checkpointing/pytorch_save",
                f"--bucket-type={bucket_type}",
                "--zone=us-central1-a",
            ]
            + _REQUIRED
        )
    err = capsys.readouterr().err
    assert "only write scenarios" in err
    assert "*_rcu_z" in err


@pytest.mark.parametrize("bucket_type", ["standard_rcu_z", "hns_rcu_z"])
def test_parse_args_allows_write_only_group_on_rcu_z(bucket_type):
    args = run.parse_args(
        [
            "--group=checkpointing/pytorch_save",
            f"--bucket-type={bucket_type}",
            "--zone=us-central1-a",
        ]
        + _REQUIRED
    )
    assert args.bucket_type == bucket_type


@pytest.mark.parametrize(
    "group",
    [
        "checkpointing/pytorch_lightning",
        "checkpointing/ray_pytorch",
        "checkpointing/pytorch_load",
        "checkpointing/pytorch_reshard",
        "dataloading/webdataset",
    ],
)
@pytest.mark.parametrize("bucket_type", ["standard_rcu_warm", "hns_rcu_cold"])
def test_parse_args_allows_read_and_mixed_groups_on_rcu_warm_and_cold(
    group, bucket_type
):
    args = run.parse_args(
        [
            f"--group={group}",
            f"--bucket-type={bucket_type}",
            "--zone=us-central1-a",
        ]
        + _REQUIRED
    )
    assert args.group == group
