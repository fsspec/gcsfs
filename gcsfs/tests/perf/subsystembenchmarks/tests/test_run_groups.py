import os

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
def test_parse_args_accepts_rapid_cache_with_zone_and_timeout(monkeypatch, bucket_type):
    for key in (
        "GCSFS_SUBSYSTEM_BUCKET_TYPE",
        "GCSFS_SUBSYSTEM_ZONE",
        "GCSFS_SUBSYSTEM_RAPID_CACHE_TIMEOUT",
    ):
        monkeypatch.delenv(key, raising=False)
    args = run.parse_args(
        [
            "--group=dataloading/webdataset",
            f"--bucket-type={bucket_type}",
            "--zone=us-central1-a",
            "--rapid-cache-timeout=900",
        ]
        + _REQUIRED
    )
    run._setup_environment(args)
    assert os.environ["GCSFS_SUBSYSTEM_BUCKET_TYPE"] == bucket_type
    assert os.environ["GCSFS_SUBSYSTEM_ZONE"] == "us-central1-a"
    assert os.environ["GCSFS_SUBSYSTEM_RAPID_CACHE_TIMEOUT"] == "900"


@pytest.mark.parametrize("bad_timeout", ["0", "-5"])
def test_parse_args_rejects_nonpositive_rapid_cache_timeout(capsys, bad_timeout):
    with pytest.raises(SystemExit):
        run.parse_args(
            [
                "--group=dataloading/webdataset",
                f"--rapid-cache-timeout={bad_timeout}",
            ]
            + _REQUIRED
        )
    assert "--rapid-cache-timeout must be > 0" in capsys.readouterr().err


def test_cloudbuild_and_runner_script_wire_rapid_cache_timeout_and_disable_leaked_caches():
    repo_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
    )
    cb_path = os.path.join(
        repo_root,
        "cloudbuild",
        "subsystembenchmarks",
        "subsystembenchmarks-cloudbuild.yaml",
    )
    sh_path = os.path.join(
        repo_root,
        "cloudbuild",
        "subsystembenchmarks",
        "scripts",
        "run-benchmarks.sh",
    )
    with open(cb_path) as f:
        cb_yaml = f.read()
    with open(sh_path) as f:
        sh_text = f.read()

    assert "_RAPID_CACHE_TIMEOUT" in cb_yaml
    assert "export RAPID_CACHE_TIMEOUT=" in cb_yaml
    assert "--rapid-cache-timeout=" in sh_text
    assert cb_yaml.count("/anywhereCaches/") >= 2
    assert cb_yaml.index("TOKEN=$$(gcloud auth print-access-token") < cb_yaml.index(
        "gcloud storage buckets list"
    )
    assert 'gcloud storage rm --recursive "gs://$$CLEAN_NAME" < /dev/null' in cb_yaml
    assert (
        'gcloud storage buckets delete "gs://$$CLEAN_NAME" --quiet < /dev/null'
        in cb_yaml
    )
    assert "HAS_CACHE=" in cb_yaml
    assert (
        "^${_INFRA_PREFIX}-(regional|zonal|hns|rapid_cache_cold|rapid_cache_warm)-[0-9a-f]{8}-"
        in cb_yaml
    )


def _rapid_cache_args(bucket_type, *extra):
    return run.parse_args(
        [
            "--group=dataloading/webdataset",
            f"--bucket-type={bucket_type}",
            "--zone=us-central1-b",
            *extra,
        ]
        + _REQUIRED
    )


def _warm_csv(tmp_path, ratio):
    csv_path = tmp_path / "results.csv"
    csv_path.write_text(
        "benchmark_case_id,bucket_type,rapid_cache_hit_ratio\n"
        f"case-a,rapid_cache_warm,{ratio}\n"
    )
    return csv_path


@pytest.mark.parametrize("bad_ratio", ["-0.1", "1.5"])
def test_parse_args_rejects_out_of_range_min_warm_hit_ratio(capsys, bad_ratio):
    with pytest.raises(SystemExit):
        _rapid_cache_args("rapid_cache_warm", f"--min-warm-hit-ratio={bad_ratio}")
    assert "--min-warm-hit-ratio must be within [0, 1]" in capsys.readouterr().err


def test_require_warm_cache_hits_rejects_a_warm_run_served_from_the_bucket(tmp_path):
    args = _rapid_cache_args("rapid_cache_warm")

    with pytest.raises(RuntimeError, match=r"case-a \(0\.0%\)"):
        run.require_warm_cache_hits(_warm_csv(tmp_path, "0.0"), args)


def test_require_warm_cache_hits_rejects_an_unmeasured_warm_run(tmp_path):
    args = _rapid_cache_args("rapid_cache_warm")

    with pytest.raises(RuntimeError, match=r"case-a \(unknown\)"):
        run.require_warm_cache_hits(_warm_csv(tmp_path, ""), args)


def test_require_warm_cache_hits_accepts_a_cached_warm_run(tmp_path):
    run.require_warm_cache_hits(
        _warm_csv(tmp_path, "0.99"), _rapid_cache_args("rapid_cache_warm")
    )


@pytest.mark.parametrize(
    "bucket_type,extra",
    [("rapid_cache_cold", ()), ("rapid_cache_warm", ("--min-warm-hit-ratio=0",))],
)
def test_require_warm_cache_hits_skips_cold_runs_and_disabled_check(
    tmp_path, bucket_type, extra
):
    run.require_warm_cache_hits(
        _warm_csv(tmp_path, "0.0"), _rapid_cache_args(bucket_type, *extra)
    )


def test_scrape_rapid_cache_hits_skips_non_rapid_cache_runs(tmp_path):
    args = run.parse_args(["--group=dataloading/webdataset"] + _REQUIRED)

    assert run._scrape_rapid_cache_hits(tmp_path / "results.csv", args) is None


def test_scrape_rapid_cache_hits_passes_client_through(monkeypatch, tmp_path):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import rapid_cache_hits

    calls = []

    def fake_enrich(csv_path, project, *, client):
        calls.append((csv_path, project, client))
        return ()

    monkeypatch.setattr(rapid_cache_hits, "enrich_csv", fake_enrich)
    client = object()

    missing = run._scrape_rapid_cache_hits(
        "results.csv", _rapid_cache_args("rapid_cache_cold"), client=client
    )

    assert missing == ()
    assert calls == [("results.csv", "pr", client)]


def test_rapid_cache_hits_retry_waits_once_for_missing_buckets(monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import rapid_cache_hits

    results = iter([("bucket-a",), ()])
    monkeypatch.setattr(rapid_cache_hits, "enrich_csv", lambda *a, **k: next(results))
    sleeps = []

    missing = run.enrich_rapid_cache_hits_with_retry(
        "results.csv", "project", object(), retry_wait=30, sleep=sleeps.append
    )

    assert missing == ()
    assert sleeps == [30]
