import argparse
import os
import unittest.mock as mock

import pytest

from gcsfs.tests.perf.microbenchmarks import run


def test_setup_environment_success():
    args = argparse.Namespace(
        regional_bucket="regional",
        zonal_bucket="zonal",
        hns_bucket="hns",
        config=["conf1"],
    )
    # Clear relevant env vars to ensure clean state
    with mock.patch.dict(os.environ, {}, clear=True):
        run._setup_environment(args)
        assert os.environ["GCSFS_TEST_BUCKET"] == "regional"
        assert os.environ["GCSFS_ZONAL_TEST_BUCKET"] == "zonal"
        assert os.environ["GCSFS_HNS_TEST_BUCKET"] == "hns"
        assert os.environ["GCSFS_BENCHMARK_FILTER"] == "conf1"
        assert os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] == "true"


def test_setup_environment_missing_buckets():
    args = argparse.Namespace(regional_bucket=None, zonal_bucket=None, hns_bucket=None)
    # Should exit if no buckets provided
    with pytest.raises(SystemExit):
        run._setup_environment(args)


@pytest.mark.parametrize(
    "cli_argv,expected_env",
    [
        (
            [
                "run.py",
                "--standard-rcu-bucket=std-rcu-bkt",
                "--bucket-types",
                "standard_rcu_z",
                "standard_rcu_warm",
            ],
            {
                "GCSFS_STANDARD_RCU_TEST_BUCKET": "std-rcu-bkt",
                "GCSFS_HNS_RCU_TEST_BUCKET": "",
                "GCSFS_BENCHMARK_BUCKET_TYPES": "standard_rcu_z,standard_rcu_warm",
            },
        ),
        (
            [
                "run.py",
                "--hns-rcu-bucket=hns-rcu-bkt",
            ],
            {
                "GCSFS_STANDARD_RCU_TEST_BUCKET": "",
                "GCSFS_HNS_RCU_TEST_BUCKET": "hns-rcu-bkt",
            },
        ),
    ],
)
def test_main_rcu_cli_args_and_environment(cli_argv, expected_env, tmp_path):
    with (
        mock.patch("sys.argv", cli_argv),
        mock.patch.dict(
            os.environ,
            {"GCSFS_BENCHMARK_BUCKET_TYPES": "stale_type"},
            clear=True,
        ),
        mock.patch.object(run, "_run_benchmarks", return_value=None) as mock_run_bench,
        mock.patch("os.makedirs"),
    ):
        run.main()
        mock_run_bench.assert_called_once()
        for key, value in expected_env.items():
            assert os.environ[key] == value
        if "GCSFS_BENCHMARK_BUCKET_TYPES" not in expected_env:
            assert "GCSFS_BENCHMARK_BUCKET_TYPES" not in os.environ


@mock.patch("subprocess.run")
@mock.patch("os.path.isdir", return_value=True)
def test_run_benchmarks(mock_isdir, mock_subprocess):
    args = argparse.Namespace(group="read", log="true", log_level="INFO")
    results_dir = "/tmp/results"

    expected_json_path = os.path.join(results_dir, "results.json")

    ret = run._run_benchmarks(results_dir, args)

    assert ret == expected_json_path
    mock_subprocess.assert_called_once()
    cmd = mock_subprocess.call_args[0][0]

    # Verify command construction
    assert "pytest" in cmd
    assert f"--benchmark-json={expected_json_path}" in cmd
    assert "log_cli=true" in cmd
    assert "log_cli_level=INFO" in cmd


def test_process_benchmark_result():
    bench = {
        "name": "test_bench",
        "group": "read",
        "extra_info": {"file_size": 100, "files": 2},
        "stats": {"min": 0.1, "data": [0.1, 0.2]},
    }
    headers = ["name", "group", "file_size", "files", "min", "p90"]
    extra = ["file_size", "files"]
    stats = ["min"]

    row = run._process_benchmark_result(bench, headers, extra, stats)

    assert row["name"] == "test_bench"
    assert row["group"] == "read"
    assert row["file_size"] == 100
    assert "p90" in row


def test_generate_report():
    json_data = {
        "benchmarks": [
            {
                "name": "b1",
                "group": "g1",
                "extra_info": {"f1": 1},
                "stats": {"min": 1.0, "data": [1.0]},
            }
        ]
    }

    # Mock json.load to return data, and open to simulate file operations
    with (
        mock.patch("json.load", return_value=json_data),
        mock.patch("builtins.open", mock.mock_open()) as mock_file,
    ):

        report_path = run._generate_report("results.json", "/tmp")

        assert report_path == os.path.join("/tmp", "results.csv")
        # Verify file was opened for writing CSV
        mock_file.assert_any_call(report_path, "w", newline="")


@pytest.mark.parametrize(
    "empty_payload",
    [
        pytest.param({}, id="empty_dict"),
        pytest.param({"benchmarks": []}, id="empty_benchmarks_list"),
    ],
)
def test_generate_report_empty_json(empty_payload):
    # Arrange
    with (
        mock.patch("json.load", return_value=empty_payload),
        mock.patch("builtins.open", mock.mock_open()),
    ):
        # Act
        report_path = run._generate_report("results.json", "/tmp")

    # Assert
    assert report_path is None


def test_format_mb():
    assert run._format_mb("N/A") == "N/A"
    # MB is 1024*1024. 2.5 MB
    val = 1024 * 1024 * 2.5
    assert run._format_mb(val) == "2.50"


def test_create_table_row():
    row = {
        "bucket_type": "regional",
        "group": "read",
        "seq_probability": 0.6,
        "pattern": "seq",
        "files": 1,
        "folders": 0,
        "threads": 1,
        "processes": 1,
        "depth": 0,
        "target_type": "file",
        "file_size": 1024 * 1024,
        "chunk_size": 1024,
        "block_size": 1024,
        "min": 1.0,
        "mean": 1.1,
        "max_throughput": 100.0,
        "cpu_max_global": 50.0,
        "mem_max": 200.0,
    }
    table_row = run._create_table_row(row)
    assert table_row[0] == "regional"
    assert table_row[3] == 0.6
    assert table_row[10] == "1.00"  # file size MB
    assert table_row[19] == "1.1000"  # mean latency
    assert table_row[20] == "0.91"  # 1 MB / 1.1s = 0.91 MB/s


def test_create_table_row_with_total_bytes():
    row = {
        "group": "cat_ranges",
        "total_bytes": 10 * 1024 * 1024,
        "file_size": 100 * 1024 * 1024,
        "files": 1,
        "mean": 2.0,
    }
    table_row = run._create_table_row(row)
    assert table_row[20] == "5.00"  # 10 MB / 2.0s = 5.00 MB/s, NOT 100 MB / 2.0s


@pytest.mark.parametrize("bad_total_bytes", ["missing", None, "N/A"])
def test_create_table_row_cat_ranges_missing_total_bytes(bad_total_bytes):
    row = {
        "group": "cat_ranges",
        "file_size": 1000 * 1024 * 1024,
        "files": 1,
        "mean": 2.0,
    }
    if bad_total_bytes != "missing":
        row["total_bytes"] = bad_total_bytes

    with pytest.raises(
        ValueError,
        match="Missing 'total_bytes' for cat_ranges benchmark",
    ):
        run._create_table_row(row)


@mock.patch("gcsfs.tests.perf.microbenchmarks.run.PrettyTable")
def test_print_csv_to_shell(mock_table):
    csv_content = "header1,header2\nval1,val2"
    with mock.patch("builtins.open", mock.mock_open(read_data=csv_content)):
        run._print_csv_to_shell("report.csv")
        mock_table.return_value.add_row.assert_called()


def test_main_exits_nonzero_when_no_report(tmp_path):
    # Arrange
    with (
        mock.patch("sys.argv", ["run.py", "--regional-bucket", "b"]),
        mock.patch.object(run, "_setup_environment"),
        mock.patch.object(
            run, "_run_benchmarks", return_value=str(tmp_path / "results.json")
        ),
        mock.patch.object(run, "_generate_report", return_value=None),
        pytest.raises(SystemExit) as exc,
    ):
        # Act
        run.main()

    # Assert
    assert exc.value.code == 1
