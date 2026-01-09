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


@mock.patch("subprocess.run")
def test_run_benchmarks(mock_subprocess):
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
    headers = ["name", "group", "file_size", "files", "min", "p90", "max_throughput"]
    extra = ["file_size", "files"]
    stats = ["min"]

    row = run._process_benchmark_result(bench, headers, extra, stats)

    assert row["name"] == "test_bench"
    assert row["group"] == "read"
    assert row["file_size"] == 100
    # Throughput = (100 * 2) / 0.1 = 2000.0
    assert row["max_throughput"] == 2000.0
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


def test_generate_report_empty_json():
    with (
        mock.patch("json.load", return_value={}),
        mock.patch("builtins.open", mock.mock_open()),
    ):

        report_path = run._generate_report("results.json", "/tmp")
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
        "pattern": "seq",
        "files": 1,
        "folders": 0,
        "threads": 1,
        "processes": 1,
        "depth": 0,
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
    assert table_row[8] == "1.00"  # file size MB


@mock.patch("gcsfs.tests.perf.microbenchmarks.run.PrettyTable")
def test_print_csv_to_shell(mock_table):
    csv_content = "header1,header2\nval1,val2"
    with mock.patch("builtins.open", mock.mock_open(read_data=csv_content)):
        run._print_csv_to_shell("report.csv")
        mock_table.return_value.add_row.assert_called()
