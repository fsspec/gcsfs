import json

from gcsfs.tests.perf.microbenchmarks import compare


def test_format_duration():
    assert compare.format_duration(None) == "N/A"
    assert "ns" in compare.format_duration(5e-10)
    assert "µs" in compare.format_duration(5e-5)
    assert "ms" in compare.format_duration(0.005)
    assert "s" in compare.format_duration(2.5)


def test_format_throughput():
    assert compare.format_throughput(None) == "N/A"
    assert compare.format_throughput(123.456) == "123.46 MB/s"


def test_extract_benchmark_metric_standard():
    bench = {
        "stats": {"mean": 0.05},
        "extra_info": {},
    }
    metric, val, unit, higher_is_better = compare.extract_benchmark_metric(bench)
    assert metric == "Mean Latency"
    assert val == 0.05
    assert unit == "s"
    assert higher_is_better is False


def test_extract_benchmark_metric_fixed_duration():
    bench = {
        "stats": {"mean": 0.0001},
        "extra_info": {
            "runtime": "10",
            "mean_run": 100 * 1024 * 1024,  # 100 MB in 10s = 10 MB/s
        },
    }
    metric, val, unit, higher_is_better = compare.extract_benchmark_metric(bench)
    assert metric == "Throughput"
    assert abs(val - 10.0) < 1e-4
    assert unit == "MB/s"
    assert higher_is_better is True


def test_extract_benchmark_metric_multi_process():
    bench = {
        "stats": {"mean": 0.0001},
        "extra_info": {
            "mean_run": 1.25,
        },
    }
    metric, val, unit, higher_is_better = compare.extract_benchmark_metric(bench)
    assert metric == "Mean Latency"
    assert val == 1.25
    assert unit == "s"
    assert higher_is_better is False


def test_compare_runs_latency():
    base = {
        "bench_pass": {"name": "bench_pass", "stats": {"mean": 1.0}},
        "bench_fail": {"name": "bench_fail", "stats": {"mean": 1.0}},
        "bench_improved": {"name": "bench_improved", "stats": {"mean": 1.0}},
        "bench_exact": {"name": "bench_exact", "stats": {"mean": 1.0}},
    }
    pr = {
        "bench_pass": {"name": "bench_pass", "stats": {"mean": 1.04}},  # +4% (pass)
        "bench_fail": {"name": "bench_fail", "stats": {"mean": 1.06}},  # +6% (fail >5%)
        "bench_improved": {
            "name": "bench_improved",
            "stats": {"mean": 0.90},
        },  # -10% (improved)
        "bench_exact": {
            "name": "bench_exact",
            "stats": {"mean": 1.05},
        },  # +5% (exact threshold, pass)
    }

    comparisons, summary = compare.compare_runs(base, pr, threshold_pct=5.0)

    res_by_name = {c["id"]: c for c in comparisons}
    assert res_by_name["bench_pass"]["status"] == "NO_CHANGE"
    assert not res_by_name["bench_pass"]["is_regression"]

    assert res_by_name["bench_fail"]["status"] == "REGRESSION"
    assert res_by_name["bench_fail"]["is_regression"]

    assert res_by_name["bench_improved"]["status"] == "IMPROVED"
    assert not res_by_name["bench_improved"]["is_regression"]

    assert res_by_name["bench_exact"]["status"] == "NO_CHANGE"
    assert not res_by_name["bench_exact"]["is_regression"]

    assert summary["regressions"] == 1
    assert summary["improvements"] == 1
    assert summary["unchanged"] == 2
    assert summary["has_regression"] is True


def test_compare_runs_throughput():
    base = {
        "bench_tp_fail": {
            "name": "bench_tp_fail",
            "stats": {"mean": 0},
            "extra_info": {"runtime": "1", "mean_run": 100 * 1024 * 1024},
        },
        "bench_tp_pass": {
            "name": "bench_tp_pass",
            "stats": {"mean": 0},
            "extra_info": {"runtime": "1", "mean_run": 100 * 1024 * 1024},
        },
    }
    pr = {
        "bench_tp_fail": {
            "name": "bench_tp_fail",
            "stats": {"mean": 0},
            "extra_info": {
                "runtime": "1",
                "mean_run": 93 * 1024 * 1024,
            },  # -7% (regression)
        },
        "bench_tp_pass": {
            "name": "bench_tp_pass",
            "stats": {"mean": 0},
            "extra_info": {
                "runtime": "1",
                "mean_run": 110 * 1024 * 1024,
            },  # +10% (improvement)
        },
    }

    comparisons, summary = compare.compare_runs(base, pr, threshold_pct=5.0)
    res = {c["id"]: c for c in comparisons}

    assert res["bench_tp_fail"]["is_regression"] is True
    assert res["bench_tp_fail"]["status"] == "REGRESSION"

    assert res["bench_tp_pass"]["is_regression"] is False
    assert res["bench_tp_pass"]["status"] == "IMPROVED"


def test_compare_runs_new_and_removed():
    base = {
        "old_bench": {"name": "old_bench", "stats": {"mean": 1.0}},
    }
    pr = {
        "new_bench": {"name": "new_bench", "stats": {"mean": 2.0}},
    }

    comparisons, summary = compare.compare_runs(base, pr, threshold_pct=5.0)
    res = {c["id"]: c for c in comparisons}

    assert res["old_bench"]["status"] == "REMOVED"
    assert res["new_bench"]["status"] == "NEW"
    assert summary["new"] == 1
    assert summary["removed"] == 1
    assert summary["has_regression"] is False


def test_markdown_and_console_generation():
    base = {
        "b1": {"name": "b1", "param": "b1_param", "stats": {"mean": 0.01}},
    }
    pr = {
        "b1": {
            "name": "b1",
            "param": "b1_param",
            "stats": {"mean": 0.012},
        },  # +20% regression
    }

    comparisons, summary = compare.compare_runs(base, pr, threshold_pct=5.0)
    console_out = compare.generate_console_table(comparisons, summary)
    assert "b1_param" in console_out
    assert "FAIL (Regression)" in console_out

    md_out = compare.generate_markdown_report(comparisons, summary, "main", "my_pr")
    assert "<!-- gcsfs-perf-benchmark-report -->" in md_out
    assert "Performance Regression Detected" in md_out
    assert "`main`" in md_out
    assert "`my_pr`" in md_out
    assert "b1_param" in md_out


def test_main_cli_success(tmp_path):
    base_file = tmp_path / "base.json"
    pr_file = tmp_path / "pr.json"
    md_file = tmp_path / "report.md"
    json_file = tmp_path / "report.json"

    data_base = {"benchmarks": [{"name": "test1", "stats": {"mean": 1.0}}]}
    data_pr = {"benchmarks": [{"name": "test1", "stats": {"mean": 1.01}}]}  # +1% (pass)

    base_file.write_text(json.dumps(data_base))
    pr_file.write_text(json.dumps(data_pr))

    ret = compare.main(
        [
            str(base_file),
            str(pr_file),
            "--threshold=5.0",
            "--output-markdown",
            str(md_file),
            "--output-json",
            str(json_file),
        ]
    )

    assert ret == 0
    assert md_file.exists()
    assert "Performance Checks Passed" in md_file.read_text()
    assert json_file.exists()
    assert json.loads(json_file.read_text())["summary"]["has_regression"] is False


def test_main_cli_failure(tmp_path):
    base_file = tmp_path / "base.json"
    pr_file = tmp_path / "pr.json"

    data_base = {"benchmarks": [{"name": "test1", "stats": {"mean": 1.0}}]}
    data_pr = {
        "benchmarks": [{"name": "test1", "stats": {"mean": 1.10}}]
    }  # +10% (fail >5%)

    base_file.write_text(json.dumps(data_base))
    pr_file.write_text(json.dumps(data_pr))

    ret = compare.main(
        [
            str(base_file),
            str(pr_file),
            "--threshold=5.0",
        ]
    )

    assert ret == 1
