import json
import os
import sys
import tempfile
import unittest

try:
    from gcsfs.tests.perf.microbenchmarks import compare
except (ImportError, ModuleNotFoundError):
    sys.path.insert(0, os.path.dirname(__file__))
    import compare  # type: ignore


class TestCompare(unittest.TestCase):

    def test_format_duration(self):
        self.assertEqual(compare.format_duration(None), "N/A")
        self.assertIn("ns", compare.format_duration(5e-10))
        self.assertIn("µs", compare.format_duration(5e-5))
        self.assertIn("ms", compare.format_duration(0.005))
        self.assertIn("s", compare.format_duration(2.5))

    def test_format_throughput(self):
        self.assertEqual(compare.format_throughput(None), "N/A")
        self.assertEqual(compare.format_throughput(123.456), "123.46 MB/s")

    def test_extract_benchmark_metric_standard(self):
        bench = {
            "stats": {"mean": 0.05},
            "extra_info": {},
        }
        metric, val, unit, higher_is_better = compare.extract_benchmark_metric(bench)
        self.assertEqual(metric, "Mean Latency")
        self.assertEqual(val, 0.05)
        self.assertEqual(unit, "s")
        self.assertFalse(higher_is_better)

    def test_extract_benchmark_metric_fixed_duration(self):
        bench = {
            "stats": {"mean": 0.0001},
            "extra_info": {
                "runtime": "10",
                "mean_run": 100 * 1024 * 1024,  # 100 MB in 10s = 10 MB/s
            },
        }
        metric, val, unit, higher_is_better = compare.extract_benchmark_metric(bench)
        self.assertEqual(metric, "Throughput")
        self.assertAlmostEqual(val, 10.0, places=4)
        self.assertEqual(unit, "MB/s")
        self.assertTrue(higher_is_better)

    def test_extract_benchmark_metric_multi_process(self):
        bench = {
            "stats": {"mean": 0.0001},
            "extra_info": {
                "mean_run": 1.25,
            },
        }
        metric, val, unit, higher_is_better = compare.extract_benchmark_metric(bench)
        self.assertEqual(metric, "Mean Latency")
        self.assertEqual(val, 1.25)
        self.assertEqual(unit, "s")
        self.assertFalse(higher_is_better)

    def test_compare_runs_latency(self):
        base = {
            "bench_pass": {"name": "bench_pass", "stats": {"mean": 1.0}},
            "bench_fail": {"name": "bench_fail", "stats": {"mean": 1.0}},
            "bench_improved": {"name": "bench_improved", "stats": {"mean": 1.0}},
            "bench_exact": {"name": "bench_exact", "stats": {"mean": 1.0}},
        }
        pr = {
            "bench_pass": {"name": "bench_pass", "stats": {"mean": 1.04}},
            "bench_fail": {"name": "bench_fail", "stats": {"mean": 1.06}},
            "bench_improved": {
                "name": "bench_improved",
                "stats": {"mean": 0.90},
            },
            "bench_exact": {
                "name": "bench_exact",
                "stats": {"mean": 1.05},
            },
        }

        comparisons, summary = compare.compare_runs(base, pr, threshold_pct=5.0)

        res_by_name = {c["id"]: c for c in comparisons}
        self.assertEqual(res_by_name["bench_pass"]["status"], "NO_CHANGE")
        self.assertFalse(res_by_name["bench_pass"]["is_regression"])

        self.assertEqual(res_by_name["bench_fail"]["status"], "REGRESSION")
        self.assertTrue(res_by_name["bench_fail"]["is_regression"])

        self.assertEqual(res_by_name["bench_improved"]["status"], "IMPROVED")
        self.assertFalse(res_by_name["bench_improved"]["is_regression"])

        self.assertEqual(res_by_name["bench_exact"]["status"], "NO_CHANGE")
        self.assertFalse(res_by_name["bench_exact"]["is_regression"])

        self.assertEqual(summary["regressions"], 1)
        self.assertEqual(summary["improvements"], 1)
        self.assertEqual(summary["unchanged"], 2)
        self.assertTrue(summary["has_regression"])

    def test_compare_runs_throughput(self):
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
                },
            },
            "bench_tp_pass": {
                "name": "bench_tp_pass",
                "stats": {"mean": 0},
                "extra_info": {
                    "runtime": "1",
                    "mean_run": 110 * 1024 * 1024,
                },
            },
        }

        comparisons, summary = compare.compare_runs(base, pr, threshold_pct=5.0)
        res = {c["id"]: c for c in comparisons}

        self.assertTrue(res["bench_tp_fail"]["is_regression"])
        self.assertEqual(res["bench_tp_fail"]["status"], "REGRESSION")

        self.assertFalse(res["bench_tp_pass"]["is_regression"])
        self.assertEqual(res["bench_tp_pass"]["status"], "IMPROVED")

    def test_compare_runs_new_and_removed(self):
        base = {
            "old_bench": {"name": "old_bench", "stats": {"mean": 1.0}},
        }
        pr = {
            "new_bench": {"name": "new_bench", "stats": {"mean": 2.0}},
        }

        comparisons, summary = compare.compare_runs(base, pr, threshold_pct=5.0)
        res = {c["id"]: c for c in comparisons}

        self.assertEqual(res["old_bench"]["status"], "REMOVED")
        self.assertEqual(res["new_bench"]["status"], "NEW")
        self.assertEqual(summary["new"], 1)
        self.assertEqual(summary["removed"], 1)
        self.assertFalse(summary["has_regression"])

    def test_markdown_and_console_generation(self):
        base = {
            "b1": {"name": "b1", "param": "b1_param", "stats": {"mean": 0.01}},
        }
        pr = {
            "b1": {
                "name": "b1",
                "param": "b1_param",
                "stats": {"mean": 0.012},
            },
        }

        comparisons, summary = compare.compare_runs(base, pr, threshold_pct=5.0)
        console_out = compare.generate_console_table(comparisons, summary)
        self.assertIn("b1_param", console_out)
        self.assertIn("FAIL (Regression)", console_out)

        md_out = compare.generate_markdown_report(comparisons, summary, "main", "my_pr")
        self.assertIn("<!-- gcsfs-perf-benchmark-report -->", md_out)
        self.assertIn("Performance Regression Detected", md_out)
        self.assertIn("`main`", md_out)
        self.assertIn("`my_pr`", md_out)
        self.assertIn("b1_param", md_out)

    def test_main_cli_success(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_file = os.path.join(tmp_dir, "base.json")
            pr_file = os.path.join(tmp_dir, "pr.json")
            md_file = os.path.join(tmp_dir, "report.md")
            json_file = os.path.join(tmp_dir, "report.json")

            data_base = {"benchmarks": [{"name": "test1", "stats": {"mean": 1.0}}]}
            data_pr = {"benchmarks": [{"name": "test1", "stats": {"mean": 1.01}}]}

            with open(base_file, "w", encoding="utf-8") as f:
                json.dump(data_base, f)
            with open(pr_file, "w", encoding="utf-8") as f:
                json.dump(data_pr, f)

            ret = compare.main(
                [
                    base_file,
                    pr_file,
                    "--threshold=5.0",
                    "--output-markdown",
                    md_file,
                    "--output-json",
                    json_file,
                ]
            )

            self.assertEqual(ret, 0)
            self.assertTrue(os.path.exists(md_file))
            with open(md_file, "r", encoding="utf-8") as f:
                md_content = f.read()
            self.assertIn("Performance Checks Passed", md_content)
            self.assertTrue(os.path.exists(json_file))
            with open(json_file, "r", encoding="utf-8") as f:
                json_data = json.load(f)
            self.assertFalse(json_data["summary"]["has_regression"])

    def test_main_cli_failure(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_file = os.path.join(tmp_dir, "base.json")
            pr_file = os.path.join(tmp_dir, "pr.json")

            data_base = {"benchmarks": [{"name": "test1", "stats": {"mean": 1.0}}]}
            data_pr = {"benchmarks": [{"name": "test1", "stats": {"mean": 1.10}}]}

            with open(base_file, "w", encoding="utf-8") as f:
                json.dump(data_base, f)
            with open(pr_file, "w", encoding="utf-8") as f:
                json.dump(data_pr, f)

            ret = compare.main(
                [
                    base_file,
                    pr_file,
                    "--threshold=5.0",
                ]
            )

            self.assertEqual(ret, 1)


if __name__ == "__main__":
    unittest.main()
