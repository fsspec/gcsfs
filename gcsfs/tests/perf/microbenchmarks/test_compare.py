"""Unit tests for compare.py."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

try:
    from gcsfs.tests.perf.microbenchmarks import compare
except (ImportError, ModuleNotFoundError):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import compare  # type: ignore


class TestCompare(unittest.TestCase):

    def test_compare_runs(self):
        base = {
            "b_pass": {"name": "b_pass", "stats": {"mean": 1.0}},
            "b_regr": {"name": "b_regr", "stats": {"mean": 1.0}},
            "b_impr": {"name": "b_impr", "stats": {"mean": 1.0}},
        }
        pr = {
            "b_pass": {"name": "b_pass", "stats": {"mean": 1.02}},
            "b_regr": {"name": "b_regr", "stats": {"mean": 1.10}},
            "b_impr": {"name": "b_impr", "stats": {"mean": 0.85}},
        }

        comparisons, summary = compare.compare_runs(base, pr, threshold_pct=5.0)
        res = {c["id"]: c for c in comparisons}

        self.assertEqual(res["b_pass"]["status"], "NO_CHANGE")
        self.assertFalse(res["b_pass"]["is_regression"])

        self.assertEqual(res["b_regr"]["status"], "REGRESSION")
        self.assertTrue(res["b_regr"]["is_regression"])

        self.assertEqual(res["b_impr"]["status"], "IMPROVED")
        self.assertFalse(res["b_impr"]["is_regression"])

        self.assertTrue(summary["has_regression"])
        self.assertEqual(summary["regressions"], 1)
        self.assertEqual(summary["improvements"], 1)
        self.assertEqual(summary["unchanged"], 1)

    def test_main_cli_end_to_end(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            base_file = tmp_path / "base.json"
            pr_pass_file = tmp_path / "pr_pass.json"
            pr_regr_file = tmp_path / "pr_regr.json"
            md_file = tmp_path / "report.md"
            json_file = tmp_path / "report.json"

            base_file.write_text(
                json.dumps({"benchmarks": [{"name": "b1", "stats": {"mean": 1.0}}]})
            )
            pr_pass_file.write_text(
                json.dumps({"benchmarks": [{"name": "b1", "stats": {"mean": 1.01}}]})
            )
            pr_regr_file.write_text(
                json.dumps({"benchmarks": [{"name": "b1", "stats": {"mean": 1.15}}]})
            )

            ret_pass = compare.main(
                [
                    str(base_file),
                    str(pr_pass_file),
                    "--threshold=5.0",
                    "--output-markdown",
                    str(md_file),
                    "--output-json",
                    str(json_file),
                ]
            )
            self.assertEqual(ret_pass, 0)
            self.assertTrue(md_file.exists())
            self.assertIn("Performance Checks Passed", md_file.read_text())
            self.assertTrue(json_file.exists())
            self.assertFalse(
                json.loads(json_file.read_text())["summary"]["has_regression"]
            )

            # By default, do not fail workload even if regression is detected
            ret_regr_default = compare.main(
                [
                    str(base_file),
                    str(pr_regr_file),
                    "--threshold=10.0",
                ]
            )
            self.assertEqual(ret_regr_default, 0)

            # With --fail-on-regression, exit code is 1
            ret_regr_fail = compare.main(
                [
                    str(base_file),
                    str(pr_regr_file),
                    "--threshold=10.0",
                    "--fail-on-regression",
                ]
            )
            self.assertEqual(ret_regr_fail, 1)


if __name__ == "__main__":
    unittest.main()
