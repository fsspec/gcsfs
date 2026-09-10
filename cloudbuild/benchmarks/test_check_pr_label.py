"""Unit tests for check_pr_label.py."""

import json
import os
import tempfile
import unittest
import urllib.error
from unittest.mock import MagicMock, patch

from cloudbuild.benchmarks import check_pr_label


class TestCheckPrLabel(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.skip_file = os.path.join(self.temp_dir.name, "SKIPPED")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_empty_required_label_proceeds(self):
        result = check_pr_label.check_pr_label(
            repo="fsspec/gcsfs",
            pr_number="123",
            required_label="",
            skip_file=self.skip_file,
        )
        self.assertTrue(result)
        self.assertFalse(os.path.exists(self.skip_file))

    def test_empty_pr_number_proceeds(self):
        result = check_pr_label.check_pr_label(
            repo="fsspec/gcsfs",
            pr_number="",
            required_label="perf-test",
            skip_file=self.skip_file,
        )
        self.assertTrue(result)
        self.assertFalse(os.path.exists(self.skip_file))

    @patch("urllib.request.urlopen")
    def test_matching_label_proceeds(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(
            {"labels": [{"name": "perf-test"}, {"name": "bug"}]}
        ).encode("utf-8")
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        result = check_pr_label.check_pr_label(
            repo="fsspec/gcsfs",
            pr_number="123",
            required_label="perf-test",
            skip_file=self.skip_file,
        )
        self.assertTrue(result)
        self.assertFalse(os.path.exists(self.skip_file))

    @patch("urllib.request.urlopen")
    def test_case_insensitive_matching(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(
            {"labels": [{"name": "Perf-Test"}]}
        ).encode("utf-8")
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        result = check_pr_label.check_pr_label(
            repo="fsspec/gcsfs",
            pr_number="123",
            required_label="perf-test",
            skip_file=self.skip_file,
        )
        self.assertTrue(result)
        self.assertFalse(os.path.exists(self.skip_file))

    @patch("urllib.request.urlopen")
    def test_multiple_target_labels(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(
            {"labels": [{"name": "run-perf"}]}
        ).encode("utf-8")
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        result = check_pr_label.check_pr_label(
            repo="fsspec/gcsfs",
            pr_number="123",
            required_label="perf-test,run-perf",
            skip_file=self.skip_file,
        )
        self.assertTrue(result)
        self.assertFalse(os.path.exists(self.skip_file))

    @patch("urllib.request.urlopen")
    def test_missing_label_writes_skip_file(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(
            {"labels": [{"name": "enhancement"}, {"name": "documentation"}]}
        ).encode("utf-8")
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        result = check_pr_label.check_pr_label(
            repo="fsspec/gcsfs",
            pr_number="123",
            required_label="perf-test",
            skip_file=self.skip_file,
        )
        self.assertFalse(result)
        self.assertTrue(os.path.exists(self.skip_file))
        with open(self.skip_file) as f:
            content = f.read()
        self.assertIn("PR #123 does not have required label 'perf-test'", content)

    @patch("urllib.request.urlopen")
    def test_http_error_proceeds_gracefully(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url="http://foo", code=404, msg="Not Found", hdrs={}, fp=None
        )

        result = check_pr_label.check_pr_label(
            repo="fsspec/gcsfs",
            pr_number="123",
            required_label="perf-test",
            skip_file=self.skip_file,
        )
        self.assertTrue(result)
        self.assertFalse(os.path.exists(self.skip_file))


if __name__ == "__main__":
    unittest.main()
