"""Unit tests for check_pr_label.py."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from cloudbuild.benchmarks import check_pr_label


class TestCheckPrLabel(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.skip_file = str(Path(self.temp_dir.name) / "SKIPPED")

    def tearDown(self):
        self.temp_dir.cleanup()

    @patch("urllib.request.urlopen")
    def test_pr_with_label_proceeds(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(
            {"labels": [{"name": "Help Wanted"}]}
        ).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        result = check_pr_label.check_pr_label(
            repo="fsspec/gcsfs",
            pr_number="123",
            required_label="help wanted",
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
            required_label="help wanted",
            skip_file=self.skip_file,
        )
        self.assertFalse(result)
        self.assertTrue(Path(self.skip_file).exists())


if __name__ == "__main__":
    unittest.main()
