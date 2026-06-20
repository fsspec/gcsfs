import datetime
import os
import subprocess
import sys

import pytest

# Add the directory containing prepare_release.py to the path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
import prepare_release


def test_parse_version():
    assert prepare_release.parse_version("2026.4.0") == (2026, 4, 0)
    assert prepare_release.parse_version("2026.12.5-post1") == (2026, 12, 5)

    with pytest.raises(ValueError, match="does not match expected CalVer pattern"):
        prepare_release.parse_version("v2026.4.0")
    with pytest.raises(ValueError, match="does not match expected CalVer pattern"):
        prepare_release.parse_version("abc")
    with pytest.raises(ValueError, match="does not match expected CalVer pattern"):
        prepare_release.parse_version("2026.4")


def test_calculate_next_version(monkeypatch):
    class MockDatetime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 4, 15, tzinfo=datetime.timezone.utc)

    monkeypatch.setattr("prepare_release.datetime.datetime", MockDatetime)

    # Same month: increment patch
    assert prepare_release.calculate_next_version("2026.4.0") == "2026.4.1"
    assert prepare_release.calculate_next_version("2026.4.5") == "2026.4.6"

    # New month: reset patch to 0
    assert prepare_release.calculate_next_version("2026.3.5") == "2026.4.0"
    assert prepare_release.calculate_next_version("2025.12.10") == "2026.4.0"

    # Regression guard: new version (2026.4.0) must be newer than latest tag (2026.5.0)
    with pytest.raises(ValueError, match="Potential version regression"):
        prepare_release.calculate_next_version("2026.5.0")

    # Regression guard: new version (2026.4.0) must be newer than latest tag (2027.1.0)
    with pytest.raises(ValueError, match="Potential version regression"):
        prepare_release.calculate_next_version("2027.1.0")


def test_get_latest_tag(monkeypatch):
    # Success case
    monkeypatch.setattr("prepare_release.run_cmd", lambda args: "2026.4.0")
    assert prepare_release.get_latest_tag() == "2026.4.0"

    # Error case
    def mock_run_cmd_fail(args):
        raise subprocess.CalledProcessError(1, args, stderr="git error")

    monkeypatch.setattr("prepare_release.run_cmd", mock_run_cmd_fail)
    with pytest.raises(subprocess.CalledProcessError):
        prepare_release.get_latest_tag()


def test_get_changelog_entries(monkeypatch):
    # Success case with commits
    def mock_run_cmd_commits(args):
        assert args == [
            "git",
            "log",
            "2026.4.0..HEAD",
            "--no-merges",
            "--pretty=format:* %s",
        ]
        return "* Commit 1 (abc1234)\n* Commit 2 (def5678)"

    monkeypatch.setattr("prepare_release.run_cmd", mock_run_cmd_commits)
    entries = prepare_release.get_changelog_entries("2026.4.0")
    assert entries == ["* Commit 1 (abc1234)", "* Commit 2 (def5678)"]

    # Success case with no commits (empty string)
    monkeypatch.setattr("prepare_release.run_cmd", lambda args: "")
    entries = prepare_release.get_changelog_entries("2026.4.0")
    assert entries == ["* No changes (released in sync with fsspec)."]

    # Error case: a git failure must propagate, not be swallowed into an
    # empty/placeholder changelog.
    def mock_run_cmd_fail(args):
        raise subprocess.CalledProcessError(1, args, stderr="git log error")

    monkeypatch.setattr("prepare_release.run_cmd", mock_run_cmd_fail)
    with pytest.raises(subprocess.CalledProcessError):
        prepare_release.get_changelog_entries("2026.4.0")


def test_update_changelog_file(tmp_path):
    changelog = tmp_path / "changelog.rst"
    initial_content = """Changelog
=========

2026.4.0
--------

* Previous change
"""
    changelog.write_text(initial_content, encoding="utf-8")

    prepare_release.update_changelog_file(str(changelog), "2026.4.1", ["* New feature"])

    expected_content = """Changelog
=========

2026.4.1
--------

* New feature

2026.4.0
--------

* Previous change
"""
    assert changelog.read_text(encoding="utf-8") == expected_content


def test_update_changelog_file_with_suffix(tmp_path):
    changelog = tmp_path / "changelog.rst"
    initial_content = """Changelog
=========

2025.5.0post1
-------------

* Previous change
"""
    changelog.write_text(initial_content, encoding="utf-8")

    prepare_release.update_changelog_file(str(changelog), "2026.4.1", ["* New feature"])

    expected_content = """Changelog
=========

2026.4.1
--------

* New feature

2025.5.0post1
-------------

* Previous change
"""
    assert changelog.read_text(encoding="utf-8") == expected_content


def test_update_changelog_file_no_header(tmp_path):
    changelog = tmp_path / "changelog.rst"
    changelog.write_text("No header here", encoding="utf-8")

    with pytest.raises(ValueError, match="Could not find a valid version header"):
        prepare_release.update_changelog_file(
            str(changelog), "2026.4.1", ["* New feature"]
        )


def test_update_changelog_file_short_underline(tmp_path):
    changelog = tmp_path / "changelog.rst"
    initial_content = """Changelog
=========

2026.4.0
--

* Previous change
"""
    changelog.write_text(initial_content, encoding="utf-8")

    with pytest.raises(ValueError, match="Could not find a valid version header"):
        prepare_release.update_changelog_file(
            str(changelog), "2026.4.1", ["* New feature"]
        )
