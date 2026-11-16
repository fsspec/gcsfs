"""Tests for the weekly Helm pin bump (scripts/bump_helm.py)."""

import hashlib
import importlib.util
import re

import pytest

from .test_rcu_scripts import REPO

SCRIPT = REPO / "cloudbuild/macrobenchmarks/scripts/bump_helm.py"
_spec = importlib.util.spec_from_file_location("bump_helm", SCRIPT)
bump_helm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bump_helm)

LIB = """# header
HELM_VERSION="v4.3.2"
HELM_SHA256="{old}"

install_helm() {{ :; }}
""".format(
    old="a" * 64
)

TARBALL = b"fake helm v4.4.0 tarball"
NEW_SHA = hashlib.sha256(TARBALL).hexdigest()
# Shape of the checksum line in a real GitHub release body.
NOTES = (
    "## Installation and Upgrading\n"
    "- [Linux amd64](https://get.helm.sh/helm-v4.4.0-linux-amd64.tar.gz) "
    "([checksum](https://get.helm.sh/helm-v4.4.0-linux-amd64.tar.gz.sha256sum)"
    f" / {NEW_SHA})\n"
    "- [Linux arm64](https://get.helm.sh/helm-v4.4.0-linux-arm64.tar.gz) "
    f"([checksum](https://x) / {'b' * 64})\n"
)
RELEASES = [
    {"tag_name": "v5.0.0", "prerelease": False, "body": ""},
    {"tag_name": "v4.5.0-rc.1", "prerelease": True, "body": ""},
    {"tag_name": "v4.4.0", "prerelease": False, "body": NOTES},
    {"tag_name": "v3.22.2", "prerelease": False, "body": ""},
    {"tag_name": "v4.3.2", "prerelease": False, "body": ""},
]


def _fetchers(releases=RELEASES, sha_file=None, tarball=TARBALL):
    sha_file = sha_file or f"{NEW_SHA}  helm-v4.4.0-linux-amd64.tar.gz\n"
    calls = []

    def fetch_json(url):
        calls.append(url)
        return releases

    def fetch_text(url):
        calls.append(url)
        assert url.endswith("helm-v4.4.0-linux-amd64.tar.gz.sha256sum")
        return sha_file

    def fetch_bytes(url):
        calls.append(url)
        assert url == "https://get.helm.sh/helm-v4.4.0-linux-amd64.tar.gz"
        return tarball

    return {"fetch_json": fetch_json, "fetch_text": fetch_text}, fetch_bytes, calls


def test_latest_release_picks_the_highest_stable_release_in_the_same_major():
    assert bump_helm.latest_release(RELEASES, major=4) == "v4.4.0"
    assert bump_helm.latest_release(RELEASES, major=6) is None


def test_latest_release_compares_versions_numerically():
    releases = [{"tag_name": t, "prerelease": False} for t in ("v4.9.0", "v4.10.0")]
    assert bump_helm.latest_release(releases, major=4) == "v4.10.0"


def test_read_pin_parses_the_real_lib_sh():
    version, sha = bump_helm.read_pin(
        (REPO / "cloudbuild/macrobenchmarks/scripts/lib.sh").read_text()
    )
    assert re.fullmatch(r"v\d+\.\d+\.\d+", version)
    assert re.fullmatch(r"[0-9a-f]{64}", sha)


def test_release_notes_sha_reads_the_linux_amd64_checksum():
    assert bump_helm.release_notes_sha(NOTES) == NEW_SHA
    with pytest.raises(ValueError):
        bump_helm.release_notes_sha("no checksums here")


def test_run_bumps_the_pin_to_the_newest_release_in_the_same_major(tmp_path):
    lib = tmp_path / "lib.sh"
    lib.write_text(LIB)
    fetch, fetch_bytes, _ = _fetchers()

    assert bump_helm.run(lib, fetch_bytes=fetch_bytes, **fetch) == "v4.4.0"

    assert lib.read_text() == LIB.replace("v4.3.2", "v4.4.0").replace("a" * 64, NEW_SHA)


def test_run_leaves_the_pin_alone_when_it_is_current(tmp_path):
    lib = tmp_path / "lib.sh"
    lib.write_text(LIB.replace("v4.3.2", "v4.4.0"))
    fetch, fetch_bytes, calls = _fetchers()

    assert bump_helm.run(lib, fetch_bytes=fetch_bytes, **fetch) is None

    assert lib.read_text() == LIB.replace("v4.3.2", "v4.4.0")
    assert len(calls) == 1  # only the release listing


def test_run_refuses_when_the_two_checksum_sources_disagree(tmp_path):
    lib = tmp_path / "lib.sh"
    lib.write_text(LIB)
    fetch, fetch_bytes, _ = _fetchers(sha_file=f"{'c' * 64}  helm.tar.gz\n")

    with pytest.raises(ValueError, match="disagree"):
        bump_helm.run(lib, fetch_bytes=fetch_bytes, **fetch)

    assert lib.read_text() == LIB


def test_run_refuses_when_the_tarball_does_not_match_the_checksum(tmp_path):
    lib = tmp_path / "lib.sh"
    lib.write_text(LIB)
    fetch, fetch_bytes, _ = _fetchers(tarball=b"tampered")

    with pytest.raises(ValueError, match="tarball"):
        bump_helm.run(lib, fetch_bytes=fetch_bytes, **fetch)

    assert lib.read_text() == LIB
