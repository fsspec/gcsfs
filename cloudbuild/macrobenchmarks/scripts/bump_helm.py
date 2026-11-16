#!/usr/bin/env python3
"""Bump the Helm pin in lib.sh to the newest release in the same major version.

Run weekly by .github/workflows/bump-helm.yml, which opens a PR when this
prints a new version. The new SHA-256 must agree between get.helm.sh's
.sha256sum file and the GitHub release notes, and must match the downloaded
tarball, before lib.sh is rewritten. Major-version upgrades stay manual.
"""

import hashlib
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

LIB_SH = Path(__file__).resolve().parent / "lib.sh"
RELEASES_URL = "https://api.github.com/repos/helm/helm/releases?per_page=100"
TARBALL_URL = "https://get.helm.sh/helm-{version}-linux-amd64.tar.gz"

_PIN = re.compile(r'^HELM_VERSION="(v[^"]+)"\nHELM_SHA256="([0-9a-f]{64})"$', re.M)
_STABLE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")


def read_pin(lib_text):
    """(version, sha256) pinned in lib.sh."""
    match = _PIN.search(lib_text)
    if not match:
        raise ValueError("HELM_VERSION/HELM_SHA256 pin not found in lib.sh")
    return match.group(1), match.group(2)


def _key(tag):
    return tuple(int(part) for part in _STABLE.match(tag).groups())


def latest_release(releases, major):
    """Highest stable vMAJOR.x.y tag, or None."""
    tags = [
        r["tag_name"]
        for r in releases
        if not r.get("prerelease")
        and _STABLE.match(r["tag_name"])
        and _key(r["tag_name"])[0] == major
    ]
    return max(tags, key=_key, default=None)


def release_notes_sha(body):
    """The linux-amd64 SHA-256 listed in a GitHub release body."""
    match = re.search(
        r"linux-amd64\.tar\.gz\.sha256sum\)\s*/\s*([0-9a-f]{64})", body or ""
    )
    if not match:
        raise ValueError("no linux-amd64 checksum in the release notes")
    return match.group(1)


def run(lib_path, fetch_json, fetch_text, fetch_bytes):
    """Rewrite the pin in lib_path if a newer same-major release exists.

    Returns the new version, or None when the pin is already current.
    """
    text = Path(lib_path).read_text()
    current, _ = read_pin(text)
    releases = fetch_json(RELEASES_URL)
    latest = latest_release(releases, major=_key(current)[0])
    if latest is None or _key(latest) <= _key(current):
        return None

    url = TARBALL_URL.format(version=latest)
    sha_file = fetch_text(url + ".sha256sum").split()[0]
    notes = next(r.get("body", "") for r in releases if r["tag_name"] == latest)
    if sha_file != release_notes_sha(notes):
        raise ValueError(
            f"{latest}: get.helm.sh and GitHub release notes disagree on the SHA-256"
        )
    if hashlib.sha256(fetch_bytes(url)).hexdigest() != sha_file:
        raise ValueError(f"{latest}: downloaded tarball does not match its SHA-256")

    Path(lib_path).write_text(
        _PIN.sub(f'HELM_VERSION="{latest}"\nHELM_SHA256="{sha_file}"', text)
    )
    return latest


def _get(url):
    headers = {"User-Agent": "gcsfs-bump-helm"}
    if os.environ.get("GH_TOKEN") and url.startswith("https://api.github.com/"):
        headers["Authorization"] = f"Bearer {os.environ['GH_TOKEN']}"
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers)) as r:
        return r.read()


def main():
    new = run(
        LIB_SH,
        fetch_json=lambda url: json.loads(_get(url)),
        fetch_text=lambda url: _get(url).decode(),
        fetch_bytes=_get,
    )
    if new:
        print(new)


if __name__ == "__main__":
    sys.exit(main())
