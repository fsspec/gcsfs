"""Offline tests for the download path of lib.sh's install_helm.

A stub ``curl`` serves a fixture tarball, so ``sha256sum -c``, ``tar``, the temp
dir cleanup and the PATH update run for real without network access.
"""

import hashlib
import io
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[4]
LIB = "cloudbuild/macrobenchmarks/scripts/lib.sh"

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")

CURL_STUB = r"""#!/usr/bin/env bash
while [[ $# -gt 0 ]]; do
  [[ "$1" == "-o" ]] && cp "$STUB_TARBALL" "$2"
  shift
done
exit 0
"""

# A different helm already on PATH, so install_helm has to download.
OLD_HELM_STUB = """#!/usr/bin/env bash
[[ "$1" == version ]] && { echo v0.0.0; exit 0; }
echo old-helm
"""


def _write_exe(path, body):
    path.write_text(body)
    path.chmod(0o755)


@pytest.fixture
def release(tmp_path):
    """A release tarball whose helm prints ``pinned-helm``, and its SHA-256."""
    body = b"#!/usr/bin/env bash\necho pinned-helm\n"
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo("linux-amd64/helm")
        info.size = len(body)
        info.mode = 0o755
        tar.addfile(info, io.BytesIO(body))
    tarball = tmp_path / "helm.tar.gz"
    tarball.write_bytes(buf.getvalue())
    return tarball, hashlib.sha256(buf.getvalue()).hexdigest()


def _install_helm(tmp_path, tarball, pinned_sha256):
    """Run install_helm against ``pinned_sha256``, then whichever helm is on PATH."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_exe(bin_dir / "curl", CURL_STUB)
    _write_exe(bin_dir / "helm", OLD_HELM_STUB)
    tmpdir = tmp_path / "tmpdir"
    tmpdir.mkdir()
    proc = subprocess.run(
        [
            "bash",
            "-c",
            f'source {LIB}\nHELM_SHA256="$1"\ninstall_helm; rc=$?\nhelm\nexit $rc',
            "bash",
            pinned_sha256,
        ],
        cwd=REPO,
        env={
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "TMPDIR": str(tmpdir),
            "WORKSPACE_DIR": str(tmp_path),
            "STUB_TARBALL": str(tarball),
        },
        capture_output=True,
        text=True,
    )
    return proc, tmpdir


def test_install_helm_installs_the_verified_release(tmp_path, release):
    tarball, sha256 = release

    proc, tmpdir = _install_helm(tmp_path, tarball, sha256)

    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == "pinned-helm\n"
    # Only the unpacked binary is left; the tarball is deleted.
    assert [p.name for p in tmpdir.rglob("*") if p.is_file()] == ["helm"]


def test_install_helm_rejects_a_checksum_mismatch(tmp_path, release):
    tarball, _ = release

    proc, tmpdir = _install_helm(tmp_path, tarball, "0" * 64)

    assert proc.returncode == 1
    assert "does not match the pinned SHA-256 checksum" in proc.stderr
    # Nothing unpacked onto PATH, and the temp dir is removed.
    assert proc.stdout == "old-helm\n"
    assert list(tmpdir.iterdir()) == []
