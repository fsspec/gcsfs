"""Tests for the pinned, checksum-verified Helm install (lib.sh install_helm).

The real ``install_helm`` runs under bash with a stub ``curl`` that serves a
fixture tarball, so ``sha256sum``/``tar`` and the PATH handling are exercised
for real; no network is used.
"""

import hashlib
import io
import re
import shutil
import subprocess
import tarfile

import pytest

from .test_rcu_scripts import (  # noqa: F401  (env is a fixture)
    PINNED_HELM_VERSION,
    REPO,
    SEED,
    _add_build_vars,
    _calls,
    _failures,
    _run_step,
    env,
)

LIB = "cloudbuild/macrobenchmarks/scripts/lib.sh"
PINNED_URL = f"https://get.helm.sh/helm-{PINNED_HELM_VERSION}-linux-amd64.tar.gz"

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")

CURL_STUB = r"""#!/usr/bin/env bash
echo "curl $*" >> "$STUB_LOG"
[[ -n "$STUB_CURL_FAIL" ]] && exit 22
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "-o" ]]; then cp "$STUB_TARBALL" "$2"; fi
  shift
done
exit 0
"""


def _write_exe(path, body):
    path.write_text(body)
    path.chmod(0o755)


@pytest.fixture
def tarball(tmp_path):
    """A helm release tarball whose binary prints ``installed-helm``."""
    body = b'#!/usr/bin/env bash\necho installed-helm "$@"\n'
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo("linux-amd64/helm")
        info.size = len(body)
        info.mode = 0o755
        tar.addfile(info, io.BytesIO(body))
    path = tmp_path / "helm.tar.gz"
    path.write_bytes(buf.getvalue())
    return path, hashlib.sha256(buf.getvalue()).hexdigest()


@pytest.fixture
def bin_dir(tmp_path):
    d = tmp_path / "bin"
    d.mkdir()
    _write_exe(d / "curl", CURL_STUB)
    return d


def _helm_stub(bin_dir, version):
    _write_exe(
        bin_dir / "helm",
        f'#!/usr/bin/env bash\n[[ "$1" == version ]] && {{ echo "{version}"; exit 0; }}\n'
        'echo "stub-helm $*"\n',
    )


def _run(script, bin_dir, tmp_path, tarball=None, **env):
    (tmp_path / "tmpdir").mkdir(exist_ok=True)
    run_env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin",
        "TMPDIR": str(tmp_path / "tmpdir"),
        "WORKSPACE_DIR": str(tmp_path),
        "STUB_LOG": str(tmp_path / "calls.log"),
        "STUB_TARBALL": str(tarball) if tarball else "/nonexistent",
        **env,
    }
    proc = subprocess.run(
        ["bash", "-c", f"source {LIB}\n{script}"],
        cwd=REPO,
        env=run_env,
        capture_output=True,
        text=True,
    )
    log = tmp_path / "calls.log"
    return proc, log.read_text() if log.exists() else ""


def test_install_helm_installs_the_pinned_release(bin_dir, tmp_path, tarball):
    path, sha = tarball
    _helm_stub(bin_dir, "v0.0.0")  # some other helm already on PATH

    proc, curl_log = _run(
        f"HELM_SHA256={sha}\ninstall_helm && helm", bin_dir, tmp_path, path
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "installed-helm" in proc.stdout
    assert PINNED_URL in curl_log


def test_install_helm_downloads_into_tmpdir(bin_dir, tmp_path, tarball):
    # Honouring TMPDIR keeps test runs out of the host's /tmp.
    path, sha = tarball

    proc, _ = _run(
        f"HELM_SHA256={sha}\ninstall_helm && command -v helm", bin_dir, tmp_path, path
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert proc.stdout.strip().startswith(str(tmp_path / "tmpdir"))


def test_install_helm_rejects_a_tarball_with_the_wrong_checksum(
    bin_dir, tmp_path, tarball
):
    path, _ = tarball

    proc, curl_log = _run("install_helm; echo rc=$?; helm", bin_dir, tmp_path, path)

    assert PINNED_URL in curl_log
    assert re.search(r"rc=[1-9]", proc.stdout), proc.stdout + proc.stderr
    assert "checksum" in proc.stderr.lower()
    assert "installed-helm" not in proc.stdout


def test_install_helm_fails_when_the_download_fails(bin_dir, tmp_path, tarball):
    path, sha = tarball

    proc, curl_log = _run(
        f"HELM_SHA256={sha}\ninstall_helm; echo rc=$?; helm",
        bin_dir,
        tmp_path,
        path,
        STUB_CURL_FAIL="1",
    )

    assert PINNED_URL in curl_log
    assert re.search(r"rc=[1-9]", proc.stdout), proc.stdout + proc.stderr
    assert "installed-helm" not in proc.stdout


def test_install_helm_skips_the_download_when_the_pinned_version_is_on_path(
    bin_dir, tmp_path
):
    _helm_stub(bin_dir, PINNED_HELM_VERSION)

    proc, curl_log = _run("install_helm", bin_dir, tmp_path)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert curl_log == ""


def test_install_helm_does_not_trip_the_callers_err_trap_when_helm_is_absent(
    bin_dir, tmp_path, tarball
):
    # Step scripts run with set -eE and an ERR trap that records the step as
    # failed; a successful first-time install (no helm on PATH yet) must not
    # fire it.
    path, sha = tarball

    proc, _ = _run(
        f"set -eE\ntrap 'echo ERR-TRAPPED' ERR\nHELM_SHA256={sha}\ninstall_helm\nhelm",
        bin_dir,
        tmp_path,
        path,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "ERR-TRAPPED" not in proc.stdout
    assert "installed-helm" in proc.stdout


def test_pinned_helm_version_and_checksum_are_well_formed(bin_dir, tmp_path):
    proc, _ = _run('echo "$HELM_VERSION $HELM_SHA256"', bin_dir, tmp_path)

    version, sha = proc.stdout.split()
    assert re.fullmatch(r"v\d+\.\d+\.\d+", version)
    assert re.fullmatch(r"[0-9a-f]{64}", sha)


# --- The helm steps (run like Cloud Build via the test_rcu_scripts harness) ---

PIPE_TO_SHELL = re.compile(r"\b(curl|wget)\b[^|\n]*\|\s*(sudo\s+)?(ba|z|da)?sh\b")


def test_no_cloudbuild_script_pipes_a_download_into_a_shell():
    offenders = [
        f"{path.relative_to(REPO)}:{n}"
        for path in sorted((REPO / "cloudbuild").rglob("*"))
        if path.suffix in (".sh", ".yaml", ".yml")
        for n, line in enumerate(path.read_text().splitlines(), 1)
        if PIPE_TO_SHELL.search(line)
    ]
    assert offenders == []


@pytest.mark.parametrize("step", ["run-workload", "seed-checkpoint", "cleanup-helm"])
def test_helm_steps_install_the_pinned_helm(env, step):  # noqa: F811
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/", SEEDED_CKPT_PATH=SEED)

    proc, calls = _run_step(step, env, STUB_LS=SEED)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _failures(env) == []
    helm_calls = _calls(calls, "helm ")
    assert helm_calls and helm_calls[0].startswith("helm version")
    assert not [c for c in calls if "raw.githubusercontent.com" in c]


@pytest.mark.parametrize("step", ["run-workload", "seed-checkpoint"])
def test_helm_steps_record_failure_when_helm_install_fails(env, step):  # noqa: F811
    # Another helm version on PATH forces a download; the stub curl serves
    # nothing, so the checksum check fails. run-workload runs with set -eE,
    # seed-checkpoint with set -e: both must record the step as failed.
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/", SEEDED_CKPT_PATH=SEED)

    _, calls = _run_step(step, env, STUB_HELM_VERSION="v0.0.0")

    assert _failures(env) == [step]
    assert not _calls(calls, "helm install")


def test_cleanup_helm_still_uninstalls_when_helm_install_fails(env):  # noqa: F811
    proc, calls = _run_step("cleanup-helm", env, STUB_HELM_VERSION="v0.0.0")

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "helm uninstall buildid-test" in calls
