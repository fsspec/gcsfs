"""Tests for the signature-verified Helm install (lib.sh install_helm).

The real ``install_helm`` runs under bash with a stub ``curl`` that serves the
latest-version file, a fixture tarball and its detached signature. Signatures
are made with throwaway GPG keys, so ``gpg``/``tar`` and the PATH handling are
exercised for real; no network is used.
"""

import io
import os
import re
import shutil
import subprocess
import tarfile

import pytest

from .test_rcu_scripts import (  # noqa: F401  (env is a fixture)
    REPO,
    SEED,
    _add_build_vars,
    _calls,
    _failures,
    _run_step,
    env,
)

LIB = "cloudbuild/macrobenchmarks/scripts/lib.sh"
KEYS = REPO / "cloudbuild/macrobenchmarks/scripts/helm-release-keys.asc"
LATEST = "v4.9.9"
LATEST_URL = "https://get.helm.sh/helm4-latest-version"
TARBALL_URL = f"https://get.helm.sh/helm-{LATEST}-linux-amd64.tar.gz"
SIG_URL = (
    f"https://github.com/helm/helm/releases/download/{LATEST}/"
    f"helm-{LATEST}-linux-amd64.tar.gz.asc"
)
# The key that signed the Helm v4.3.2 release (Scott Rigby).
CURRENT_HELM_SIGNER = "208DD36ED5BB3745A16743A4C7C6FBB5B91C1155"

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None or shutil.which("gpg") is None,
    reason="needs bash and gpg",
)

CURL_STUB = r"""#!/usr/bin/env bash
echo "curl $*" >> "$STUB_LOG"
[[ -n "$STUB_CURL_FAIL" && "$*" =~ $STUB_CURL_FAIL ]] && exit 22
url="${@: -1}"
out=""
while [[ $# -gt 0 ]]; do
  [[ "$1" == "-o" ]] && out="$2"
  shift
done
case "$url" in
  *-latest-version) printf '%s' "$STUB_LATEST" ;;
  *.asc) cp "$STUB_SIGNATURE" "$out" ;;
  *.tar.gz) cp "$STUB_TARBALL" "$out" ;;
esac
exit 0
"""


def _write_exe(path, body):
    path.write_text(body)
    path.chmod(0o755)


def _gpg(home, *args, **kwargs):
    return subprocess.run(
        ["gpg", "--homedir", str(home), "--batch", "--quiet", *args],
        check=True,
        capture_output=True,
        **kwargs,
    )


@pytest.fixture(scope="module")
def keys(tmp_path_factory):
    """A trusted key (exported like Helm's KEYS file) and a stranger key."""
    home = tmp_path_factory.mktemp("gpg")
    home.chmod(0o700)
    fingerprints = {}
    for name in ("trusted", "stranger"):
        _gpg(
            home,
            "--pinentry-mode",
            "loopback",
            "--passphrase",
            "",
            "--quick-gen-key",
            f"{name} <{name}@example.com>",
            "ed25519",
            "sign",
            "never",
        )
        listing = _gpg(home, "--with-colons", "--list-keys", f"{name}@example.com")
        fingerprints[name] = re.search(
            r"^fpr:+([0-9A-F]+):", listing.stdout.decode(), re.M
        ).group(1)
    exported = _gpg(home, "--armor", "--export", fingerprints["trusted"]).stdout
    keyring = home / "trusted-keys.asc"
    keyring.write_bytes(exported)
    yield {"home": home, "keyring": keyring, **fingerprints}
    subprocess.run(
        ["gpgconf", "--homedir", str(home), "--kill", "all"], capture_output=True
    )


def _release(tmp_path, keys, signer="trusted", tamper=False):
    """A helm tarball whose binary prints ``installed-helm``, plus its .asc."""
    body = b'#!/usr/bin/env bash\necho installed-helm "$@"\n'
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo("linux-amd64/helm")
        info.size = len(body)
        info.mode = 0o755
        tar.addfile(info, io.BytesIO(body))
    tarball = tmp_path / "helm.tar.gz"
    tarball.write_bytes(buf.getvalue())
    signature = tmp_path / "helm.tar.gz.asc"
    _gpg(
        keys["home"],
        "--pinentry-mode",
        "loopback",
        "--passphrase",
        "",
        "--local-user",
        keys[signer],
        "--armor",
        "--detach-sign",
        "--output",
        str(signature),
        str(tarball),
    )
    if tamper:
        tarball.write_bytes(buf.getvalue() + b"tampered")
    return tarball, signature


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


def _run(script, bin_dir, tmp_path, keys, release=None, **env):
    (tmp_path / "tmpdir").mkdir(exist_ok=True)
    tarball, signature = release or ("/nonexistent", "/nonexistent")
    run_env = {
        "PATH": f"{bin_dir}:{os.path.dirname(shutil.which('gpg'))}:/usr/bin:/bin",
        "TMPDIR": str(tmp_path / "tmpdir"),
        "WORKSPACE_DIR": str(tmp_path),
        "STUB_LOG": str(tmp_path / "calls.log"),
        "STUB_LATEST": LATEST,
        "STUB_TARBALL": str(tarball),
        "STUB_SIGNATURE": str(signature),
        **env,
    }
    proc = subprocess.run(
        [
            "bash",
            "-c",
            f"source {LIB}\nHELM_RELEASE_KEYS={keys['keyring']}\n{script}",
        ],
        cwd=REPO,
        env=run_env,
        capture_output=True,
        text=True,
    )
    log = tmp_path / "calls.log"
    return proc, log.read_text() if log.exists() else ""


def _tmpdir_files(tmp_path):
    return sorted(p for p in (tmp_path / "tmpdir").rglob("*") if p.is_file())


def test_install_helm_installs_the_latest_signed_release(bin_dir, tmp_path, keys):
    _helm_stub(bin_dir, "v0.0.0")  # some other helm already on PATH

    proc, curl_log = _run(
        "install_helm && helm", bin_dir, tmp_path, keys, _release(tmp_path, keys)
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert f"Installed helm {LATEST} (signature verified)" in proc.stdout
    assert "installed-helm" in proc.stdout
    for url in (LATEST_URL, TARBALL_URL, SIG_URL):
        assert url in curl_log


def test_install_helm_downloads_into_tmpdir(bin_dir, tmp_path, keys):
    # Honouring TMPDIR keeps test runs out of the host's /tmp.
    proc, _ = _run(
        "install_helm >/dev/null && command -v helm",
        bin_dir,
        tmp_path,
        keys,
        _release(tmp_path, keys),
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert proc.stdout.strip().startswith(str(tmp_path / "tmpdir"))
    # Only the helm binary is kept: tarball, signature and keyring are deleted.
    assert [p.name for p in _tmpdir_files(tmp_path)] == ["helm"]


def test_install_helm_rejects_a_signature_from_an_unknown_key(bin_dir, tmp_path, keys):
    proc, curl_log = _run(
        "install_helm; echo rc=$?; helm",
        bin_dir,
        tmp_path,
        keys,
        _release(tmp_path, keys, signer="stranger"),
    )

    assert SIG_URL in curl_log
    assert re.search(r"rc=[1-9]", proc.stdout), proc.stdout + proc.stderr
    assert "signature" in proc.stderr.lower()
    assert "installed-helm" not in proc.stdout
    assert list((tmp_path / "tmpdir").iterdir()) == []


def test_install_helm_rejects_a_tampered_tarball(bin_dir, tmp_path, keys):
    proc, _ = _run(
        "install_helm; echo rc=$?; helm",
        bin_dir,
        tmp_path,
        keys,
        _release(tmp_path, keys, tamper=True),
    )

    assert re.search(r"rc=[1-9]", proc.stdout), proc.stdout + proc.stderr
    assert "signature" in proc.stderr.lower()
    assert "installed-helm" not in proc.stdout
    assert list((tmp_path / "tmpdir").iterdir()) == []


@pytest.mark.parametrize("latest", ["v5.0.0", "<html>Not Found</html>", ""])
def test_install_helm_rejects_an_unexpected_latest_version(
    bin_dir, tmp_path, keys, latest
):
    proc, curl_log = _run(
        "install_helm; echo rc=$?",
        bin_dir,
        tmp_path,
        keys,
        _release(tmp_path, keys),
        STUB_LATEST=latest,
    )

    assert re.search(r"rc=[1-9]", proc.stdout), proc.stdout + proc.stderr
    assert ".tar.gz" not in curl_log  # nothing downloaded
    assert list((tmp_path / "tmpdir").iterdir()) == []


@pytest.mark.parametrize(
    ("failing", "url"),
    [
        ("latest-version", LATEST_URL),
        (r"\.tar\.gz$", TARBALL_URL),
        (r"\.asc$", SIG_URL),
    ],
)
def test_install_helm_fails_when_a_download_fails(
    bin_dir, tmp_path, keys, failing, url
):
    proc, curl_log = _run(
        "install_helm; echo rc=$?; helm",
        bin_dir,
        tmp_path,
        keys,
        _release(tmp_path, keys),
        STUB_CURL_FAIL=failing,
    )

    assert url in curl_log
    assert re.search(r"rc=[1-9]", proc.stdout), proc.stdout + proc.stderr
    assert "installed-helm" not in proc.stdout
    assert list((tmp_path / "tmpdir").iterdir()) == []


def test_install_helm_skips_the_download_when_the_latest_version_is_on_path(
    bin_dir, tmp_path, keys
):
    _helm_stub(bin_dir, LATEST)

    proc, curl_log = _run("install_helm", bin_dir, tmp_path, keys)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert curl_log.strip().splitlines() == [
        line for line in curl_log.strip().splitlines() if LATEST_URL in line
    ]


def test_install_helm_does_not_trip_the_callers_err_trap_when_helm_is_absent(
    bin_dir, tmp_path, keys
):
    # Step scripts run with set -eE and an ERR trap that records the step as
    # failed; a successful first-time install (no helm on PATH yet) must not
    # fire it.
    proc, _ = _run(
        "set -eE\ntrap 'echo ERR-TRAPPED' ERR\ninstall_helm\nhelm",
        bin_dir,
        tmp_path,
        keys,
        _release(tmp_path, keys),
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "ERR-TRAPPED" not in proc.stdout
    assert "installed-helm" in proc.stdout


def test_release_keys_include_the_current_helm_signer(tmp_path):
    home = tmp_path / "gnupg"
    home.mkdir(mode=0o700)
    listing = _gpg(home, "--with-colons", "--show-keys", str(KEYS)).stdout.decode()

    assert CURRENT_HELM_SIGNER in re.findall(r"^fpr:+([0-9A-F]+):", listing, re.M)


# --- The helm steps (run like Cloud Build via the test_rcu_scripts harness) ---

PIPE_TO_SHELL = re.compile(r"\b(curl|wget)\b[^|\n]*\|\s*(sudo\s+)?(ba|z|da)?sh\b")


def test_no_cloudbuild_script_pipes_a_download_into_a_shell():
    offenders = [
        f"{path.relative_to(REPO)}:{n}"
        for path in sorted((REPO / "cloudbuild").rglob("*"))
        if path.is_file() and path.suffix in (".sh", ".yaml", ".yml")
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if PIPE_TO_SHELL.search(line)
    ]
    assert offenders == []


@pytest.mark.parametrize("step", ["run-workload", "seed-checkpoint", "cleanup-helm"])
def test_helm_steps_install_the_latest_helm(env, step):  # noqa: F811
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/", SEEDED_CKPT_PATH=SEED)

    proc, calls = _run_step(step, env, STUB_LS=SEED)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _failures(env) == []
    assert any(LATEST_URL in c for c in calls)
    helm_calls = _calls(calls, "helm ")
    assert helm_calls and helm_calls[0].startswith("helm version")
    assert not [c for c in calls if "raw.githubusercontent.com" in c]


@pytest.mark.parametrize("step", ["run-workload", "seed-checkpoint"])
def test_helm_steps_record_failure_when_helm_install_fails(env, step):  # noqa: F811
    # Another helm version on PATH forces a download; the stub curl serves
    # nothing, so signature verification fails. run-workload runs with set -eE,
    # seed-checkpoint with set -e: both must record the step as failed.
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/", SEEDED_CKPT_PATH=SEED)

    _, calls = _run_step(step, env, STUB_HELM_VERSION="v0.0.0")

    assert _failures(env) == [step]
    assert not _calls(calls, "helm install")


def test_cleanup_helm_still_uninstalls_when_helm_install_fails(env):  # noqa: F811
    proc, calls = _run_step("cleanup-helm", env, STUB_HELM_VERSION="v0.0.0")

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "helm uninstall buildid-test" in calls
