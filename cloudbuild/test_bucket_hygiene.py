"""Bucket hygiene checks for the Cloud Build scripts.

Security scanners flag deleted buckets whose *own* Public Access Prevention is not
"enforced" (an org-policy-inherited PAP still reads as "inherited") once it was
more than 14 days old. So every bucket we create must enforce PAP itself, and
cleanup must actually delete (or loudly report) every bucket it is given.
"""

import os
import pathlib
import re
import subprocess
import textwrap

import pytest

CLOUDBUILD = pathlib.Path(__file__).resolve().parent
_CREATE = re.compile(r"gcloud storage buckets create\b")


def _logical_lines(text):
    """Yield shell lines with backslash-continuations joined."""
    buf = ""
    for line in text.splitlines():
        if line.rstrip().endswith("\\"):
            buf += line.rstrip()[:-1] + " "
            continue
        yield buf + line
        buf = ""
    if buf:
        yield buf


def _bucket_create_commands():
    for path in sorted(CLOUDBUILD.rglob("*")):
        if path.suffix not in (".sh", ".yaml", ".yml") or not path.is_file():
            continue
        for line in _logical_lines(path.read_text()):
            if _CREATE.search(line):
                yield path.relative_to(CLOUDBUILD), line.strip()


def test_finds_bucket_create_commands():
    # Guard against the scan silently matching nothing.
    assert len(list(_bucket_create_commands())) >= 10


@pytest.mark.parametrize(
    "path,command",
    list(_bucket_create_commands()),
    ids=lambda v: str(v)[:60],
)
def test_every_bucket_create_enforces_pap(path, command):
    assert re.search(
        r"--(public-access-prevention|pap)\b", command
    ), f"{path}: bucket created without --public-access-prevention: {command}"


# --- cleanup.sh ---------------------------------------------------------------

_FAKE_GCLOUD = textwrap.dedent(
    """\
    #!/bin/bash
    # Fake gcloud: records calls; behaviour per bucket is driven by name.
    echo "$*" >> "${GCLOUD_CALLS}"
    url=""
    for a in "$@"; do [[ "$a" == gs://* ]] && url="$a"; done
    case "$*" in
      "compute "*) exit 1 ;;  # no VM to delete
      "storage rm "*|"storage buckets describe "*)
        case "$url" in
          *missing*) echo "ERROR: (gcloud.storage.rm) $url not found: 404." >&2; exit 1 ;;
          *denied*) echo "ERROR: (gcloud) 403 serviceusage.services.use denied" >&2; exit 1 ;;
        esac
        exit 0 ;;
    esac
    exit 0
    """
)


def _run_cleanup(tmp_path, short_build_id):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    gcloud = bin_dir / "gcloud"
    gcloud.write_text(_FAKE_GCLOUD)
    gcloud.chmod(0o755)
    calls = tmp_path / "calls.txt"
    calls.touch()
    env = dict(
        os.environ,
        PATH=f"{bin_dir}:{os.environ['PATH']}",
        GCLOUD_CALLS=str(calls),
        SHORT_BUILD_ID=short_build_id,
        PROJECT_ID="proj",
        ZONE="us-central1-a",
        WORKERS_STANDARD="2",
        WORKERS_HNS="2",
        FAILED_FILE=str(tmp_path / "FAILED"),
    )
    proc = subprocess.run(
        ["bash", str(CLOUDBUILD / "cleanup.sh")],
        env=env,
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    return proc, calls.read_text().splitlines()


def _rm_targets(calls):
    return {c.split()[3] for c in calls if c.startswith("storage rm --recursive")}


def test_cleanup_deletes_requester_pays_buckets_with_billing_project(tmp_path):
    proc, calls = _run_cleanup(tmp_path, "abcd1234")
    assert proc.returncode == 0, proc.stderr
    targets = _rm_targets(calls)
    for name in (
        "gs://gcsfs-test-standard-req-pay-abcd1234-gw0",
        "gs://gcsfs-test-standard-req-pay-abcd1234-gw1",
        "gs://gcsfs-test-hns-req-pay-abcd1234-gw0",
        "gs://gcsfs-test-hns-req-pay-abcd1234-gw1",
    ):
        assert name in targets
        (call,) = [c for c in calls if f"rm --recursive {name} " in c + " "]
        assert "--billing-project=proj" in call
    assert "ALERT" not in proc.stdout + proc.stderr


def test_cleanup_skips_buckets_that_were_never_created(tmp_path):
    # "missing" buckets 404: nothing to clean, so no alert.
    proc, _ = _run_cleanup(tmp_path, "missing0")
    assert proc.returncode == 0
    assert "ALERT" not in proc.stdout + proc.stderr


def test_cleanup_reports_buckets_it_fails_to_delete(tmp_path):
    # A non-404 failure (e.g. missing serviceusage.services.use on the billing
    # project) must not be swallowed: that is how requester-pays buckets leaked.
    proc, _ = _run_cleanup(tmp_path, "denied00")
    out = proc.stdout + proc.stderr
    assert "ALERT" in out
    assert "gs://gcsfs-test-standard-req-pay-denied00-gw0" in out
    assert "serviceusage.services.use" in out
    # ...and fails the build via the check-failure step.
    assert "cleanup" in (tmp_path / "FAILED").read_text()
