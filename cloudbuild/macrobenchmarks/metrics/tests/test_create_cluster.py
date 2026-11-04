"""Tests for the control-plane zone fallback in create_cluster.sh and the
matching lookup in cleanup_cluster.sh.

The scripts run for real under bash, with stub ``gcloud``/``kubectl``/``sleep``
executables on PATH that log every call and fail on demand.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")

GCLOUD_STUB = r"""#!/usr/bin/env bash
echo "gcloud $*" >> "$STUB_LOG"
case "$*" in
  "compute zones list"*)
    [ -n "$STUB_ZONES_LIST_FAIL" ] && exit 1
    printf '%s\n' $STUB_ZONES ;;
  "container clusters create"*)
    for z in $STUB_FAIL_ZONES; do
      [[ "$*" == *"--zone=$z "* ]] && exit 1
    done ;;
  "container clusters list"*)
    [ -n "$STUB_LIST_FAIL" ] && exit 1
    [ -n "$STUB_CLUSTER_LOCATIONS" ] && printf '%s\n' $STUB_CLUSTER_LOCATIONS ;;
esac
exit 0
"""


@pytest.fixture
def env(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in {
        "gcloud": GCLOUD_STUB,
        "kubectl": '#!/usr/bin/env bash\necho "kubectl $*" >> "$STUB_LOG"\n',
        "sleep": "#!/usr/bin/env bash\n",
    }.items():
        stub = bin_dir / name
        stub.write_text(body)
        stub.chmod(0o755)

    build_vars = tmp_path / "build_vars.env"
    build_vars.write_text(
        "export CLUSTER_NAME=test-cluster\n"
        "export NETWORK_NAME=test-net\n"
        "export SUBNET_NAME=test-subnet\n"
        "export REGION=region1\n"
    )
    return {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "STUB_LOG": str(tmp_path / "calls.log"),
        "FAILED_FILE": str(tmp_path / "FAILED"),
        "BUILD_VARS_FILE": str(build_vars),
        "PROJECT_ID": "test-project",
        "_ZONE": "region1-a",
        "_MACHINE_TYPE": "test-machine",
        "_NODES": "1",
        "_GKE_SERVICE_ACCOUNT": "sa@example.com",
        "_JOBSET_VERSION": "v0.0.0",
        "STUB_ZONES": "region1-a region1-b region1-c",
    }


def _run(script, env, **overrides):
    env = {**env, **overrides}
    proc = subprocess.run(
        ["bash", str(SCRIPTS / script)], env=env, capture_output=True, text=True
    )
    log = Path(env["STUB_LOG"])
    calls = log.read_text().splitlines() if log.exists() else []
    return proc, calls


def _calls(calls, prefix):
    return [c for c in calls if c.startswith(prefix)]


def _failures(env):
    failed = Path(env["FAILED_FILE"])
    return failed.read_text().split() if failed.exists() else []


def _cluster_location(env):
    for line in Path(env["BUILD_VARS_FILE"]).read_text().splitlines():
        if line.startswith("export CLUSTER_LOCATION="):
            return line.split("=", 1)[1]
    return None


def test_create_succeeds_in_preferred_zone(env):
    proc, calls = _run("create_cluster.sh", env)

    assert proc.returncode == 0, proc.stderr
    creates = _calls(calls, "gcloud container clusters create")
    assert len(creates) == 1 and "--zone=region1-a " in creates[0]
    assert not _calls(calls, "gcloud container clusters delete")
    assert _cluster_location(env) == "region1-a"
    assert _failures(env) == []


def test_create_falls_back_and_keeps_node_pool_in_preferred_zone(env):
    proc, calls = _run("create_cluster.sh", env, STUB_FAIL_ZONES="region1-a")

    assert proc.returncode == 0, proc.stderr
    creates = _calls(calls, "gcloud container clusters create")
    assert ["--zone=region1-a " in c for c in creates] == [True, False]
    assert "--zone=region1-b " in creates[1]
    deletes = _calls(calls, "gcloud container clusters delete")
    assert len(deletes) == 1 and "--zone=region1-a" in deletes[0]
    (pool,) = _calls(calls, "gcloud container node-pools create")
    assert "--zone=region1-b" in pool and "--node-locations=region1-a" in pool
    (creds,) = _calls(calls, "gcloud container clusters get-credentials")
    assert "--zone=region1-b" in creds
    assert _cluster_location(env) == "region1-b"
    assert _failures(env) == []


def test_create_records_failure_once_when_all_zones_fail(env):
    proc, calls = _run(
        "create_cluster.sh", env, STUB_FAIL_ZONES="region1-a region1-b region1-c"
    )

    assert proc.returncode == 1
    assert len(_calls(calls, "gcloud container clusters create")) == 3
    assert not _calls(calls, "gcloud container node-pools create")
    assert _failures(env) == ["create-cluster"]
    assert _cluster_location(env) is None
    assert "could not create test-cluster" in proc.stderr


def test_create_tries_only_preferred_zone_when_zone_list_fails(env):
    proc, calls = _run(
        "create_cluster.sh", env, STUB_ZONES_LIST_FAIL="1", STUB_FAIL_ZONES="region1-a"
    )

    assert proc.returncode == 1
    creates = _calls(calls, "gcloud container clusters create")
    assert len(creates) == 1 and "--zone=region1-a " in creates[0]
    assert _failures(env) == ["create-cluster"]


def test_cleanup_deletes_cluster_in_every_location_found(env):
    proc, calls = _run(
        "cleanup_cluster.sh", env, STUB_CLUSTER_LOCATIONS="region1-a region1-b"
    )

    assert proc.returncode == 0, proc.stderr
    deletes = _calls(calls, "gcloud container clusters delete")
    assert len(deletes) == 2
    assert "--zone=region1-a" in deletes[0] and "--zone=region1-b" in deletes[1]


def test_cleanup_skips_delete_when_no_cluster_found(env):
    proc, calls = _run("cleanup_cluster.sh", env)

    assert proc.returncode == 0, proc.stderr
    assert not _calls(calls, "gcloud container clusters delete")
    assert "No cluster test-cluster found" in proc.stdout


def test_cleanup_falls_back_to_recorded_location_when_lookup_fails(env):
    proc, calls = _run(
        "cleanup_cluster.sh", env, STUB_LIST_FAIL="1", CLUSTER_LOCATION="region1-c"
    )

    assert proc.returncode == 0, proc.stderr
    (delete,) = _calls(calls, "gcloud container clusters delete")
    assert "--zone=region1-c" in delete
