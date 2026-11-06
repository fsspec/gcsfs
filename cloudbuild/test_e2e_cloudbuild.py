import json
import os
import re
import resource
import stat
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import yaml

CLOUDBUILD_DIR = Path(__file__).resolve().parent
REPO_ROOT = CLOUDBUILD_DIR.parent


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def test_e2e_yaml_substitutions_and_rcu_steps():
    yaml_path = CLOUDBUILD_DIR / "e2e-tests-cloudbuild.yaml"
    raw = yaml_path.read_text()
    cfg = yaml.safe_load(raw)

    subs = cfg["substitutions"]
    assert subs["_ZONE"] == "us-central1-a"
    assert subs["_RCU_CROSS_ZONE"] == "us-central1-b"
    assert subs["_WORKERS_RCU"] == "2"
    assert subs["_WORKERS_HNS_RCU"] == "2"

    steps = {step["id"]: step for step in cfg["steps"]}
    for step_id in ("create-buckets", "cleanup"):
        env = set(steps[step_id]["env"])
        assert "RCU_CROSS_ZONE=${_RCU_CROSS_ZONE}" in env
        assert "WORKERS_RCU=${_WORKERS_RCU}" in env
        assert "WORKERS_HNS_RCU=${_WORKERS_HNS_RCU}" in env

    for step_id, suite, workers_var in (
        ("run-rcu-tests", "rcu", "${_WORKERS_RCU}"),
        ("run-hns-rcu-tests", "hns-rcu", "${_WORKERS_HNS_RCU}"),
    ):
        assert step_id in steps
        step = steps[step_id]
        assert step["allowFailure"] is True
        assert set(step["waitFor"]) == {"setup-vm", "create-buckets"}
        body = " ".join(step["args"])
        assert f"TEST_SUITE='{suite}'" in body
        assert f"PYTEST_XDIST_WORKERS='{workers_var}'" in body

    fetch_wait = set(steps["fetch-test-results"]["waitFor"])
    assert {"run-rcu-tests", "run-hns-rcu-tests"} <= fetch_wait

    report_body = " ".join(steps["test-report"]["args"])
    assert "--suites standard zonal zonal-core hns rcu hns-rcu" in report_body
    assert "for suite in standard zonal zonal-core hns rcu hns-rcu; do" in report_body

    # cloud-sdk image ships no system python3; ensure neither cloud-sdk inline scripts
    # nor create_buckets.sh / cleanup.sh invoke python.
    for step in cfg["steps"]:
        if "cloud-sdk" in step["name"]:
            body = "\n".join(step.get("args", []))
            # Remote ssh commands run on the VM (which has python3), so only check non-ssh steps.
            if "gcloud compute ssh" not in body:
                assert not re.search(r"\bpython3?\b", body), step["id"]

    for script_name in ("create_buckets.sh", "cleanup.sh"):
        script_text = (CLOUDBUILD_DIR / script_name).read_text()
        assert not re.search(r"\bpython3?\b", script_text), script_name

    # Guard against unescaped single-$ bash variables in Cloud Build YAML.
    # Cloud Build built-ins (PROJECT_ID, BUILD_ID) and user/trigger substitutions (_*)
    # use single-$; step-local bash variables (attempt, suite, RANDOM, ...) must use $$.
    builtin_cb_vars = {"PROJECT_ID", "BUILD_ID"}
    for match in re.finditer(
        r"(?<!\$)\$(?!\$)(?:\{([^}:]+)(?::[^}]*)?\}|([A-Za-z_][A-Za-z0-9_]*))", raw
    ):
        var_name = match.group(1) or match.group(2)
        assert var_name in builtin_cb_vars or var_name.startswith(
            "_"
        ), f"Unescaped bash variable ${var_name} in YAML"


def test_create_buckets_provisions_rcu_and_retries_transient_error(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls_file = tmp_path / "gcloud_calls.jsonl"
    fail_once_marker = tmp_path / "failed_once"

    _write_executable(
        bin_dir / "gcloud",
        f"""#!/bin/bash
set -e
python3_bin="{os.sys.executable}"
"$python3_bin" - "$0" "$@" <<'PY'
import json, pathlib, sys
args = sys.argv[2:]
calls_path = pathlib.Path({str(calls_file)!r})
with calls_path.open("a") as f:
    f.write(json.dumps(args) + "\\n")

marker = pathlib.Path({str(fail_once_marker)!r})
if args[:3] == ["storage", "buckets", "create"] and "gs://gcsfs-test-rcu-deadbeef-gw0" in args:
    if not marker.exists():
        marker.write_text("1")
        sys.stderr.write("HTTPError 429: Create Bucket quota limit exceeded\\n")
        sys.exit(1)
PY
""",
    )

    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "PROJECT_ID": "test-proj",
        "REGION": "us-central1",
        "ZONE": "us-central1-c",
        "RCU_CROSS_ZONE": "us-central1-b",
        "SHORT_BUILD_ID": "deadbeef",
        "WORKERS_STANDARD": "1",
        "WORKERS_HNS": "1",
        "WORKERS_ZONAL": "1",
        "WORKERS_ZONAL_CORE": "1",
        "WORKERS_RCU": "2",
        "WORKERS_HNS_RCU": "2",
        "BUCKET_CREATE_RETRY_MAX_SLEEP": "0",
    }

    proc = subprocess.run(
        ["bash", str(CLOUDBUILD_DIR / "create_buckets.sh")],
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr

    calls = [json.loads(line) for line in calls_file.read_text().splitlines()]

    # gw0 failed once with 429 and succeeded on retry (2 calls total).
    rcu_gw0_creates = [
        c
        for c in calls
        if c[:4] == ["storage", "buckets", "create", "gs://gcsfs-test-rcu-deadbeef-gw0"]
    ]
    assert len(rcu_gw0_creates) == 2
    assert "--uniform-bucket-level-access" in rcu_gw0_creates[0]
    assert "--enable-hierarchical-namespace" not in rcu_gw0_creates[0]

    # HNS RCU buckets enable hierarchical namespace.
    for suffix in ("-gw0", "-gw1"):
        hns_rcu_creates = [
            c
            for c in calls
            if c[:4]
            == [
                "storage",
                "buckets",
                "create",
                f"gs://gcsfs-test-hns-rcu-deadbeef{suffix}",
            ]
        ]
        assert len(hns_rcu_creates) == 1
        assert "--enable-hierarchical-namespace" in hns_rcu_creates[0]
        assert "--uniform-bucket-level-access" in hns_rcu_creates[0]

    # Rapid caches created in ZONE for same-zone RCU buckets, and RCU_CROSS_ZONE for cross-zone bucket.
    # Command shape: `gcloud alpha storage buckets rapid-caches create gs://<bucket> <zone> ...`
    cache_calls = [
        c
        for c in calls
        if c[:5] == ["alpha", "storage", "buckets", "rapid-caches", "create"]
    ]
    created_targets = {(c[5], c[6]) for c in cache_calls}
    assert created_targets == {
        ("gs://gcsfs-test-rcu-deadbeef-gw0", "us-central1-c"),
        ("gs://gcsfs-test-rcu-deadbeef-gw1", "us-central1-c"),
        ("gs://gcsfs-test-hns-rcu-deadbeef-gw0", "us-central1-c"),
        ("gs://gcsfs-test-hns-rcu-deadbeef-gw1", "us-central1-c"),
        ("gs://gcsfs-test-rcu-cross-zone-deadbeef", "us-central1-b"),
    }
    for c in cache_calls:
        assert "--cache-type=rapid-cache-ultra" in c
        assert "--admission-policy=admit-on-first-miss" in c
        assert "--no-async" in c


def test_run_tests_rcu_and_hns_rcu_suites(tmp_path):
    venv_bin = tmp_path / "env" / "bin"
    venv_bin.mkdir(parents=True)
    dump_path = tmp_path / "pytest_invocation.json"

    (venv_bin / "activate").write_text(f'export PATH="{venv_bin}:$PATH"\n')
    _write_executable(
        venv_bin / "pytest",
        f"""#!{os.sys.executable}
import json, os, pathlib, sys
keys = [
    k for k in os.environ
    if k.startswith("GCSFS_") or k in ("STORAGE_EMULATOR_HOST", "GOOGLE_CLOUD_PROJECT")
]
payload = {{"argv": sys.argv[1:], "env": {{k: os.environ[k] for k in keys}}}}
pathlib.Path({str(dump_path)!r}).write_text(json.dumps(payload))
print("=== 10 passed, 1 skipped in 1.23s ===")
""",
    )

    base_env = {
        **os.environ,
        "HOME": str(tmp_path),
        "PROJECT_ID": "test-proj",
        "REGION": "us-central1",
        "KEY_RING": "ring",
        "KEY_NAME": "key",
        "SHORT_BUILD_ID": "deadbeef",
        "PYTEST_XDIST_WORKERS": "2",
    }

    # 1. Flat RCU suite
    proc_rcu = subprocess.run(
        ["bash", str(CLOUDBUILD_DIR / "run_tests.sh")],
        cwd=tmp_path,
        env={**base_env, "TEST_SUITE": "rcu"},
        capture_output=True,
        text=True,
    )
    assert proc_rcu.returncode == 0, proc_rcu.stderr
    rcu_inv = json.loads(dump_path.read_text())
    assert rcu_inv["env"]["GCSFS_TEST_BUCKET"] == "gcsfs-test-rcu-deadbeef"
    assert rcu_inv["env"]["GCSFS_ZONAL_TEST_BUCKET"] == "gcsfs-test-rcu-deadbeef"
    assert (
        rcu_inv["env"]["GCSFS_CROSS_ZONE_RCU_TEST_BUCKET"]
        == "gcsfs-test-rcu-cross-zone-deadbeef"
    )
    assert rcu_inv["env"]["GCSFS_RUN_RAPID_TESTS"] == "true"
    assert rcu_inv["env"]["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] == "true"
    assert rcu_inv["env"].get("GCSFS_RUN_HNS_TESTS") != "true"
    for target in (
        "gcsfs/tests/test_rcu.py",
        "gcsfs/tests/test_zonal.py",
        "gcsfs/tests/test_zonal_file.py",
        "gcsfs/tests/test_core.py",
    ):
        assert target in rcu_inv["argv"]
    # Flat RCU keeps standard flat wildcard non-recursive rm behavior.
    assert (
        "--deselect=gcsfs/tests/test_core.py::test_rm_wildcards_non_recursive"
        not in rcu_inv["argv"]
    )

    # 2. HNS RCU suite
    dump_path.unlink()
    proc_hns_rcu = subprocess.run(
        ["bash", str(CLOUDBUILD_DIR / "run_tests.sh")],
        cwd=tmp_path,
        env={**base_env, "TEST_SUITE": "hns-rcu"},
        capture_output=True,
        text=True,
    )
    assert proc_hns_rcu.returncode == 0, proc_hns_rcu.stderr
    hns_inv = json.loads(dump_path.read_text())
    assert hns_inv["env"]["GCSFS_ZONAL_TEST_BUCKET"] == "gcsfs-test-hns-rcu-deadbeef"
    assert hns_inv["env"]["GCSFS_HNS_TEST_BUCKET"] == "gcsfs-test-hns-rcu-deadbeef"
    assert hns_inv["env"]["GCSFS_RUN_HNS_TESTS"] == "true"
    assert hns_inv["env"]["GCSFS_RUN_RAPID_TESTS"] == "true"
    assert not hns_inv["env"].get("GCSFS_CROSS_ZONE_RCU_TEST_BUCKET")
    for target in (
        "gcsfs/tests/test_rcu.py",
        "gcsfs/tests/test_zonal.py",
        "gcsfs/tests/test_zonal_file.py",
        "gcsfs/tests/test_async.py",
        "gcsfs/tests/test_hns.py",
    ):
        assert target in hns_inv["argv"]


def test_run_tests_grpc_suites_raise_soft_fd_limit_without_lowering(tmp_path):
    venv_bin = tmp_path / "env" / "bin"
    venv_bin.mkdir(parents=True)
    dump_path = tmp_path / "rlimit.json"

    (venv_bin / "activate").write_text(f'export PATH="{venv_bin}:$PATH"\n')
    _write_executable(
        venv_bin / "pytest",
        f"""#!{os.sys.executable}
import json, pathlib, resource
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
pathlib.Path({str(dump_path)!r}).write_text(json.dumps({{"soft": soft, "hard": hard}}))
print("=== 1 passed in 0.10s ===")
""",
    )

    base_env = {
        **os.environ,
        "HOME": str(tmp_path),
        "PROJECT_ID": "test-proj",
        "REGION": "us-central1",
        "KEY_RING": "ring",
        "KEY_NAME": "key",
        "SHORT_BUILD_ID": "deadbeef",
        "PYTEST_XDIST_WORKERS": "2",
    }

    _, current_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    expected_raised = (
        65536 if current_hard == resource.RLIM_INFINITY else min(65536, current_hard)
    )

    for suite in ("zonal", "zonal-core", "rcu", "hns-rcu"):
        if dump_path.exists():
            dump_path.unlink()
        proc = subprocess.run(
            ["bash", str(CLOUDBUILD_DIR / "run_tests.sh")],
            cwd=tmp_path,
            env={**base_env, "TEST_SUITE": suite},
            preexec_fn=lambda: resource.setrlimit(
                resource.RLIMIT_NOFILE, (min(1024, current_hard), current_hard)
            ),
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0, f"{suite}: {proc.stderr}"
        observed = json.loads(dump_path.read_text())
        assert observed["soft"] == expected_raised, suite
        assert observed["hard"] == current_hard, suite

    if current_hard == resource.RLIM_INFINITY or current_hard > 65536:
        high_soft = (
            131072
            if current_hard == resource.RLIM_INFINITY
            else min(131072, current_hard)
        )
        dump_path.unlink()
        proc = subprocess.run(
            ["bash", str(CLOUDBUILD_DIR / "run_tests.sh")],
            cwd=tmp_path,
            env={**base_env, "TEST_SUITE": "rcu"},
            preexec_fn=lambda: resource.setrlimit(
                resource.RLIMIT_NOFILE, (high_soft, current_hard)
            ),
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0, proc.stderr
        observed = json.loads(dump_path.read_text())
        assert observed["soft"] == high_soft


def test_cleanup_disables_rcu_caches_and_reaps_only_stale_leaked_rcu_buckets(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls_file = tmp_path / "cleanup_calls.jsonl"
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    _write_executable(
        bin_dir / "gcloud",
        f"""#!/bin/bash
set -e
"{os.sys.executable}" - "$@" <<'PY'
import json, pathlib, sys
args = sys.argv[1:]
with pathlib.Path({str(calls_file)!r}).open("a") as f:
    f.write(json.dumps(args) + "\\n")

if args[:3] == ["storage", "buckets", "list"]:
    # Stale leaked RCU bucket (2025) + fresh concurrent build RCU bucket (now).
    print("gs://gcsfs-test-rcu-11111111-gw0\\t2025-01-01T00:00:00Z")
    print("gs://gcsfs-test-hns-rcu-22222222-gw0\\t{now_iso}")
    sys.exit(0)

if args[:5] == ["alpha", "storage", "buckets", "rapid-caches", "list"]:
    if "gs://gcsfs-test-rcu-11111111-gw0" in args:
        print("gcsfs-test-rcu-11111111-gw0/us-central1-c\\trunning")
    sys.exit(0)

# Simulate buckets delete failing on an active/disabling RCU cache for current build:
# cleanup.sh must still succeed because rapid-caches disable runs asynchronously.
if args[:3] == ["storage", "buckets", "delete"] and "gs://gcsfs-test-rcu-deadbeef-gw0" in args:
    sys.exit(1)
PY
""",
    )

    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "PROJECT_ID": "test-proj",
        "ZONE": "us-central1-c",
        "RCU_CROSS_ZONE": "us-central1-b",
        "SHORT_BUILD_ID": "deadbeef",
        "WORKERS_STANDARD": "1",
        "WORKERS_HNS": "1",
        "WORKERS_ZONAL": "1",
        "WORKERS_ZONAL_CORE": "1",
        "WORKERS_RCU": "2",
        "WORKERS_HNS_RCU": "2",
    }

    proc = subprocess.run(
        ["bash", str(CLOUDBUILD_DIR / "cleanup.sh")],
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr

    calls = [json.loads(line) for line in calls_file.read_text().splitlines()]
    disabled_ids = {
        c[5]
        for c in calls
        if c[:5] == ["alpha", "storage", "buckets", "rapid-caches", "disable"]
    }
    assert {
        "gcsfs-test-rcu-deadbeef-gw0/us-central1-c",
        "gcsfs-test-rcu-deadbeef-gw1/us-central1-c",
        "gcsfs-test-hns-rcu-deadbeef-gw0/us-central1-c",
        "gcsfs-test-hns-rcu-deadbeef-gw1/us-central1-c",
        "gcsfs-test-rcu-cross-zone-deadbeef/us-central1-b",
        # Stale leaked RCU bucket cache also disabled:
        "gcsfs-test-rcu-11111111-gw0/us-central1-c",
    } <= disabled_ids

    # Fresh concurrent build RCU bucket (22222222) must NOT be disabled or deleted.
    for c in calls:
        assert not any("22222222" in arg for arg in c), c

    # Standalone invocation with empty SHORT_BUILD_ID must still reap stale leaked RCU buckets.
    calls_file.unlink()
    proc_empty_id = subprocess.run(
        ["bash", str(CLOUDBUILD_DIR / "cleanup.sh")],
        env={**env, "SHORT_BUILD_ID": ""},
        capture_output=True,
        text=True,
    )
    assert proc_empty_id.returncode == 0, proc_empty_id.stderr
    empty_id_calls = [json.loads(line) for line in calls_file.read_text().splitlines()]
    empty_id_disabled = {
        c[5]
        for c in empty_id_calls
        if c[:5] == ["alpha", "storage", "buckets", "rapid-caches", "disable"]
    }
    assert "gcsfs-test-rcu-11111111-gw0/us-central1-c" in empty_id_disabled
