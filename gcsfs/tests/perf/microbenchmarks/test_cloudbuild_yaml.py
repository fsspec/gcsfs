import json
import os
import pathlib
import re
import stat
import subprocess

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
CLOUDBUILD_YAML_PATH = (
    REPO_ROOT / "cloudbuild" / "benchmarks" / "benchmarks-cloudbuild.yaml"
)

BUILTIN_SUBSTITUTIONS = {
    "BUILD_ID": "abcdef12-3456-7890-abcd-ef1234567890",
    "BRANCH_NAME": "feat-rcu-microbenchmarks",
    "PROJECT_ID": "test-project-101",
    "LOCATION": "us-central1",
}


def _load_cloudbuild():
    with open(CLOUDBUILD_YAML_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _render_cloudbuild_str(raw: str, substitutions: dict[str, str]) -> str:
    def _replace(match: re.Match) -> str:
        if match.group(0) == "$$":
            return "$"
        key = match.group(1)
        if key not in substitutions:
            raise KeyError(f"Unescaped or unknown Cloud Build substitution: ${{{key}}}")
        return substitutions[key]

    return re.sub(r"\$\$|\$\{([^}]+)\}", _replace, raw)


def _get_step(doc: dict, step_id: str) -> dict:
    for step in doc["steps"]:
        if step.get("id") == step_id:
            return step
    raise KeyError(f"Step {step_id!r} not found")


def _run_step(
    doc: dict,
    step_id: str,
    workspace_dir: pathlib.Path,
    bin_dir: pathlib.Path,
    overrides: dict[str, str] | None = None,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    subs = dict(doc.get("substitutions", {}))
    subs.update(BUILTIN_SUBSTITUTIONS)
    if overrides:
        subs.update(overrides)
    # Re-render substitutions that reference /workspace (e.g., _SKIP_IF_FAILED)
    subs = {k: v.replace("/workspace", str(workspace_dir)) for k, v in subs.items()}

    step = _get_step(doc, step_id)
    script = _render_cloudbuild_str(step["args"][1], subs).replace(
        "/workspace", str(workspace_dir)
    )

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
    for env_entry in step.get("env", []):
        rendered_entry = _render_cloudbuild_str(env_entry, subs)
        k, _, v = rendered_entry.partition("=")
        env[k] = v
    if extra_env:
        env.update(extra_env)

    return subprocess.run(
        ["bash", "-c", script],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )


def _write_executable(path: pathlib.Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def test_cloudbuild_substitutions_and_dollar_escaping():
    doc = _load_cloudbuild()
    subs = dict(doc.get("substitutions", {}))
    subs.update(BUILTIN_SUBSTITUTIONS)

    for step in doc["steps"]:
        for arg in step.get("args", []):
            _render_cloudbuild_str(arg, subs)
        for env_entry in step.get("env", []):
            _render_cloudbuild_str(env_entry, subs)

    pool_name = doc["options"]["pool"]["name"]
    _render_cloudbuild_str(pool_name, subs)


def test_init_variables_exports_standard_and_hns_rcu_buckets(tmp_path):
    doc = _load_cloudbuild()
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    res = _run_step(
        doc,
        "init-variables",
        workspace_dir,
        bin_dir,
        overrides={
            "_INFRA_PREFIX": "test-pfx",
            "_ZONE": "us-central1-c",
            "_BENCHMARK_TYPE": "IO",
        },
    )
    assert res.returncode == 0, res.stderr

    dump = subprocess.run(
        [
            "bash",
            "-c",
            f"source {workspace_dir}/build_vars.env && "
            "python3 -c 'import json, os; print(json.dumps(dict(os.environ)))'",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    exported = json.loads(dump.stdout)
    assert exported["STANDARD_RCU_BUCKET"] == "test-pfx-standard-rcu-abcdef12"
    assert exported["HNS_RCU_BUCKET"] == "test-pfx-hns-rcu-abcdef12"


def test_create_buckets_provisions_flat_and_hns_rcu_buckets_and_caches(tmp_path):
    doc = _load_cloudbuild()
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls_file = tmp_path / "gcloud_calls.jsonl"
    calls_file.write_text("", encoding="utf-8")
    created_buckets_dir = tmp_path / "created_buckets"
    created_buckets_dir.mkdir()

    _write_executable(
        bin_dir / "gcloud",
        f"""#!/usr/bin/env python3
import fcntl
import json
import pathlib
import sys

args = sys.argv[1:]
created_dir = pathlib.Path({str(created_buckets_dir)!r})

if args[:3] == ["storage", "buckets", "describe"]:
    # Simulate persistent results bucket already existing.
    if "run-results" in " ".join(args):
        sys.exit(0)
    sys.exit(1)

if args[:3] == ["storage", "buckets", "create"]:
    url = args[3]
    bkt = url.removeprefix("gs://")
    (created_dir / bkt).write_text("created", encoding="utf-8")

if args[:4] == ["alpha", "storage", "buckets", "rapid-caches"] and args[4] == "create":
    url = args[5]
    bkt = url.removeprefix("gs://")
    if not (created_dir / bkt).exists():
        sys.stderr.write(f"Bucket {{bkt}} was not created before rapid-caches create\\n")
        sys.exit(2)

with open({str(calls_file)!r}, "a", encoding="utf-8") as f:
    fcntl.flock(f, fcntl.LOCK_EX)
    f.write(json.dumps(args) + "\\n")
""",
    )

    overrides = {
        "_INFRA_PREFIX": "test-pfx",
        "_ZONE": "us-central1-c",
        "_BENCHMARK_TYPE": "IO",
        "_BUCKET_TYPES": "standard_rcu_warm hns_rcu_z",
    }
    init_res = _run_step(doc, "init-variables", workspace_dir, bin_dir, overrides)
    assert init_res.returncode == 0, init_res.stderr

    res = _run_step(doc, "create-buckets", workspace_dir, bin_dir, overrides)
    assert res.returncode == 0, f"stdout={res.stdout}\nstderr={res.stderr}"
    assert not (workspace_dir / "FAILED").exists()

    calls = [
        json.loads(line) for line in calls_file.read_text(encoding="utf-8").splitlines()
    ]
    # gcloud config must be initialized before the parallel bucket jobs start.
    assert calls[0] == ["config", "list"]

    std_bucket_calls = [
        c
        for c in calls
        if c[:3] == ["storage", "buckets", "create"]
        and "gs://test-pfx-standard-rcu-abcdef12" in c
    ]
    assert len(std_bucket_calls) == 1
    assert "--uniform-bucket-level-access" in std_bucket_calls[0]
    assert "--enable-hierarchical-namespace" not in std_bucket_calls[0]
    assert "--location=us-central1" in std_bucket_calls[0]

    hns_bucket_calls = [
        c
        for c in calls
        if c[:3] == ["storage", "buckets", "create"]
        and "gs://test-pfx-hns-rcu-abcdef12" in c
    ]
    assert len(hns_bucket_calls) == 1
    assert "--uniform-bucket-level-access" in hns_bucket_calls[0]
    assert "--enable-hierarchical-namespace" in hns_bucket_calls[0]
    assert "--location=us-central1" in hns_bucket_calls[0]

    for bkt in (
        "gs://test-pfx-standard-rcu-abcdef12",
        "gs://test-pfx-hns-rcu-abcdef12",
    ):
        cache_calls = [
            c
            for c in calls
            if c[:5] == ["alpha", "storage", "buckets", "rapid-caches", "create"]
            and bkt in c
        ]
        assert len(cache_calls) == 1
        call = cache_calls[0]
        assert call[5] == bkt
        assert call[6] == "us-central1-c"
        assert "--cache-type=rapid-cache-ultra" in call
        assert "--admission-policy=admit-on-first-miss" in call
        assert "--no-async" in call


def test_create_buckets_fails_when_background_rcu_creation_fails(tmp_path):
    doc = _load_cloudbuild()
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    _write_executable(
        bin_dir / "gcloud",
        """#!/usr/bin/env python3
import sys

args = sys.argv[1:]
if args[:3] == ["storage", "buckets", "describe"]:
    sys.exit(0)
if args[:5] == ["alpha", "storage", "buckets", "rapid-caches", "create"]:
    sys.stderr.write("simulated rapid-caches create failure\\n")
    sys.exit(1)
sys.exit(0)
""",
    )

    overrides = {
        "_INFRA_PREFIX": "test-pfx",
        "_ZONE": "us-central1-c",
        "_BENCHMARK_TYPE": "IO",
        "_BUCKET_TYPES": "standard_rcu_z",
    }
    assert (
        _run_step(doc, "init-variables", workspace_dir, bin_dir, overrides).returncode
        == 0
    )

    res = _run_step(doc, "create-buckets", workspace_dir, bin_dir, overrides)
    assert res.returncode != 0
    failed_steps = (workspace_dir / "FAILED").read_text(encoding="utf-8").splitlines()
    assert "create-buckets" in failed_steps


def test_run_benchmarks_passes_rcu_buckets_and_bucket_types_to_run_py(tmp_path):
    doc = _load_cloudbuild()
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    vm_home = tmp_path / "vm_home"
    vm_home.mkdir()
    run_py_calls = tmp_path / "run_py_calls.jsonl"

    _write_executable(bin_dir / "sleep", "#!/usr/bin/env bash\nexit 0\n")

    # Prepare fake remote ~/gcsfs/env/bin/activate that puts stub `python` first on PATH.
    venv_bin = vm_home / "gcsfs" / "env" / "bin"
    venv_bin.mkdir(parents=True)
    (venv_bin / "activate").write_text(
        f'export PATH="{venv_bin}:$PATH"\n', encoding="utf-8"
    )
    _write_executable(
        venv_bin / "python",
        f"""#!/usr/bin/env python3
import json
import sys

with open({str(run_py_calls)!r}, "a", encoding="utf-8") as f:
    f.write(json.dumps(sys.argv[1:]) + "\\n")
""",
    )

    _write_executable(
        bin_dir / "gcloud",
        f"""#!/usr/bin/env python3
import os
import subprocess
import sys

args = sys.argv[1:]
if args[:2] == ["compute", "scp"]:
    sys.exit(0)
if args[:2] == ["compute", "ssh"]:
    cmd = ""
    for a in args:
        if a.startswith("--command="):
            cmd = a.split("=", 1)[1]
            break
    if "apt-get install" in cmd:
        sys.exit(0)
    env = os.environ.copy()
    env["HOME"] = {str(vm_home)!r}
    res = subprocess.run(["bash", "-c", cmd], cwd={str(vm_home)!r}, env=env)
    sys.exit(res.returncode)
sys.exit(0)
""",
    )

    overrides = {
        "_INFRA_PREFIX": "test-pfx",
        "_ZONE": "us-central1-c",
        "_BENCHMARK_TYPE": "IO",
        "_BUCKET_TYPES": "standard_rcu_z hns_rcu_cold",
        "_BENCHMARK_CONFIG": "cat:cat_file_whole",
    }
    assert (
        _run_step(doc, "init-variables", workspace_dir, bin_dir, overrides).returncode
        == 0
    )

    res = _run_step(doc, "run-benchmarks", workspace_dir, bin_dir, overrides)
    assert res.returncode == 0, f"stdout={res.stdout}\nstderr={res.stderr}"

    invocations = [
        json.loads(line)
        for line in run_py_calls.read_text(encoding="utf-8").splitlines()
    ]
    assert len(invocations) == 1
    argv = invocations[0]
    assert argv[0] == "gcsfs/tests/perf/microbenchmarks/run.py"
    assert "--group=cat" in argv
    assert "--config=cat_file_whole" in argv
    assert "--standard-rcu-bucket=test-pfx-standard-rcu-abcdef12" in argv
    assert "--hns-rcu-bucket=test-pfx-hns-rcu-abcdef12" in argv

    bt_idx = argv.index("--bucket-types")
    assert argv[bt_idx + 1 : bt_idx + 3] == ["standard_rcu_z", "hns_rcu_cold"]


def test_delete_buckets_and_cleanup_leaked_resources_handle_rcu_buckets(tmp_path):
    doc = _load_cloudbuild()
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls_file = tmp_path / "gcloud_cleanup_calls.jsonl"
    calls_file.write_text("", encoding="utf-8")

    _write_executable(
        bin_dir / "gcloud",
        f"""#!/usr/bin/env python3
import fcntl
import json
import sys

args = sys.argv[1:]
with open({str(calls_file)!r}, "a", encoding="utf-8") as f:
    fcntl.flock(f, fcntl.LOCK_EX)
    f.write(json.dumps(args) + "\\n")

if args[:3] == ["compute", "instances", "list"]:
    sys.exit(0)

if args[:3] == ["storage", "buckets", "list"]:
    # 1 old leaked standard-rcu bucket, 1 young hns-rcu bucket, 1 old run-results bucket
    print("test-pfx-standard-rcu-11112222\\t2026-01-01T00:00:00+0000")
    print("test-pfx-hns-rcu-abcdef12\\t2099-01-01T00:00:00+0000")
    print("test-pfx-run-results\\t2026-01-01T00:00:00+0000")
    sys.exit(0)

if args[:5] == ["alpha", "storage", "buckets", "rapid-caches", "list"]:
    if "gs://test-pfx-standard-rcu-11112222" in args:
        print("test-pfx-standard-rcu-11112222/us-central1-c\\trunning")
        print("test-pfx-standard-rcu-11112222/us-central1-a\\tdisabled")
    sys.exit(0)

sys.exit(0)
""",
    )

    overrides = {
        "_INFRA_PREFIX": "test-pfx",
        "_ZONE": "us-central1-c",
        "_BENCHMARK_TYPE": "IO",
        "_BUCKET_TYPES": "standard_rcu_cold hns_rcu_warm",
    }
    assert (
        _run_step(doc, "init-variables", workspace_dir, bin_dir, overrides).returncode
        == 0
    )

    del_res = _run_step(doc, "delete-buckets", workspace_dir, bin_dir, overrides)
    assert del_res.returncode == 0, del_res.stderr

    calls = [
        json.loads(line) for line in calls_file.read_text(encoding="utf-8").splitlines()
    ]
    assert calls[0] == ["config", "list"]
    assert any(
        c[:5] == ["alpha", "storage", "buckets", "rapid-caches", "disable"]
        and "test-pfx-standard-rcu-abcdef12/us-central1-c" in c
        for c in calls
    )
    assert any(
        c[:5] == ["alpha", "storage", "buckets", "rapid-caches", "disable"]
        and "test-pfx-hns-rcu-abcdef12/us-central1-c" in c
        for c in calls
    )
    assert any(
        c[:3] == ["storage", "buckets", "delete"]
        and "gs://test-pfx-standard-rcu-abcdef12" in c
        for c in calls
    )
    assert any(
        c[:3] == ["storage", "buckets", "delete"]
        and "gs://test-pfx-hns-rcu-abcdef12" in c
        for c in calls
    )

    calls_file.write_text("", encoding="utf-8")
    leak_res = _run_step(
        doc, "cleanup-leaked-resources", workspace_dir, bin_dir, overrides
    )
    assert leak_res.returncode == 0, leak_res.stderr

    leak_calls = [
        json.loads(line) for line in calls_file.read_text(encoding="utf-8").splitlines()
    ]
    disable_calls = [
        c
        for c in leak_calls
        if c[:5] == ["alpha", "storage", "buckets", "rapid-caches", "disable"]
    ]
    assert len(disable_calls) == 1
    assert "test-pfx-standard-rcu-11112222/us-central1-c" in disable_calls[0]
    assert "test-pfx-standard-rcu-11112222/us-central1-a" not in disable_calls[0]

    assert any(
        "gs://test-pfx-standard-rcu-11112222" in c
        and c[:2] in (["storage", "rm"], ["storage", "buckets"])
        for c in leak_calls
    )
    assert not any("test-pfx-hns-rcu-abcdef12" in " ".join(c) for c in leak_calls)
    assert not any("test-pfx-run-results" in " ".join(c) for c in leak_calls)


def test_cleanup_leaked_resources_preserves_rapid_caches_list_stderr(tmp_path):
    doc = _load_cloudbuild()
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    _write_executable(
        bin_dir / "gcloud",
        """#!/usr/bin/env python3
import sys

args = sys.argv[1:]
if args[:3] == ["storage", "buckets", "list"]:
    print("test-pfx-standard-rcu-11112222\\t2026-01-01T00:00:00+0000")
    sys.exit(0)
if args[:5] == ["alpha", "storage", "buckets", "rapid-caches", "list"]:
    sys.stderr.write("rapid-caches list permission denied\\n")
    sys.exit(1)
sys.exit(0)
""",
    )

    overrides = {
        "_INFRA_PREFIX": "test-pfx",
        "_ZONE": "us-central1-c",
        "_BENCHMARK_TYPE": "IO",
    }
    assert (
        _run_step(doc, "init-variables", workspace_dir, bin_dir, overrides).returncode
        == 0
    )

    leak_res = _run_step(
        doc, "cleanup-leaked-resources", workspace_dir, bin_dir, overrides
    )
    assert leak_res.returncode == 0
    assert "rapid-caches list permission denied" in leak_res.stderr
