"""Tests for the Rapid Cache Ultra (RCU) bucket types in the pipeline steps.

Each step runs its real script under bash the way Cloud Build does: from the
repo root, with only the env entries its macrobenchmarks-cloudbuild.yaml step
passes (so the wiring is tested too). Stub ``gcloud``/``kubectl``/``helm``/
``curl``/``sleep`` executables on PATH log every call, print canned output and
fail on demand; ``python3``/``pip3`` fail, as the gcloud image has neither.
"""

import os
import re
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[4]
STEPS = {
    step["id"]: step
    for step in yaml.safe_load(
        (
            REPO / "cloudbuild/macrobenchmarks/macrobenchmarks-cloudbuild.yaml"
        ).read_text()
    )["steps"]
}
PLUMBING = ("PATH", "STUB_LOG", "STUB_MANIFESTS", "FAILED_FILE", "BUILD_VARS_FILE")

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")

RCU_TYPES = [
    "standard_rcu_z",
    "standard_rcu_warm",
    "standard_rcu_cold",
    "hns_rcu_z",
    "hns_rcu_warm",
    "hns_rcu_cold",
]
SEED = "gs://ckpt-bucket/seed/buildid-test-seed/model.ckpt"
GiB = 1 << 30

# STUB_FAIL is a regex matched against the arguments (its error message is
# STUB_FAIL_MESSAGE); STUB_FAIL_ONCE fails only the first matching call. The
# STUB_* outputs are printed verbatim.
GCLOUD_STUB = r"""#!/usr/bin/env bash
echo "gcloud $*" >> "$STUB_LOG"
if [[ -n "$STUB_FAIL_ONCE" && "$*" =~ $STUB_FAIL_ONCE && ! -e "$STUB_LOG.failed" ]]; then
  touch "$STUB_LOG.failed"; echo "ERROR: transient error" >&2; exit 1
fi
if [[ -n "$STUB_FAIL" && "$*" =~ $STUB_FAIL ]]; then echo "$STUB_FAIL_MESSAGE" >&2; exit 1; fi
case "$*" in
  "storage objects list"*) [ -n "$STUB_LIST" ] && printf '%s\n' "$STUB_LIST" ;;
  "storage buckets describe"*"value(location)"*) echo "US-CENTRAL1" ;;
  "alpha storage buckets rapid-caches describe"*) echo "${STUB_CACHE_STATE-running}" ;;
  "alpha storage buckets rapid-caches list gs://"*)
    # BUCKET in STUB_CACHES stands for the listed bucket.
    b="${6#gs://}"; [ -n "$STUB_CACHES" ] && printf '%s\n' "${STUB_CACHES//BUCKET/$b}" ;;
  "storage buckets list"*) [ -n "$STUB_BUCKETS" ] && printf '%s\n' "$STUB_BUCKETS" ;;
  "storage du"*) [ -n "$STUB_DU" ] && printf '%s\n' "$STUB_DU" ;;
  "storage ls"*) [ -n "$STUB_LS" ] && printf '%s\n' "$STUB_LS" ;;
esac
exit 0
"""

KUBECTL_STUB = r"""#!/usr/bin/env bash
echo "kubectl $*" >> "$STUB_LOG"
if [[ -n "$STUB_FAIL" && "$*" =~ $STUB_FAIL ]]; then exit 1; fi
case "$*" in
  "apply -f -"*) { cat; echo "---"; } >> "$STUB_MANIFESTS" ;;
  "get jobset "*Completed*) echo True ;;
  "get job "*) echo "${STUB_JOB_STATUS-1/}" ;;
esac
exit 0
"""


def _stub(body):
    return f"#!/usr/bin/env bash\n{body}\n"


@pytest.fixture
def env(tmp_path):
    """Stub plumbing plus default substitution values."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in {
        "gcloud": GCLOUD_STUB,
        "kubectl": KUBECTL_STUB,
        "helm": _stub('echo "helm $*" >> "$STUB_LOG"'),
        "curl": _stub('echo "curl $*" >> "$STUB_LOG"'),
        "sleep": _stub('echo "sleep $*" >> "$STUB_LOG"'),
        "python3": _stub('echo "python3 is not in the gcloud image" >&2; exit 1'),
        "pip3": _stub('echo "pip3 is not in the gcloud image" >&2; exit 1'),
    }.items():
        stub = bin_dir / name
        stub.write_text(body)
        stub.chmod(0o755)

    build_vars = tmp_path / "build_vars.env"
    build_vars.write_text(
        "export RUN_ID=buildid-test\n"
        "export CLUSTER_NAME=test-cluster\n"
        "export CLUSTER_LOCATION=us-central1-a\n"
        "export CHECKPOINT_BUCKET=ckpt-bucket\n"
        "export DATASET_BUCKET=dataset-bucket\n"
        "export RESULTS_BUCKET=results-bucket\n"
        "export REGION=us-central1\n"
    )
    return {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "STUB_LOG": str(tmp_path / "calls.log"),
        "STUB_MANIFESTS": str(tmp_path / "manifests.yaml"),
        "FAILED_FILE": str(tmp_path / "FAILED"),
        "BUILD_VARS_FILE": str(build_vars),
        "PROJECT_ID": "test-project",
        "LOCATION": "us-central1",
        "_INFRA_PREFIX": "pfx",
        "_ZONE": "us-central1-a",
        "_MACHINE_TYPE": "test-machine",
        "_IMAGE": "test-image:1",
        "_REQUIREMENTS": "gcsfs-under-test other-pkg",
        "_DATASET_PATH": "gs://src-bucket",
        "_BUCKET_TYPE": "regional",
        "_SEED_CHECKPOINT": "true",
        "_SKIP_CLEANUP": "false",
    }


def _run_step(step_id, env, **overrides):
    """Run a step like Cloud Build: its script, from the repo root, with only
    its env entries (substituted from env/overrides) plus the stub plumbing."""
    values = {**env, **overrides}
    step = STEPS[step_id]
    run_env = {k: values[k] for k in PLUMBING}
    run_env.update({k: v for k, v in values.items() if k.startswith("STUB_")})
    for entry in step.get("env", []):
        name, template = entry.split("=", 1)
        run_env[name] = re.sub(
            r"\$\{(\w+)\}", lambda m: values.get(m.group(1), ""), template
        )
    proc = subprocess.run(
        ["bash", *step["args"]], cwd=REPO, env=run_env, capture_output=True, text=True
    )
    log = Path(values["STUB_LOG"])
    calls = log.read_text().splitlines() if log.exists() else []
    return proc, calls


def _calls(calls, prefix):
    return [c for c in calls if c.startswith(prefix)]


def _failures(env):
    failed = Path(env["FAILED_FILE"])
    return failed.read_text().split() if failed.exists() else []


def _build_var(env, name):
    """The value a later step sees after sourcing build_vars.env (last wins)."""
    value = None
    for line in Path(env["BUILD_VARS_FILE"]).read_text().splitlines():
        if line.startswith(f"export {name}="):
            value = line.split("=", 1)[1]
    return value


def _add_build_vars(env, **values):
    with open(env["BUILD_VARS_FILE"], "a") as f:
        for name, value in values.items():
            f.write(f"export {name}={value}\n")


def _job(env):
    """The one Job applied with kubectl: (job, pod spec, container)."""
    manifests = [
        m for m in yaml.safe_load_all(Path(env["STUB_MANIFESTS"]).read_text()) if m
    ]
    (job,) = [m for m in manifests if m["kind"] == "Job"]
    pod = job["spec"]["template"]["spec"]
    (container,) = pod["containers"]
    return job, pod, container


def test_rcu_steps_run_after_provisioning_and_seeding_and_before_the_clock():
    assert sorted(STEPS["rcu-stage"]["waitFor"]) == ["create-buckets", "create-cluster"]
    assert STEPS["seed-checkpoint"]["waitFor"] == ["rcu-stage"]
    assert STEPS["rcu-prepare-reads"]["waitFor"] == ["seed-checkpoint"]
    assert STEPS["run-workload"]["waitFor"] == ["rcu-prepare-reads"]


# --- init-variables -------------------------------------------------------


@pytest.fixture
def init_env(env):
    Path(env["BUILD_VARS_FILE"]).write_text("")
    return {
        **env,
        "BUILD_ID": "1234abcd-0000",
        "BRANCH_NAME": "main",
        "_GKE_SERVICE_ACCOUNT": "sa@example.com",
        "_WORKLOAD": "hf-datasets-pytorch-lightning",
        "_TRAINING_STRATEGY": "ddp",
        "_ENABLE_TIER1_NETWORKING": "true",
        "_SIMULATED_STEP_COMPUTE_SECONDS": "0.0",
        "_NODES": "2",
        "_RANKS_PER_NODE": "4",
        "_CHECKPOINT_INTERVAL": "25",
        "_CKPT_TO_KEEP": "1",
        "_PER_DEVICE_BATCH": "8",
        "_GRAD_ACCUM": "1",
        "_TENSOR_PARALLEL_SIZE": "4",
        "_DATA_PARALLEL_SIZE": "2",
        "_DATALOADER_WORKERS": "4",
        "_EPOCHS": "3",
        "_SHUFFLE_BUFFER_SIZE": "10000",
        "_SHUFFLE_MAX_BUFFER_INPUT_SHARDS": "10",
        "_DATALOADER_PREFETCH_FACTOR": "2",
    }


@pytest.mark.parametrize("bucket_type", RCU_TYPES)
def test_init_accepts_rcu_bucket_types(init_env, bucket_type):
    proc, _ = _run_step("init-variables", init_env, _BUCKET_TYPE=bucket_type)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _build_var(init_env, "RUN_ID") == "buildid-1234abcd"


@pytest.mark.parametrize("bucket_type", ["rcu_z", "standard_rcu", "standard_rcu_hot"])
def test_init_rejects_unknown_rcu_like_types_before_any_gcloud_call(
    init_env, bucket_type
):
    proc, calls = _run_step("init-variables", init_env, _BUCKET_TYPE=bucket_type)

    assert proc.returncode == 1
    assert "_BUCKET_TYPE" in proc.stdout
    assert _calls(calls, "gcloud") == []


def test_init_rejects_external_checkpoint_with_rcu(init_env):
    proc, _ = _run_step(
        "init-variables",
        init_env,
        _BUCKET_TYPE="standard_rcu_warm",
        _CHECKPOINT_LOAD_PATH="gs://somewhere/ckpt",
    )

    assert proc.returncode == 1
    assert "_CHECKPOINT_LOAD_PATH" in proc.stdout
    assert _build_var(init_env, "RUN_ID") is None


@pytest.mark.parametrize("settle", ["", "0", "45"])
def test_init_accepts_empty_or_integer_warmup_settle_seconds(init_env, settle):
    proc, _ = _run_step(
        "init-variables",
        init_env,
        _BUCKET_TYPE="standard_rcu_warm",
        _RCU_WARMUP_SETTLE_SECONDS=settle,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr


@pytest.mark.parametrize("settle", ["1.5", "abc", "-5"])
def test_init_rejects_non_integer_warmup_settle_seconds(init_env, settle):
    proc, _ = _run_step(
        "init-variables",
        init_env,
        _BUCKET_TYPE="standard_rcu_warm",
        _RCU_WARMUP_SETTLE_SECONDS=settle,
    )

    assert proc.returncode == 1
    assert "_RCU_WARMUP_SETTLE_SECONDS" in proc.stdout


# --- create-buckets -------------------------------------------------------


def _copies(calls):
    return [
        c for c in calls if c.startswith(("gcloud storage rsync", "gcloud storage cp"))
    ]


@pytest.mark.parametrize(
    ("bucket_type", "hns"),
    [("standard_rcu_z", False), ("standard_rcu_cold", False), ("hns_rcu_warm", True)],
)
def test_create_buckets_adds_a_rapid_cache_in_the_run_zone(env, bucket_type, hns):
    proc, calls = _run_step("create-buckets", env, _BUCKET_TYPE=bucket_type)

    assert proc.returncode == 0, proc.stderr
    assert _failures(env) == []
    for bucket in ("ckpt-bucket", "dataset-bucket"):
        (create,) = _calls(calls, f"gcloud storage buckets create gs://{bucket} ")
        assert "--location=us-central1" in create
        assert "--uniform-bucket-level-access" in create
        assert ("--enable-hierarchical-namespace" in create) == hns
        assert "--placement" not in create and "RAPID" not in create
        (cache,) = _calls(
            calls,
            f"gcloud alpha storage buckets rapid-caches create gs://{bucket} us-central1-a ",
        )
        assert "--cache-type=rapid-cache-ultra" in cache
        assert "--admission-policy=admit-on-first-miss" in cache
        assert "--no-async" in cache
        assert calls.index(create) < calls.index(cache)


@pytest.mark.parametrize(
    "stub", [{"STUB_FAIL": "rapid-caches create"}, {"STUB_CACHE_STATE": "creating"}]
)
def test_create_buckets_records_failure_when_cache_is_not_running(env, stub):
    proc, calls = _run_step(
        "create-buckets", env, _BUCKET_TYPE="standard_rcu_warm", **stub
    )

    assert proc.returncode != 0
    assert _failures(env) == ["create-buckets"]
    assert _copies(calls) == []


def _cache_creates(calls, bucket):
    return _calls(
        calls, f"gcloud alpha storage buckets rapid-caches create gs://{bucket} "
    )


def test_create_buckets_retries_a_failed_rapid_cache_create(env):
    proc, calls = _run_step(
        "create-buckets",
        env,
        _BUCKET_TYPE="standard_rcu_warm",
        STUB_FAIL_ONCE="rapid-caches create",
    )

    assert proc.returncode == 0, proc.stderr
    assert _failures(env) == []
    first, second = [
        i for i, c in enumerate(calls) if c in _cache_creates(calls, "ckpt-bucket")
    ]
    assert [c.split()[0] for c in calls[first + 1 : second]] == ["sleep"]
    assert len(_cache_creates(calls, "dataset-bucket")) == 1


def test_create_buckets_treats_already_exists_on_a_retry_as_created(env):
    proc, calls = _run_step(
        "create-buckets",
        env,
        _BUCKET_TYPE="hns_rcu_cold",
        STUB_FAIL_ONCE="rapid-caches create gs://ckpt-bucket ",
        STUB_FAIL="rapid-caches create gs://ckpt-bucket ",
        STUB_FAIL_MESSAGE="ERROR: HTTPError 409: The rapid cache already exists.",
    )

    assert proc.returncode == 0, proc.stderr
    assert _failures(env) == []
    assert len(_cache_creates(calls, "ckpt-bucket")) == 2


def test_create_buckets_gives_up_after_three_rapid_cache_create_attempts(env):
    proc, calls = _run_step(
        "create-buckets",
        env,
        _BUCKET_TYPE="standard_rcu_z",
        STUB_FAIL="rapid-caches create",
    )

    assert proc.returncode != 0
    assert _failures(env) == ["create-buckets"]
    assert len(_cache_creates(calls, "ckpt-bucket")) == 3
    assert _calls(calls, "gcloud storage buckets create gs://dataset-bucket ") == []


def test_create_buckets_keeps_polling_after_a_failed_describe(env):
    proc, calls = _run_step(
        "create-buckets",
        env,
        _BUCKET_TYPE="standard_rcu_warm",
        STUB_FAIL_ONCE="rapid-caches describe",
    )

    assert proc.returncode == 0, proc.stderr
    assert _failures(env) == []
    describes = _calls(
        calls, "gcloud alpha storage buckets rapid-caches describe ckpt-bucket/"
    )
    assert len(describes) == 2


@pytest.mark.parametrize("bucket_type", ["standard_rcu_z", "hns_rcu_z"])
def test_create_buckets_leaves_z_dataset_to_rcu_stage(env, bucket_type):
    proc, calls = _run_step(
        "create-buckets",
        env,
        _BUCKET_TYPE=bucket_type,
        _DATASET_PATH="gs://src-bucket/data/parquet",
    )

    assert proc.returncode == 0, proc.stderr
    assert _copies(calls) == []
    assert _build_var(env, "RUN_DATASET_PATH") == "gs://dataset-bucket/data/parquet"


RSYNC_COPY = (
    "gcloud storage rsync --recursive gs://src-bucket/data gs://dataset-bucket/data"
)
DAISY_CHAIN_COPY = "gcloud storage cp --recursive --daisy-chain gs://src-bucket/data/* gs://dataset-bucket"


@pytest.mark.parametrize(
    ("bucket_type", "src_is_rapid", "copy"),
    [
        ("standard_rcu_warm", "no", RSYNC_COPY),
        ("hns_rcu_cold", "yes", DAISY_CHAIN_COPY),
        ("regional", "no", RSYNC_COPY),
        ("zonal", "no", DAISY_CHAIN_COPY),
    ],
)
def test_create_buckets_copies_dataset_with_gcloud(
    env, bucket_type, src_is_rapid, copy
):
    _add_build_vars(env, DATASET_SRC_IS_RAPID=src_is_rapid)

    proc, calls = _run_step(
        "create-buckets",
        env,
        _BUCKET_TYPE=bucket_type,
        _DATASET_PATH="gs://src-bucket/data",
    )

    assert proc.returncode == 0, proc.stderr
    assert _copies(calls) == [copy]
    assert _build_var(env, "RUN_DATASET_PATH") == "gs://dataset-bucket/data"


@pytest.mark.parametrize("bucket_type", ["regional", "zonal", "hns"])
def test_create_buckets_adds_no_cache_for_non_rcu_types(env, bucket_type):
    proc, calls = _run_step("create-buckets", env, _BUCKET_TYPE=bucket_type)

    assert proc.returncode == 0, proc.stderr
    assert _calls(calls, "gcloud alpha") == []


# --- delete-buckets / cleanup-leaked-resources ------------------------------


@pytest.mark.parametrize("bucket", ["ckpt-bucket", "dataset-bucket"])
def test_delete_buckets_disables_caches_between_object_and_bucket_deletion(env, bucket):
    proc, calls = _run_step(
        "delete-buckets", env, STUB_CACHES="BUCKET/us-central1-a\trunning"
    )

    assert proc.returncode == 0, proc.stderr
    mine = [c for c in calls if bucket in c]
    expected = [
        "gcloud storage rm --recursive",
        f"gcloud alpha storage buckets rapid-caches list gs://{bucket} ",
        f"gcloud alpha storage buckets rapid-caches disable {bucket}/us-central1-a ",
        f"gcloud storage buckets delete gs://{bucket} ",
    ]
    assert len(mine) == len(expected), mine
    for call, prefix in zip(mine, expected):
        assert call.startswith(prefix), (call, prefix)


def test_delete_buckets_keeps_going_when_teardown_calls_fail(env):
    proc, calls = _run_step(
        "delete-buckets",
        env,
        STUB_CACHES="BUCKET/us-central1-a\trunning",
        STUB_FAIL="storage rm|rapid-caches|buckets delete",
    )

    assert proc.returncode == 0, proc.stderr
    deletes = _calls(calls, "gcloud storage buckets delete")
    assert [d.split()[4] for d in deletes] == [
        "gs://ckpt-bucket",
        "gs://dataset-bucket",
    ]


def test_reaper_deletes_old_run_buckets_and_disables_their_caches(env):
    now = datetime.now(timezone.utc)
    old = (now - timedelta(hours=7)).strftime("%Y-%m-%dT%H:%M:%S+0000")
    young = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S+0000")
    buckets = [
        f"pfx-macrobench-checkpoint-aaaaaaaa\t{old}",
        f"pfx-macrobench-dataset-bbbbbbbb\t{old}",
        f"pfx-macrobench-dataset-cccccccc\t{young}",
        f"pfx-macrobench-results\t{old}",
        f"other-macrobench-dataset-dddddddd\t{old}",
    ]

    proc, calls = _run_step(
        "cleanup-leaked-resources",
        env,
        STUB_BUCKETS="\n".join(buckets),
        STUB_CACHES="BUCKET/us-central1-a\trunning",
    )

    assert proc.returncode == 0, proc.stderr
    disabled = [
        c.split()[6]
        for c in _calls(calls, "gcloud alpha storage buckets rapid-caches disable")
    ]
    deleted = [c.split()[4] for c in _calls(calls, "gcloud storage buckets delete")]
    assert disabled == [
        "pfx-macrobench-checkpoint-aaaaaaaa/us-central1-a",
        "pfx-macrobench-dataset-bbbbbbbb/us-central1-a",
    ]
    assert deleted == [
        "gs://pfx-macrobench-checkpoint-aaaaaaaa",
        "gs://pfx-macrobench-dataset-bbbbbbbb",
    ]


# --- rcu-stage / rcu-prepare-reads ----------------------------------------


@pytest.mark.parametrize("bucket_type", ["regional", "zonal", "hns"])
@pytest.mark.parametrize("step", ["rcu-stage", "rcu-prepare-reads"])
def test_rcu_steps_do_nothing_for_non_rcu_types(env, step, bucket_type):
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/", SEEDED_CKPT_PATH=SEED)

    proc, calls = _run_step(step, env, _BUCKET_TYPE=bucket_type)

    assert proc.returncode == 0, proc.stderr
    assert calls == []


def test_rcu_stage_z_copies_the_dataset_in_a_job_on_the_workload_pool(env):
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/")

    proc, calls = _run_step(
        "rcu-stage",
        env,
        _BUCKET_TYPE="hns_rcu_z",
        STUB_LIST="a.parquet\tRAPID\nsub/b.parquet\tRAPID",
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _failures(env) == []
    job, pod, container = _job(env)
    assert job["spec"]["backoffLimit"] == 0
    assert pod["nodeSelector"] == {"cloud.google.com/gke-nodepool": "test-machine"}
    assert container["image"] == "test-image:1"
    assert container["args"] == [
        "stage-rapid",
        "gs://src-bucket",
        "gs://dataset-bucket/",
    ]
    # The RAPID check lists the staged objects after the Job finished.
    (listing,) = _calls(calls, "gcloud storage objects list")
    assert listing.startswith("gcloud storage objects list gs://dataset-bucket/** ")
    assert calls.index(listing) > calls.index(_calls(calls, "kubectl get job ")[-1])


def test_rcu_job_installs_requirements_then_runs_the_mounted_prepare_py(env, tmp_path):
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/")
    proc, calls = _run_step(
        "rcu-stage", env, _BUCKET_TYPE="standard_rcu_z", STUB_LIST="a\tRAPID"
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    _, pod, container = _job(env)

    # The ConfigMap made from rcu/prepare.py is what the container mounts at /rcu.
    (create_configmap,) = _calls(calls, "kubectl create configmap ")
    configmap = create_configmap.split()[3]
    source = create_configmap.split("--from-file=prepare.py=")[1].split()[0]
    assert (
        REPO / source
    ).resolve() == REPO / "cloudbuild/macrobenchmarks/rcu/prepare.py"
    (volume,) = pod["volumes"]
    (mount,) = container["volumeMounts"]
    assert volume["configMap"]["name"] == configmap
    assert (mount["name"], mount["mountPath"]) == (volume["name"], "/rcu")

    # Run the container command with stub pip3/python3.
    container_bin = tmp_path / "container_bin"
    container_bin.mkdir()
    for tool in ("pip3", "python3"):
        stub = container_bin / tool
        stub.write_text(_stub(f'echo "{tool} $*" >> "{tmp_path}/container.log"'))
        stub.chmod(0o755)
    container_env = {e["name"]: e["value"] for e in container["env"]}
    subprocess.run(
        container["command"] + container["args"],
        env={"PATH": f"{container_bin}:/usr/bin:/bin", **container_env},
        check=True,
    )
    assert (tmp_path / "container.log").read_text().splitlines() == [
        "pip3 install --no-cache-dir gcsfs-under-test other-pkg",
        "python3 /rcu/prepare.py stage-rapid gs://src-bucket gs://dataset-bucket/",
    ]


@pytest.mark.parametrize(
    "listing", ["", "a.parquet\tRAPID\nb.parquet\tSTANDARD"], ids=["empty", "standard"]
)
def test_rcu_stage_z_fails_unless_every_staged_object_is_rapid(env, listing):
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/")

    proc, _ = _run_step(
        "rcu-stage", env, _BUCKET_TYPE="standard_rcu_z", STUB_LIST=listing
    )

    assert proc.returncode != 0
    assert _failures(env) == ["rcu-stage"]


@pytest.mark.parametrize("status", ["/1", "/"], ids=["failed", "never-finished"])
def test_rcu_stage_records_failure_and_prints_logs_when_the_job_fails(env, status):
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/")

    proc, calls = _run_step(
        "rcu-stage", env, _BUCKET_TYPE="standard_rcu_z", STUB_JOB_STATUS=status
    )

    assert proc.returncode != 0
    assert _failures(env) == ["rcu-stage"]
    assert _calls(calls, "kubectl logs job/buildid-test-rcu-")
    assert _calls(calls, "gcloud storage objects list") == []


def test_rcu_stage_records_failure_when_the_job_cannot_be_created(env):
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/")

    proc, calls = _run_step(
        "rcu-stage", env, _BUCKET_TYPE="standard_rcu_z", STUB_FAIL="^apply"
    )

    assert proc.returncode != 0
    assert _failures(env) == ["rcu-stage"]
    assert _calls(calls, "kubectl get job") == []


@pytest.mark.parametrize(
    ("bucket_type", "listing", "failures"),
    [
        ("standard_rcu_warm", "a.parquet\tSTANDARD", []),
        ("hns_rcu_cold", "a.parquet\tSTANDARD", []),
        ("standard_rcu_warm", "a.parquet\tRAPID", ["rcu-stage"]),
    ],
)
def test_rcu_stage_checks_warm_and_cold_datasets_are_standard_by_listing(
    env, bucket_type, listing, failures
):
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/")

    _, calls = _run_step("rcu-stage", env, _BUCKET_TYPE=bucket_type, STUB_LIST=listing)

    assert _failures(env) == failures
    (listing_call,) = _calls(calls, "gcloud storage objects list")
    assert listing_call.startswith(
        "gcloud storage objects list gs://dataset-bucket/** "
    )
    _, _, container = _job(env)
    assert container["args"] == ["check", "gs://dataset-bucket/", "gs://ckpt-bucket"]


@pytest.mark.parametrize(
    ("listing", "failures"),
    [("model.ckpt\tRAPID", []), ("model.ckpt\tSTANDARD", ["rcu-prepare-reads"])],
)
def test_prepare_reads_z_checks_the_seed_checkpoint_is_rapid(env, listing, failures):
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/", SEEDED_CKPT_PATH=SEED)

    _, calls = _run_step(
        "rcu-prepare-reads", env, _BUCKET_TYPE="standard_rcu_z", STUB_LIST=listing
    )

    assert _failures(env) == failures
    (listing_call,) = _calls(calls, "gcloud storage objects list")
    assert listing_call.startswith(f"gcloud storage objects list {SEED}** ")
    assert _calls(calls, "kubectl") == []


@pytest.mark.parametrize("bucket_type", ["standard_rcu_z", "hns_rcu_cold"])
def test_prepare_reads_does_nothing_for_fresh_z_and_cold_runs(env, bucket_type):
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/")

    proc, calls = _run_step("rcu-prepare-reads", env, _BUCKET_TYPE=bucket_type)

    assert proc.returncode == 0, proc.stderr
    assert calls == []


def test_prepare_reads_warm_reads_dataset_and_seed_then_waits_for_ingest(env):
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/", SEEDED_CKPT_PATH=SEED)

    proc, calls = _run_step(
        "rcu-prepare-reads",
        env,
        _BUCKET_TYPE="hns_rcu_warm",
        STUB_LIST="model.ckpt\tSTANDARD",
        STUB_DU=f"{5 * GiB}    gs://dataset-bucket/\n{45 * GiB}    {SEED}",
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _failures(env) == []
    _, pod, container = _job(env)
    assert pod["nodeSelector"] == {"cloud.google.com/gke-nodepool": "test-machine"}
    assert container["args"] == ["warm", "gs://dataset-bucket/", SEED]
    (seed_check,) = _calls(calls, "gcloud storage objects list")
    assert seed_check.startswith(f"gcloud storage objects list {SEED}** ")
    assert calls.index(seed_check) < calls.index(_calls(calls, "kubectl apply")[0])
    (du,) = _calls(calls, "gcloud storage du")
    assert du.split()[-2:] == ["gs://dataset-bucket/", SEED]
    # 120 s + 1.5 s per GiB of the warmed objects (50 GiB), after the Job
    # finished.
    assert calls[-1] == "sleep 195"


def test_prepare_reads_warm_fresh_run_reads_only_the_dataset(env):
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/")

    proc, calls = _run_step(
        "rcu-prepare-reads",
        env,
        _BUCKET_TYPE="standard_rcu_warm",
        STUB_DU=f"{5 * GiB}    gs://dataset-bucket/",
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    _, _, container = _job(env)
    assert container["args"] == ["warm", "gs://dataset-bucket/"]
    assert _calls(calls, "gcloud storage objects list") == []
    assert calls[-1] == "sleep 127"


def test_prepare_reads_warm_records_failure_when_du_fails(env):
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/")

    proc, calls = _run_step(
        "rcu-prepare-reads", env, _BUCKET_TYPE="hns_rcu_warm", STUB_FAIL="storage du"
    )

    assert proc.returncode != 0
    # The ERR trap also runs in the $(...) subshell, so the step is recorded
    # twice. check-failure dedupes the ledger.
    assert set(_failures(env)) == {"rcu-prepare-reads"}
    assert _calls(calls, "sleep") == []


def test_prepare_reads_warm_settle_override_skips_sizing(env):
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/", SEEDED_CKPT_PATH=SEED)

    proc, calls = _run_step(
        "rcu-prepare-reads",
        env,
        _BUCKET_TYPE="standard_rcu_warm",
        _RCU_WARMUP_SETTLE_SECONDS="7",
        STUB_LIST="model.ckpt\tSTANDARD",
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _calls(calls, "gcloud storage du") == []
    assert calls[-1] == "sleep 7"


def test_prepare_reads_cold_restore_rewrites_the_dataset_and_copies_the_seed(env):
    _add_build_vars(
        env,
        RUN_DATASET_PATH="gs://dataset-bucket/data",
        SEEDED_CKPT_PATH=SEED,
        DATASET_SRC_IS_RAPID="no",
    )

    proc, calls = _run_step(
        "rcu-prepare-reads",
        env,
        _BUCKET_TYPE="standard_rcu_cold",
        _DATASET_PATH="gs://src-bucket/data",
        STUB_LIST="model.ckpt\tSTANDARD",
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _failures(env) == []
    (seed_check,) = _calls(calls, "gcloud storage objects list")
    assert seed_check.startswith(f"gcloud storage objects list {SEED}** ")
    # rsync would skip objects that are already there, so delete them first.
    (delete,) = _calls(calls, "gcloud storage rm")
    assert delete.startswith("gcloud storage rm gs://dataset-bucket/data/**")
    assert _copies(calls) == [
        "gcloud storage rsync --recursive gs://src-bucket/data gs://dataset-bucket/data",
        f"gcloud storage cp --recursive {SEED} gs://ckpt-bucket/seed-cold/",
    ]
    assert calls.index(delete) < calls.index(_copies(calls)[0])
    assert (
        _build_var(env, "SEEDED_CKPT_PATH") == "gs://ckpt-bucket/seed-cold/model.ckpt"
    )
    assert _calls(calls, "kubectl") == []


def test_prepare_reads_cold_records_failure_when_the_dataset_copy_fails(env):
    _add_build_vars(env, RUN_DATASET_PATH="gs://dataset-bucket/", SEEDED_CKPT_PATH=SEED)

    proc, _ = _run_step(
        "rcu-prepare-reads",
        env,
        _BUCKET_TYPE="hns_rcu_cold",
        STUB_LIST="model.ckpt\tSTANDARD",
        STUB_FAIL="storage rsync",
    )

    assert proc.returncode != 0
    assert _failures(env) == ["rcu-prepare-reads"]
    assert _build_var(env, "SEEDED_CKPT_PATH") == SEED


# --- seed-checkpoint ------------------------------------------------------


@pytest.mark.parametrize(
    ("bucket_type", "force_http"),
    [
        ("standard_rcu_warm", True),
        ("hns_rcu_cold", True),
        ("standard_rcu_z", False),
        ("regional", False),
    ],
)
def test_seed_release_writes_over_http_only_for_warm_and_cold(
    env, bucket_type, force_http
):
    proc, calls = _run_step(
        "seed-checkpoint", env, _BUCKET_TYPE=bucket_type, STUB_LS=SEED
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    (install,) = _calls(calls, "helm install")
    assert ("gcsfs.forceHttp=true" in install) == force_http
