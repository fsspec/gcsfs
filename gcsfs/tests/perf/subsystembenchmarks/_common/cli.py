import json
import logging
import os
import subprocess
import sys
import tempfile

from gcsfs.tests.perf.subsystembenchmarks._common import nodes, placement

# Per-case timeout (in seconds) overriding pyproject.toml's 1800s default.
CASE_TIMEOUT_SECONDS = 7200


def tests_globs(suite_dir):
    """Return anchored glob patterns for nested unit test directories to exclude from benchmark execution."""
    root = suite_dir.rstrip("/")
    return [f"{root}/tests/*", f"{root}/*/tests/*"]


def build_pytest_args(
    suite_dir, json_path, filter_expr=None, *, disable_benchmark=False
):
    """Construct the pytest-benchmark measurement invocation."""
    args = [
        suite_dir,
        "--run-benchmarks",
        "-vv",
        "-s",
        f"--timeout={CASE_TIMEOUT_SECONDS}",
    ]
    if disable_benchmark:
        args.append("--benchmark-disable")
    else:
        args.append(f"--benchmark-json={json_path}")
    if filter_expr:
        args += ["-k", filter_expr]
    args += [f"--ignore-glob={g}" for g in tests_globs(suite_dir)]
    return args


def build_collect_args(suite_dir: str, filter_expr: str | None = None) -> list[str]:
    """Construct the pytest --collect-only invocation for ``suite_dir``."""
    args = [
        suite_dir,
        "--run-benchmarks",
        "--collect-only",
        "-q",
        "-p",
        "no:cacheprovider",
    ]
    if filter_expr:
        args += ["-k", filter_expr]
    args += [f"--ignore-glob={g}" for g in tests_globs(suite_dir)]
    return args


def collect_cases(suite_dir: str, filter_expr: str | None = None) -> list[dict]:
    """Collect enabled, non-skipped cases ``[{"id": str, "nodes": int}]`` in ``suite_dir``."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_path = os.path.join(tmp_dir, "cases.json")
        cmd = [sys.executable, "-m", "pytest"] + build_collect_args(
            suite_dir, filter_expr=filter_expr
        )

        env = os.environ.copy()
        env[placement.COLLECTED_CASES_FILE_ENV] = out_path
        env.pop(placement.NODES_ENV, None)
        if not env.get("GCSFS_SUBSYSTEM_BUCKET_PREFIX"):
            env["GCSFS_SUBSYSTEM_BUCKET_PREFIX"] = "listing"

        proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=600)
        if proc.returncode != 0:
            raise RuntimeError(
                f"case collection failed (rc={proc.returncode}):\n"
                f"{proc.stdout}\n{proc.stderr}"
            )
        if not os.path.exists(out_path):
            return []
        with open(out_path) as f:
            return json.load(f)


def collect_node_counts(suite_dir: str, filter_expr: str | None = None) -> list[int]:
    """Collect distinct node counts required by enabled, non-skipped cases in ``suite_dir``."""
    return sorted(
        {
            int(entry["nodes"])
            for entry in collect_cases(suite_dir, filter_expr=filter_expr)
        }
    )


def _read_outcome(path: str) -> dict | None:
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def skipped_only_by_limitations(outcome: dict | None) -> bool:
    """True when every collected case was skipped by a known limitation."""
    return (
        outcome is not None
        and outcome.get("limitation_skipped", 0) > 0
        and not outcome.get("passed", 0)
        and not outcome.get("failed", 0)
        and not outcome.get("other_skipped", 0)
    )


def run_suite(suite_dir, results_dir, filter_expr=None):
    """Run the suite; return ``(rc, csv_path)``.

    A leader or single-node run that exits 0 without writing benchmark rows fails
    (rc 1) unless every case was skipped by a known limitation.
    """
    from gcsfs.tests.perf.subsystembenchmarks._common.report import generate_csv

    node_group = nodes.NodeGroup.from_env()
    is_follower = node_group is not None and not node_group.is_leader
    json_path = os.path.join(results_dir, "results.json")
    cmd = [sys.executable, "-m", "pytest"] + build_pytest_args(
        suite_dir, json_path, filter_expr, disable_benchmark=is_follower
    )
    # Kept out of results_dir, which launchers upload verbatim.
    with tempfile.TemporaryDirectory() as tmp_dir:
        outcome_path = os.path.join(tmp_dir, "outcome.json")
        env = os.environ.copy()
        env[placement.OUTCOME_FILE_ENV] = outcome_path
        rc = subprocess.run(cmd, env=env, text=True).returncode
        outcome = _read_outcome(outcome_path)
    csv_path = (
        generate_csv(json_path, results_dir)
        if not is_follower and os.path.exists(json_path)
        else None
    )
    if (
        rc == 0
        and csv_path is None
        and not is_follower
        and not skipped_only_by_limitations(outcome)
    ):
        logging.error("no benchmark rows written; pytest outcome: %s", outcome)
        rc = 1
    return rc, csv_path
