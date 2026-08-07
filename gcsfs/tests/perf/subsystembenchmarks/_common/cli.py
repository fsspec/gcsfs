import os
import subprocess
import sys


def tests_globs(suite_dir):
    """Return anchored glob patterns for nested unit test directories to exclude from benchmark execution."""
    root = suite_dir.rstrip("/")
    return [f"{root}/tests/*", f"{root}/*/tests/*"]


def build_pytest_args(suite_dir, json_path, filter_expr=None):
    """Construct the pytest-benchmark measurement invocation."""
    args = [
        suite_dir,
        "--run-benchmarks",
        "-vv",
        "-s",
        f"--benchmark-json={json_path}",
    ]
    if filter_expr:
        args += ["-k", filter_expr]
    args += [f"--ignore-glob={g}" for g in tests_globs(suite_dir)]
    return args


def run_suite(suite_dir, results_dir, filter_expr=None):
    from gcsfs.tests.perf.subsystembenchmarks._common.report import generate_csv

    json_path = os.path.join(results_dir, "results.json")
    cmd = [sys.executable, "-m", "pytest"] + build_pytest_args(
        suite_dir, json_path, filter_expr
    )
    rc = subprocess.run(cmd, env=os.environ.copy(), text=True).returncode
    csv_path = (
        generate_csv(json_path, results_dir) if os.path.exists(json_path) else None
    )
    return rc, csv_path
