import os
import subprocess
import sys


def tests_globs(suite_dir):
    """Every `tests/` dir inside the suite, at any depth.

    A suite keeps its unit tests next to the code they cover (`<group>/tests/`,
    `<group>/read/tests/`, ...). None of them are benchmarks, and a failure in one would be
    read by CI as a performance regression -- but naming the directories one by one misses the
    nested ones. These must stay ANCHORED to suite_dir: the whole benchmark tree already lives
    under `gcsfs/tests/`, so a bare `*/tests/*` matches the benchmarks themselves and collects
    nothing at all. pytest fnmatches these against the full path, and `*` spans `/`, so the
    second glob covers any nesting depth.
    """
    root = suite_dir.rstrip("/")
    return [f"{root}/tests/*", f"{root}/*/tests/*"]


def build_pytest_args(suite_dir, json_path):
    """Construct the pytest-benchmark measurement invocation."""
    args = [
        suite_dir,
        f"--benchmark-json={json_path}",
    ]
    args += [f"--ignore-glob={g}" for g in tests_globs(suite_dir)]
    return args


def run_suite(suite_dir, results_dir):
    from gcsfs.tests.perf.subsystembenchmarks._common.report import generate_csv

    json_path = os.path.join(results_dir, "results.json")
    cmd = [sys.executable, "-m", "pytest"] + build_pytest_args(suite_dir, json_path)
    rc = subprocess.run(cmd, env=os.environ.copy(), text=True).returncode
    csv_path = (
        generate_csv(json_path, results_dir) if os.path.exists(json_path) else None
    )
    return rc, csv_path
