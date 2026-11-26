import json
import multiprocessing
import os

import pytest

from gcsfs.tests.perf._common.resource_monitor import ResourceMonitor
from gcsfs.tests.perf.subsystembenchmarks._common import placement
from gcsfs.tests.perf.subsystembenchmarks._common.benchmark_publish import (  # noqa: F401
    publish_resource_metrics,
    publish_round_stats,
    pytest_benchmark_generate_json,
    pytest_benchmark_update_machine_info,
)

# ResourceMonitor runs a sampler thread before DataLoader workers start. Force the safe
# start method before any benchmark creates workers; forking the threaded pytest process can
# inherit locked interpreter/library state and deadlock.
multiprocessing.set_start_method("spawn", force=True)


CASES_ENV = placement.CASES_ENV

# Per-session test outcomes, written to $GCSFS_SUBSYSTEM_OUTCOME_FILE so the runner can
# tell "every case skipped by a known limitation" apart from "nothing ran".
_OUTCOME = {"passed": 0, "failed": 0, "limitation_skipped": 0, "other_skipped": 0}


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "limitation: skipped because of a known framework limitation "
        "(_common/limitations.py)",
    )


def pytest_runtest_logreport(report):
    if report.passed and report.when == "call":
        _OUTCOME["passed"] += 1
    elif report.skipped:
        key = (
            "limitation_skipped" if "limitation" in report.keywords else "other_skipped"
        )
        _OUTCOME[key] += 1
    elif report.failed:
        _OUTCOME["failed"] += 1


def pytest_sessionfinish(session, exitstatus):
    out_path = os.environ.get(placement.OUTCOME_FILE_ENV)
    if out_path:
        with open(out_path, "w") as f:
            json.dump(_OUTCOME, f)


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config, items):
    """Drop synthetic empty-parametrize items and enforce exact --cases selection."""
    if not config.getoption("--run-benchmarks", default=False):
        return
    kept = []
    for item in items:
        skip = item.get_closest_marker("skip")
        reason = (skip.kwargs.get("reason", "") if skip is not None else "") or ""
        if reason.startswith("got empty parameter set"):
            continue
        kept.append(item)
    items[:] = kept

    raw_cases = os.environ.get(CASES_ENV, "").strip()
    if raw_cases:
        wanted = set(raw_cases.split())
        matched = []
        found = set()
        for item in items:
            callspec = getattr(item, "callspec", None)
            params = callspec.params.get("params") if callspec is not None else None
            name = getattr(params, "name", None)
            if name in wanted:
                matched.append(item)
                found.add(name)
        missing = sorted(wanted - found)
        if missing:
            raise pytest.UsageError(f"unknown case id(s): {', '.join(missing)}")
        items[:] = matched


def pytest_collection_finish(session):
    out_path = os.environ.get(placement.COLLECTED_CASES_FILE_ENV)
    if not out_path:
        return
    per_node = None
    cases_by_id = {}
    for item in session.items:
        if item.get_closest_marker("skip") is not None:
            continue
        callspec = getattr(item, "callspec", None)
        if callspec is None or "params" not in callspec.params:
            continue
        params = callspec.params["params"]
        if hasattr(params, "max_world_size") and hasattr(params, "name"):
            if per_node is None:
                per_node = placement.devices_per_node()
            nodes = placement.case_nodes(params, per_node)
            if nodes is None:
                continue
            cases_by_id[params.name] = {"id": params.name, "nodes": nodes}
    entries = [cases_by_id[k] for k in sorted(cases_by_id)]
    with open(out_path, "w") as f:
        json.dump(entries, f)


@pytest.fixture
def monitor():
    return ResourceMonitor
