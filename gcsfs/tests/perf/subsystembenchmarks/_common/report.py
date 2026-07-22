import csv
import json
import logging
import os

import numpy as np

# CSV/BQ column name per pytest-benchmark stat. A round is the timed unit: one
# full-corpus iteration for the read benchmarks.
STATS_HEADERS = {
    "min": "round_duration_min_seconds",
    "max": "round_duration_max_seconds",
    "mean": "round_duration_mean_seconds",
    "median": "round_duration_p50_seconds",
    "stddev": "round_duration_stddev_seconds",
}
PERCENTILE_HEADERS = {
    90: "round_duration_p90_seconds",
    95: "round_duration_p95_seconds",
    99: "round_duration_p99_seconds",
}


def _process_benchmark_result(bench, extra_info_headers):
    row = {}
    row["benchmark_case_id"] = bench["name"]
    row["benchmark_group"] = bench.get("group", "")
    extra_info = bench.get("extra_info", {})
    for key in extra_info_headers:
        row[key] = extra_info.get(key)
    for stat, header in STATS_HEADERS.items():
        row[header] = bench["stats"].get(stat)
    rounds_data = bench["stats"].get("data")
    if rounds_data:
        for pct, header in PERCENTILE_HEADERS.items():
            row[header] = np.percentile(rounds_data, pct)
    return row


def generate_csv(json_path: str, results_dir: str):
    """Convert a pytest-benchmark JSON file into a CSV with dynamic extra_info columns.

    Returns None when the file holds no benchmark results -- including the empty file
    pytest-benchmark 5.x leaves behind for --benchmark-json when every case was skipped.
    That None is the signal run.py's no-results guard consumes; crashing here would bury
    the real problem (a run that produced no data) under a JSONDecodeError.
    """
    with open(json_path, "r") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            data = None
    if not data or not data.get("benchmarks"):
        logging.error("No benchmarks found in %s", json_path)
        return None

    report_path = os.path.join(results_dir, "results.csv")
    # Union across ALL benchmarks, not just the first: loaders (and, later, the checkpoint
    # family) publish different extra_info keys, and taking the first row's keys would
    # silently drop every column the first case happens not to emit.
    extra_info_headers = sorted(
        {key for bench in data["benchmarks"] for key in bench.get("extra_info", {})}
    )
    headers = (
        ["benchmark_case_id", "benchmark_group"]
        + extra_info_headers
        + list(STATS_HEADERS.values())
        + list(PERCENTILE_HEADERS.values())
    )

    with open(report_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for bench in data["benchmarks"]:
            row = _process_benchmark_result(bench, extra_info_headers)
            writer.writerow([row.get(h, "") for h in headers])
    logging.info("CSV report generated at %s", report_path)
    return report_path


def print_csv_to_shell(report_path: str):
    """Print every column of a generated CSV report as a Markdown table."""
    try:
        from prettytable import PrettyTable, TableStyle
    except ImportError:
        logging.warning("prettytable is unavailable; skipping markdown table output")
        return

    try:
        with open(report_path, newline="") as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            headers = reader.fieldnames or []
    except (OSError, csv.Error, UnicodeError) as exc:
        logging.error("Failed to read or parse report at %s: %s", report_path, exc)
        return

    if not rows:
        logging.info("No data to display.")
        return
    if len(headers) != len(set(headers)):
        logging.error("Failed to render report at %s: duplicate CSV headers", report_path)
        return

    table = PrettyTable()
    table.set_style(TableStyle.MARKDOWN)
    table.field_names = headers
    for row in rows:
        table.add_row([row.get(header) or "" for header in headers])
    print(table)
