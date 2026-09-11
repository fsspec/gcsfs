"""Performance benchmark comparison tool for GCSFS microbenchmarks.

Compares benchmark results between two runs (e.g. base branch vs PR branch),
calculates percentage differences, and flags regressions exceeding a threshold.
"""

import argparse
import json
import logging
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple


def format_duration(seconds: Optional[float]) -> str:
    """Format duration in seconds into a human-readable string."""
    if seconds is None:
        return "N/A"
    if seconds < 1e-6:
        return f"{seconds * 1e9:.2f} ns"
    if seconds < 1e-3:
        return f"{seconds * 1e6:.2f} µs"
    if seconds < 1.0:
        return f"{seconds * 1e3:.3f} ms"
    return f"{seconds:.4f} s"


def format_throughput(mb_s: Optional[float]) -> str:
    """Format throughput in MB/s into a human-readable string."""
    return f"{mb_s:.2f} MB/s" if mb_s is not None else "N/A"


def format_metric_val(val: Optional[float], unit: str) -> str:
    """Format metric value with appropriate unit."""
    if val is None:
        return "N/A"
    if unit == "s":
        return format_duration(val)
    if unit == "MB/s":
        return format_throughput(val)
    return f"{val:.4f} {unit}"


def extract_benchmark_metric(
    bench: Optional[Dict[str, Any]],
) -> Tuple[str, float, str, bool]:
    """Extract primary performance metric. Returns (metric_name, value, unit, higher_is_better)."""
    if not bench:
        return "Mean Latency", 0.0, "s", False

    extra = bench.get("extra_info", {})
    runtime = extra.get("runtime")

    # Fixed duration benchmark (throughput)
    if runtime not in (None, "N/A"):
        try:
            r_val = float(runtime)
            mean_bytes = float(
                extra.get("mean_run", bench.get("stats", {}).get("mean", 0))
            )
            throughput = (mean_bytes / r_val) / (1024 * 1024) if r_val > 0 else 0.0
            return "Throughput", throughput, "MB/s", True
        except (ValueError, TypeError):
            pass

    # Multi-process duration metric
    if extra.get("mean_run") not in (None, "N/A"):
        try:
            return "Mean Latency", float(extra["mean_run"]), "s", False
        except (ValueError, TypeError):
            pass

    # Standard latency metric
    mean_val = float(bench.get("stats", {}).get("mean", 0.0))
    return "Mean Latency", mean_val, "s", False


def load_benchmarks(json_path: str) -> Dict[str, Dict[str, Any]]:
    """Load pytest-benchmark JSON into a dictionary keyed by benchmark name."""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {b["name"]: b for b in data.get("benchmarks", []) if "name" in b}


def compare_runs(
    base_benchmarks: Dict[str, Dict[str, Any]],
    pr_benchmarks: Dict[str, Dict[str, Any]],
    threshold_pct: float = 5.0,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Compare benchmark metrics between base and PR runs."""
    comparisons = []
    regressions = improvements = unchanged = new_count = removed_count = 0

    for key in sorted(set(base_benchmarks) | set(pr_benchmarks)):
        base = base_benchmarks.get(key)
        pr = pr_benchmarks.get(key)

        b_metric, b_val, b_unit, b_hib = (
            extract_benchmark_metric(base) if base else (None, None, "", False)
        )
        p_metric, p_val, p_unit, p_hib = (
            extract_benchmark_metric(pr) if pr else (None, None, "", False)
        )

        metric_name = p_metric or b_metric
        unit = p_unit or b_unit
        higher_is_better = p_hib if pr else b_hib
        diff_pct = None
        is_regression = False

        if base is None:
            status = "NEW"
            new_count += 1
        elif pr is None:
            status = "REMOVED"
            removed_count += 1
        else:
            diff_pct = (
                0.0 if b_val == 0 else round(((p_val - b_val) / abs(b_val)) * 100.0, 6)
            )
            is_regression = (
                diff_pct < -threshold_pct
                if higher_is_better
                else diff_pct > threshold_pct
            )
            is_improvement = (
                diff_pct > threshold_pct
                if higher_is_better
                else diff_pct < -threshold_pct
            )

            if is_regression:
                status = "REGRESSION"
                regressions += 1
            elif is_improvement:
                status = "IMPROVED"
                improvements += 1
            else:
                status = "NO_CHANGE"
                unchanged += 1

        comparisons.append(
            {
                "id": key,
                "name": (pr or base).get("param") or key,
                "group": (pr or base).get("group", ""),
                "metric_name": metric_name,
                "unit": unit,
                "higher_is_better": higher_is_better,
                "base_value": b_val,
                "pr_value": p_val,
                "diff_pct": diff_pct,
                "status": status,
                "is_regression": is_regression,
                "base_bench": base,
                "pr_bench": pr,
            }
        )

    summary = {
        "total": len(comparisons),
        "regressions": regressions,
        "improvements": improvements,
        "unchanged": unchanged,
        "new": new_count,
        "removed": removed_count,
        "has_regression": regressions > 0,
        "threshold_pct": threshold_pct,
    }
    return comparisons, summary


def generate_console_table(
    comparisons: List[Dict[str, Any]], summary: Dict[str, Any]
) -> str:
    """Format comparisons as a readable console table."""
    status_display = {
        "REGRESSION": "FAIL (Regression)",
        "IMPROVED": "PASS (Improved)",
        "NO_CHANGE": "PASS (No change)",
        "NEW": "NEW",
        "REMOVED": "REMOVED",
    }
    headers = ["Benchmark", "Metric", "Base", "PR", "Diff (%)", "Status"]
    rows = []
    for row in comparisons:
        base_str = format_metric_val(row["base_value"], row["unit"])
        pr_str = format_metric_val(row["pr_value"], row["unit"])
        diff_str = (
            f"{row['diff_pct']:+.2f}%" if row["diff_pct"] is not None else "N/A"
        )
        rows.append(
            [
                str(row["name"]),
                str(row["metric_name"]),
                base_str,
                pr_str,
                diff_str,
                status_display.get(row["status"], row["status"]),
            ]
        )

    all_rows = [headers] + rows
    col_widths = [max(len(r[i]) for r in all_rows) for i in range(len(headers))]
    lines = [
        "| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |",
        "| " + " | ".join("-" * w for w in col_widths) + " |",
    ]
    for r in rows:
        lines.append(
            "| " + " | ".join(cell.ljust(w) for cell, w in zip(r, col_widths)) + " |"
        )
    return "\n".join(lines)


def generate_markdown_report(
    comparisons: List[Dict[str, Any]],
    summary: Dict[str, Any],
    base_ref: Optional[str] = None,
    pr_ref: Optional[str] = None,
) -> str:
    """Generate a crisp Markdown report for PR comments and summaries."""
    threshold = summary["threshold_pct"]
    has_reg = summary["has_regression"]

    alert = (
        "> [!CAUTION]\n"
        f"> **Performance Regression Detected:** {summary['regressions']}"
        f" benchmark(s) exceeded the {threshold:.1f}% degradation threshold.\n"
        if has_reg
        else "> [!NOTE]\n"
        f"> **Performance Checks Passed:** Microbenchmarks are within the {threshold:.1f}% threshold.\n"
    )

    lines = [
        "<!-- gcsfs-perf-benchmark-report -->",
        "## Microbenchmark Performance Comparison\n",
        f"**Base:** `{base_ref or 'master'}` | **PR:** `{pr_ref or 'PR'}` | **Threshold:** `{threshold:.1f}%`\n",
        alert,
        "| Benchmark | Metric | Base | PR | Diff (%) | Status |",
        "| :--- | :--- | :--- | :--- | :--- | :--- |",
    ]

    status_labels = {
        "REGRESSION": f"FAIL (Regression >+{threshold:.1f}%)",
        "IMPROVED": "PASS (Improved)",
        "NO_CHANGE": "PASS (No change)",
        "NEW": "NEW",
        "REMOVED": "REMOVED",
    }

    for row in comparisons:
        base_str = format_metric_val(row["base_value"], row["unit"])
        pr_str = format_metric_val(row["pr_value"], row["unit"])
        diff_str = (
            f"**{row['diff_pct']:+.2f}%**" if row["diff_pct"] is not None else "N/A"
        )
        status_text = status_labels.get(row["status"], row["status"])
        lines.append(
            f"| `{row['name']}` | {row['metric_name']} | {base_str} | {pr_str} |"
            f" {diff_str} | {status_text} |"
        )

    return "\n".join(lines) + "\n"


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point for comparing two benchmark JSON files."""
    parser = argparse.ArgumentParser(description="Compare GCSFS microbenchmark runs.")
    parser.add_argument("base_json", help="Path to base branch benchmark results JSON")
    parser.add_argument("pr_json", help="Path to PR branch benchmark results JSON")
    parser.add_argument(
        "--threshold",
        type=float,
        default=5.0,
        help="Regression threshold %% (default: 5.0)",
    )
    parser.add_argument(
        "--base-ref", default="master", help="Base branch name or commit"
    )
    parser.add_argument("--pr-ref", default="PR", help="PR branch name or commit")
    parser.add_argument("--output-markdown", help="Path to save markdown report")
    parser.add_argument("--output-json", help="Path to save comparison JSON")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        base_benchmarks = load_benchmarks(args.base_json)
        pr_benchmarks = load_benchmarks(args.pr_json)
    except Exception as e:
        logging.error("Failed to load benchmark results: %s", e)
        return 1

    comparisons, summary = compare_runs(
        base_benchmarks, pr_benchmarks, threshold_pct=args.threshold
    )

    print("\n" + generate_console_table(comparisons, summary))
    print(
        f"\nSummary: {summary['total']} total | {summary['improvements']} improved | "
        f"{summary['unchanged']} unchanged | {summary['regressions']} regressions\n"
    )

    if args.output_markdown:
        p = Path(args.output_markdown)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            generate_markdown_report(comparisons, summary, args.base_ref, args.pr_ref),
            encoding="utf-8",
        )

    if args.output_json:
        p = Path(args.output_json)
        p.parent.mkdir(parents=True, exist_ok=True)
        clean_rows = [
            {k: v for k, v in c.items() if k not in ("base_bench", "pr_bench")}
            for c in comparisons
        ]
        p.write_text(
            json.dumps({"summary": summary, "comparisons": clean_rows}, indent=2),
            encoding="utf-8",
        )

    return 1 if summary["has_regression"] else 0


if __name__ == "__main__":
    sys.exit(main())

