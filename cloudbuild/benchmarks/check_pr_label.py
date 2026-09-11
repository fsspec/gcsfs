#!/usr/bin/env python3
"""Checks if a pull request contains the required label to run performance benchmarks."""

import argparse
import json
import logging
from pathlib import Path
import sys
import urllib.request

logger = logging.getLogger(__name__)


def check_pr_label(
    repo: str,
    pr_number: str,
    required_label: str,
    skip_file: str = "/workspace/SKIPPED",
) -> bool:
    """Check if the given PR has the required label. Returns True if build should proceed."""
    if not required_label or not required_label.strip():
        logger.info("No _REQUIRED_LABEL specified. Proceeding with build.")
        return True

    if not pr_number or not pr_number.strip():
        logger.info("No _PR_NUMBER specified (non-PR build). Proceeding with build.")
        return True

    repo, pr_number = repo.strip(), pr_number.strip()
    target_labels = {t.strip().lower() for t in required_label.split(",") if t.strip()}
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}"

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "CloudBuild-PR-Benchmark",
            "Accept": "application/vnd.github.v3+json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        pr_labels = {
            str(l.get("name", "")).strip().lower() for l in data.get("labels", [])
        }
    except Exception as exc:
        logger.warning(
            "Failed to query GitHub API (%s): %s. Proceeding with build.", url, exc
        )
        return True

    logger.info(
        "PR #%s labels: %s", pr_number, sorted(pr_labels) if pr_labels else "None"
    )

    if target_labels & pr_labels:
        logger.info(
            "Matched required label (%s). Proceeding with benchmarks.", target_labels
        )
        return True

    logger.info("Required label (%s) not found. Skipping benchmarks.", target_labels)
    skip_path = Path(skip_file)
    skip_path.parent.mkdir(parents=True, exist_ok=True)
    skip_path.write_text(
        f"PR #{pr_number} does not have required label '{required_label}'.\n",
        encoding="utf-8",
    )
    return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check PR labels for performance benchmarks."
    )
    parser.add_argument(
        "--repo", default="fsspec/gcsfs", help="Repository (owner/repo)"
    )
    parser.add_argument("--pr", default="", help="PR number")
    parser.add_argument(
        "--label", default="execute-perf-test", help="Required label(s)"
    )
    parser.add_argument(
        "--skip-file", default="/workspace/SKIPPED", help="Skip file path"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    check_pr_label(args.repo, args.pr, args.label, args.skip_file)
    sys.exit(0)


if __name__ == "__main__":
    main()

