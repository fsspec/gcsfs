#!/usr/bin/env python3
"""Checks if a pull request contains the required label to run performance benchmarks.

If the required label is missing, writes /workspace/SKIPPED so subsequent pipeline
steps can exit early without provisioning VMs or creating test buckets.
"""

import argparse
import json
import logging
import os
import sys
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)


def check_pr_label(
    repo: str,
    pr_number: str,
    required_label: str,
    skip_file: str = "/workspace/SKIPPED",
) -> bool:
    """Check if the given PR in the repo has the required label.

    Returns True if benchmarks should proceed, False if they should be skipped.
    """
    if not required_label or not required_label.strip():
        logger.info("No _REQUIRED_LABEL specified. Proceeding with benchmarks.")
        return True

    if not pr_number or not pr_number.strip():
        logger.info(
            "Not a pull request build (_PR_NUMBER is empty). Proceeding with"
            " benchmarks."
        )
        return True

    repo = repo.strip()
    pr_number = pr_number.strip()
    required_label = required_label.strip()

    target_labels = [t.strip().lower() for t in required_label.split(",") if t.strip()]

    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}"
    logger.info(
        "Checking for label(s) %s on %s#%s via GitHub API (%s)...",
        target_labels,
        repo,
        pr_number,
        url,
    )

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
    except urllib.error.HTTPError as err:
        logger.warning(
            "GitHub API returned HTTP %s: %s for %s.", err.code, err.reason, url
        )
        logger.info("Proceeding with build to avoid blocking CI on external errors.")
        return True
    except Exception as exc:
        logger.warning("Failed to query GitHub API for PR labels: %s", exc)
        logger.info("Proceeding with build to avoid blocking CI on external errors.")
        return True

    pr_labels = [str(l.get("name", "")).strip().lower() for l in data.get("labels", [])]
    logger.info(
        "PR #%s current labels: %s", pr_number, pr_labels if pr_labels else "None"
    )

    matched = any(t in pr_labels for t in target_labels)
    if matched:
        logger.info(
            "Required label (one of %s) found on PR #%s. Proceeding with benchmarks.",
            target_labels,
            pr_number,
        )
        return True

    logger.info(
        "Required label (one of %s) not found on PR #%s.",
        target_labels,
        pr_number,
    )
    logger.info("Skipping benchmarks to conserve compute resources.")
    try:
        os.makedirs(os.path.dirname(skip_file), exist_ok=True)
        with open(skip_file, "w", encoding="utf-8") as f:
            f.write(
                f"PR #{pr_number} does not have required label '{required_label}'.\n"
            )
    except Exception as exc:
        logger.warning("Failed to write %s: %s", skip_file, exc)
    return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check PR labels for performance benchmarks."
    )
    parser.add_argument(
        "--repo",
        default=os.environ.get("REPO_FULL_NAME", "fsspec/gcsfs"),
        help="Repository full name (owner/repo)",
    )
    parser.add_argument(
        "--pr", default=os.environ.get("_PR_NUMBER", ""), help="PR number"
    )
    parser.add_argument(
        "--label",
        default=os.environ.get("_REQUIRED_LABEL", "perf-test"),
        help="Required label(s), comma-separated",
    )
    parser.add_argument(
        "--skip-file",
        default="/workspace/SKIPPED",
        help="File to write if build should be skipped",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    check_pr_label(
        repo=args.repo,
        pr_number=args.pr,
        required_label=args.label,
        skip_file=args.skip_file,
    )
    # Always exit 0 so Cloud Build doesn't fail on label check;
    # downstream steps check for /workspace/SKIPPED.
    sys.exit(0)


if __name__ == "__main__":
    main()
