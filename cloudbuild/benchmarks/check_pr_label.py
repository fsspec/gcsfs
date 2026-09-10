#!/usr/bin/env python3
"""Checks if a pull request contains the required label to run performance benchmarks.

If the required label is missing, writes /workspace/SKIPPED so subsequent pipeline
steps can exit early without provisioning VMs or creating test buckets.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


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
        print("ℹ️ No _REQUIRED_LABEL specified. Proceeding with benchmarks.")
        return True

    if not pr_number or not pr_number.strip():
        print(
            "ℹ️ Not a pull request build (_PR_NUMBER is empty). Proceeding with"
            " benchmarks."
        )
        return True

    repo = repo.strip()
    pr_number = pr_number.strip()
    required_label = required_label.strip()

    target_labels = [t.strip().lower() for t in required_label.split(",") if t.strip()]

    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}"
    print(
        f"Checking for label(s) {target_labels} on {repo}#{pr_number} via"
        f" GitHub API ({url})..."
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
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as err:
        print(f"⚠️ GitHub API returned HTTP {err.code}: {err.reason} for {url}.")
        print("Proceeding with build to avoid blocking CI on external errors.")
        return True
    except Exception as exc:
        print(f"⚠️ Warning: Failed to query GitHub API for PR labels: {exc}")
        print("Proceeding with build to avoid blocking CI on external errors.")
        return True

    pr_labels = [str(l.get("name", "")).strip().lower() for l in data.get("labels", [])]
    print(f"PR #{pr_number} current labels: {pr_labels if pr_labels else 'None'}")

    matched = any(t in pr_labels for t in target_labels)
    if matched:
        print(
            f"✅ Required label (one of {target_labels}) found on PR"
            f" #{pr_number}. Proceeding with benchmarks."
        )
        return True
    else:
        print(
            f"⏭️ Required label (one of {target_labels}) NOT found on PR"
            f" #{pr_number}."
        )
        print("Marking build as skipped to conserve compute resources.")
        try:
            os.makedirs(os.path.dirname(skip_file), exist_ok=True)
            with open(skip_file, "w") as f:
                f.write(
                    f"PR #{pr_number} does not have required label"
                    f" '{required_label}'.\n"
                )
        except Exception as e:
            print(f"Warning: Failed to write {skip_file}: {e}")
        return False


def main():
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
