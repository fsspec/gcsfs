#!/usr/bin/env python3
import datetime
import os
import re
import subprocess
import sys


def run_cmd(cmd_args):
    """Runs a terminal command without shell=True to avoid injection risks."""
    result = subprocess.run(cmd_args, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def get_latest_tag():
    """Gets the latest git tag reachable from HEAD."""
    return run_cmd(["git", "describe", "--tags", "--abbrev=0"])


def parse_version(version_str):
    """Parses a version string like YYYY.M.PATCH[-suffix] into a tuple of integers."""
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", version_str)
    if not match:
        raise ValueError(
            f"Version '{version_str}' does not match expected CalVer pattern YYYY.M.PATCH"
        )
    return tuple(map(int, match.groups()))


def calculate_next_version(latest_tag):
    """Calculates the next CalVer version based on the latest tag and current date."""
    latest_ver = parse_version(latest_tag)
    tag_year, tag_month, tag_patch = latest_ver

    now = datetime.datetime.now(datetime.timezone.utc)
    current_year = now.year
    current_month = now.month

    if tag_year == current_year and tag_month == current_month:
        # Same month, increment patch
        next_patch = tag_patch + 1
    else:
        # New month, reset patch to 0
        next_patch = 0

    next_version_str = f"{current_year}.{current_month}.{next_patch}"
    next_ver = parse_version(next_version_str)

    # Safety guard: Ensure we never release a version older or equal to the last one
    if next_ver <= latest_ver:
        raise ValueError(
            f"Calculated next version ({next_version_str}) is not newer than "
            f"the latest tag ({latest_tag}). Potential version regression!"
        )

    return next_version_str


def get_changelog_entries(latest_tag):
    """Retrieves all non-merge commit subjects since the latest tag."""
    cmd_args = [
        "git",
        "log",
        f"{latest_tag}..HEAD",
        "--no-merges",
        "--pretty=format:* %s",
    ]
    log_output = run_cmd(cmd_args)
    if not log_output:
        return ["* No changes (released in sync with fsspec)."]
    return log_output.split("\n")


def update_changelog_file(changelog_path, version, entries):
    """Inserts a new release section with version and commit logs into the changelog.rst file."""
    if not os.path.exists(changelog_path):
        raise FileNotFoundError(f"Changelog file not found at {changelog_path}")

    with open(changelog_path, "r", encoding="utf-8") as f:
        content = f.read()

    lines = content.split("\n")
    insert_idx = -1
    # Regex to match version header (e.g., "2026.4.0" or "2025.5.0post1")
    version_re = re.compile(r"^\d{4}\.\d+\.\d+\S*$")

    for i in range(len(lines) - 1):
        if (
            version_re.match(lines[i])
            and lines[i + 1].startswith("---")
            and len(lines[i + 1]) >= len(lines[i])
        ):
            insert_idx = i
            break

    if insert_idx == -1:
        # If we couldn't find a version header, we might be in an empty or differently formatted file.
        # In this case, we raise an error.
        raise ValueError(
            "Could not find a valid version header in changelog to insert before."
        )

    # Prepare the new section
    version_underline = "-" * len(version)
    new_section_lines = (
        [
            version,
            version_underline,
            "",
        ]
        + entries
        + [""]
    )

    # Insert the new section. We want to keep an empty line between sections.
    # The first version header we found should be pushed down.
    # We insert before the version line.
    updated_lines = lines[:insert_idx] + new_section_lines + lines[insert_idx:]

    with open(changelog_path, "w", encoding="utf-8") as f:
        f.write("\n".join(updated_lines))

    print(f"Successfully updated changelog with version {version}")


def main():
    changelog_path = "docs/source/changelog.rst"

    try:
        # 1. Retrieve the latest release tag from Git
        latest_tag = get_latest_tag()
        print(f"Latest tag found: {latest_tag}")

        # 2. Calculate the next CalVer version and perform regression checks
        next_version = calculate_next_version(latest_tag)
        print(f"Calculated next version: {next_version}")

        # 3. Fetch the changelog entries (non-merge commits) since the last tag
        entries = get_changelog_entries(latest_tag)
        print(f"Found {len(entries)} changelog entries.")

        # 4. Update the changelog file in place with the new release section
        update_changelog_file(changelog_path, next_version, entries)

        # 5. Output the version to GITHUB_ENV for downstream workflow consumption
        print(f"NEXT_VERSION={next_version}")
        if "GITHUB_ENV" in os.environ:
            with open(os.environ["GITHUB_ENV"], "a") as gh_env:
                gh_env.write(f"VERSION={next_version}\n")
                gh_env.write(f"BRANCH_NAME=release-{next_version}\n")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
