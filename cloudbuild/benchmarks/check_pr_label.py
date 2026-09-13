#!/usr/bin/env python3
"""Checks if a pull request contains the required label to run performance benchmarks.

Also reads optional `perf-*` directives from the PR description so an author can
widen or narrow the benchmark selection for a single PR. Cloud Build's GitHub app
matches `/gcbrun` exactly and cannot carry arguments, so the PR body is the only
per-PR channel available without extra infrastructure.
"""

import argparse
import json
import logging
import re
import sys
import time
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

# The GitHub API is queried anonymously, so transient rate limiting is expected.
_MAX_ATTEMPTS = 3
_RETRY_BACKOFF_SECONDS = 5

_DIRECTIVE_RE = re.compile(
    r"^[ \t>*-]*perf-(groups|scenarios|io-sizes)[ \t]*:[ \t]*(.+?)[ \t]*$",
    re.IGNORECASE | re.MULTILINE,
)
_IDENTIFIER_RE = re.compile(r"^[a-z0-9_]+$")
_NUMBER_RE = re.compile(r"^[0-9]+(\.[0-9]+)?$")
_SCENARIO_NAME_RE = re.compile(
    r"^[ \t]*-[ \t]*name:[ \t]*[\"\']?([A-Za-z0-9_]+)", re.MULTILINE
)

_IDENTIFIER_DESC = "a bare identifier (lowercase letters, digits and underscores only)"
# Token rule per directive: (allowed pattern, description used when rejecting).
_TOKEN_RULES = {
    "groups": (_IDENTIFIER_RE, _IDENTIFIER_DESC),
    "scenarios": (_IDENTIFIER_RE, _IDENTIFIER_DESC),
    "io-sizes": (_NUMBER_RE, "a number (sizes are in MB, so write 1 rather than 1MB)"),
}

_BENCHMARK_ROOT = (
    Path(__file__).resolve().parents[2] / "gcsfs" / "tests" / "perf" / "microbenchmarks"
)


def _write_skip_file(skip_file: str, reason: str) -> None:
    skip_path = Path(skip_file)
    skip_path.parent.mkdir(parents=True, exist_ok=True)
    skip_path.write_text(f"{reason}\n", encoding="utf-8")


def _catalog() -> dict:
    """Map each benchmark group to the scenario names its configs.yaml declares.

    Parsed with a regex rather than PyYAML, because this runs in the Cloud Build
    cloud-sdk image whose system Python has no third-party packages installed. An
    empty result means the benchmark tree is unreadable, which callers treat as
    "cannot check names" rather than "every name is wrong".
    """
    catalog: dict = {}
    if not _BENCHMARK_ROOT.is_dir():
        return catalog
    for path in _BENCHMARK_ROOT.iterdir():
        if not path.is_dir() or path.name.startswith(("_", ".")):
            continue
        try:
            text = (path / "configs.yaml").read_text(encoding="utf-8")
        except OSError:
            text = ""
        # Only the scenarios block declares names; skip the preceding common block.
        _, _, scenarios = text.partition("\nscenarios:")
        catalog[path.name] = set(_SCENARIO_NAME_RE.findall(scenarios))
    return catalog


def _validate_scenarios(tokens: list, group_names: list, catalog: dict) -> None:
    """Reject scenario names that none of the selected groups declares.

    The runner matches scenario names exactly (microbenchmarks/configs.py), so a typo
    selects nothing rather than erroring: the build would provision a large VM, run
    zero cases and report an empty comparison. Catching it here happens before any
    infrastructure exists.

    Raises:
        ValueError: if a scenario is unknown or belongs to an unselected group.
    """
    scope = sorted(set(group_names) or catalog)
    available = {name for group in scope for name in catalog.get(group, ())}

    for token in tokens:
        if token in available:
            continue
        owners = " ".join(sorted(g for g, names in catalog.items() if token in names))
        if owners:
            raise ValueError(
                f"scenario {token!r} is not declared by group(s) {', '.join(scope)}; "
                f"it belongs to {owners}. Add 'perf-groups: {owners}' to run it."
            )
        raise ValueError(
            f"unknown scenario {token!r}. Scenarios in {', '.join(scope)}: "
            f"{', '.join(sorted(available))}"
        )


def format_catalog() -> str:
    """Render every group with the scenarios it declares, for `--list`.

    PR authors have no other way to discover valid `perf-scenarios` values: the
    rejection message only names scenarios in the selected groups, and the Cloud Build
    log it appears in is not visible to contributors outside the project.
    """
    lines = ["Benchmark groups and the scenarios they declare:", ""]
    for group, scenarios in sorted(_catalog().items()):
        lines.append(f"  {group}")
        lines.extend(f"    {name}" for name in sorted(scenarios))
        lines.append("")
    lines.append(
        "Use these as 'perf-groups: <group> ...' and 'perf-scenarios: <scenario> ...' "
        "in the PR description."
    )
    return "\n".join(lines)


def _strip_code_blocks(body: str) -> str:
    """Blank out fenced code blocks so an illustrative example is not acted on.

    Authors quote the directive syntax when explaining it, and a fenced block is the
    natural way to do that. Matching inside one would silently change what their own
    PR benchmarks. An unterminated fence blanks the remainder, matching how GitHub
    renders it.
    """
    lines, in_fence = [], False
    for line in body.splitlines():
        if line.lstrip().startswith(("```", "~~~")):
            in_fence = not in_fence
            lines.append("")
        else:
            lines.append("" if in_fence else line)
    return "\n".join(lines)


def parse_selection(body: str, default_groups: str = "") -> dict:
    """Parse `perf-*` directives from a PR description into environment assignments.

    Recognised anywhere in the body, case-insensitively::

        perf-groups:    read write
        perf-scenarios: read_seq_fixed_duration
        perf-io-sizes:  1, 16

    A directive must begin its own line, optionally behind indentation or a list or
    quote marker, and may appear at most once. Values are separated by commas or
    spaces. Text inside fenced code blocks is ignored.

    Every token is checked against a strict allowlist. These values are interpolated
    into a quoted `gcloud compute ssh --command` targeting a VM with cloud-platform
    scope, so a token containing a quote or semicolon would be command injection from
    an attacker-controlled field. Anything that is not a bare identifier or a number
    is rejected outright rather than escaped. Group and scenario names are then checked
    against what the repository declares, so a typo fails the build with the valid
    names listed instead of quietly running an empty set of benchmarks.

    ``default_groups`` is what the build runs when the description names no group;
    scenarios are validated against those.

    Raises:
        ValueError: if a directive is empty or contains an invalid or unknown token.
    """
    if not body:
        return {}

    # Collect every directive before validating, so `perf-scenarios` can be checked
    # against `perf-groups` regardless of the order they appear in the body.
    directives: dict = {}
    for key, raw in _DIRECTIVE_RE.findall(_strip_code_blocks(body)):
        key = key.lower()
        if key in directives:
            raise ValueError(
                f"perf-{key} is given more than once; keep a single line per directive"
            )
        tokens = [t for t in re.split(r"[,\s]+", raw.strip()) if t]
        if not tokens:
            raise ValueError(f"perf-{key} directive is empty")
        pattern, expected = _TOKEN_RULES[key]
        for token in tokens:
            if not pattern.match(token):
                raise ValueError(f"perf-{key} value {token!r} is not {expected}")
        directives[key] = tokens

    catalog = _catalog()
    if catalog and "groups" in directives:
        unknown = sorted(set(directives["groups"]) - set(catalog))
        if unknown:
            raise ValueError(
                f"unknown benchmark group(s): {', '.join(unknown)}. "
                f"Available: {', '.join(sorted(catalog))}"
            )
    if catalog and "scenarios" in directives:
        _validate_scenarios(
            directives["scenarios"],
            directives.get("groups") or default_groups.split(),
            catalog,
        )

    selection = {}
    if "io-sizes" in directives:
        selection["CHUNK_SIZES_MB"] = ",".join(directives["io-sizes"])
    if "groups" in directives:
        selection["BENCHMARK_GROUPS"] = " ".join(directives["groups"])
    if "scenarios" in directives:
        selection["BENCHMARK_CONFIG"] = ",".join(directives["scenarios"])
    elif "groups" in directives:
        # The default scenario filter names read-specific scenarios. Choosing different
        # groups without naming scenarios must clear it, otherwise the filter matches
        # nothing in the new groups and the run collects zero cases.
        selection["BENCHMARK_CONFIG"] = ""

    return selection


def write_selection(selection: dict, path: str) -> None:
    """Write the selection as shell assignments for the build step to source."""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        "".join(f"{k}='{v}'\n" for k, v in sorted(selection.items())),
        encoding="utf-8",
    )


def _fetch_pr(url: str) -> dict:
    """Fetch the PR, retrying transient failures. Raises on final failure."""
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "CloudBuild-PR-Benchmark",
            "Accept": "application/vnd.github.v3+json",
        },
    )
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            if attempt == _MAX_ATTEMPTS:
                raise
            logger.warning(
                "GitHub API attempt %d/%d failed (%s): %s. Retrying in %ds.",
                attempt,
                _MAX_ATTEMPTS,
                url,
                exc,
                _RETRY_BACKOFF_SECONDS,
            )
            time.sleep(_RETRY_BACKOFF_SECONDS)


def check_pr_label(
    repo: str,
    pr_number: str,
    required_label: str,
    skip_file: str = "/workspace/SKIPPED",
    selection_file: str = "",
    default_groups: str = "",
) -> bool:
    """Check if the given PR has the required label. Returns True if build should proceed.

    When the label matches and ``selection_file`` is set, any ``perf-*`` directives in
    the PR description are validated and written there for later build steps to source.
    ``default_groups`` is the build's own group selection, used to validate scenario
    names when the description names scenarios but no groups.

    Raises:
        ValueError: if the PR description contains a malformed ``perf-*`` directive.
    """
    if not required_label or not required_label.strip():
        logger.info("No _REQUIRED_LABEL specified. Proceeding with build.")
        return True

    if not pr_number or not pr_number.strip():
        logger.info("No _PR_NUMBER specified (non-PR build). Proceeding with build.")
        return True

    repo, pr_number = repo.strip(), pr_number.strip()
    target_labels = {t.strip().lower() for t in required_label.split(",") if t.strip()}
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}"

    try:
        pr_data = _fetch_pr(url)
    except Exception as exc:
        # Fail closed. Proceeding would provision a large VM and several buckets for a
        # PR we cannot confirm was opted in, so an unverifiable label is treated as absent.
        logger.error(
            "Failed to query GitHub API (%s): %s. Skipping benchmarks.", url, exc
        )
        _write_skip_file(
            skip_file, f"Could not verify labels on PR #{pr_number}: {exc}"
        )
        return False

    pr_labels = {
        str(l.get("name", "")).strip().lower() for l in pr_data.get("labels", [])
    }
    logger.info(
        "PR #%s labels: %s", pr_number, sorted(pr_labels) if pr_labels else "None"
    )

    if target_labels & pr_labels:
        logger.info(
            "Matched required label (%s). Proceeding with benchmarks.", target_labels
        )
        if selection_file:
            # Raises ValueError on a malformed directive; main() turns that into a
            # non-zero exit so the author sees the problem instead of silently
            # getting the default selection.
            selection = parse_selection(pr_data.get("body") or "", default_groups)
            if selection:
                logger.info("Benchmark selection from PR description: %s", selection)
                write_selection(selection, selection_file)
            else:
                logger.info(
                    "No perf-* directives in the PR description; "
                    "the pipeline defaults apply."
                )
        return True

    logger.info("Required label (%s) not found. Skipping benchmarks.", target_labels)
    _write_skip_file(
        skip_file, f"PR #{pr_number} does not have required label '{required_label}'."
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
    parser.add_argument(
        "--selection-file",
        default="/workspace/benchmark_selection.env",
        help="Where to write benchmark selection parsed from the PR description",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print every benchmark group and scenario name, then exit",
    )
    parser.add_argument(
        "--default-groups",
        default="",
        help=(
            "Groups the build runs by default, used to validate scenario names when "
            "the PR description names scenarios but no groups"
        ),
    )
    args = parser.parse_args()

    if args.list:
        print(format_catalog())
        sys.exit(0)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        check_pr_label(
            args.repo,
            args.pr,
            args.label,
            args.skip_file,
            args.selection_file,
            args.default_groups,
        )
    except ValueError as exc:
        # Reject loudly. A typo here would otherwise fall back to the default
        # selection, and failing now costs nothing because no VM or bucket exists yet.
        logger.error("Invalid perf directive in PR description: %s", exc)
        sys.exit(1)

    # Otherwise always exit 0: a skipped build is not a failed build. Downstream steps
    # read the skip file to decide whether to run, and the final step reports the outcome.
    sys.exit(0)


if __name__ == "__main__":
    main()
