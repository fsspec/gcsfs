"""Print a size-capped summary of the e2e test suites' JUnit XML results.

GitHub shows only the first ~65k chars of a Cloud Build log, so the test-report step in
e2e-tests-cloudbuild.yaml prints this summary before the full pytest logs. Sections are
added in priority order (failed steps, per-suite counts, failure details, one-line
failure list) and the output never exceeds --max-chars.
"""

import argparse
import os
import xml.etree.ElementTree as ET
from collections import deque

TRACEBACK_LINES = 40
LOG_TAIL_LINES = 60
MESSAGE_CHARS = 500
DETAIL_CHARS = 4000
ONE_LINE_CHARS = 300
# Room for the "not shown in detail" header and the "... and N more" note.
NOTE_RESERVE = 150
FOOTER = "===== END OF E2E TEST SUMMARY (full logs follow) ====="


def _tail(text, lines, chars=DETAIL_CHARS):
    tail = "\n".join((text or "").rstrip().splitlines()[-lines:])
    return tail[-chars:]


def parse_junit(path):
    """Return ({outcome: count}, seconds, [(outcome, nodeid, message, traceback)])."""
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else root.findall("testsuite")
    counts = {"passed": 0, "failed": 0, "error": 0, "skipped": 0}
    seconds = 0.0
    problems = []
    for suite in suites:
        seconds += float(suite.get("time") or 0)
        for case in suite.iter("testcase"):
            nodeid = "::".join(
                part for part in (case.get("classname"), case.get("name")) if part
            )
            tags = {child.tag for child in case}
            # pytest reports xfailed tests as <skipped>, so they are counted as skipped.
            if "failure" in tags:
                counts["failed"] += 1
            elif "error" in tags:
                counts["error"] += 1
            elif "skipped" in tags:
                counts["skipped"] += 1
            else:
                counts["passed"] += 1
            for child in case:
                if child.tag not in ("failure", "error"):
                    continue
                outcome = "FAILED" if child.tag == "failure" else "ERROR"
                message = (child.get("message") or "").strip().split("\n")[0]
                traceback = _tail(child.text, TRACEBACK_LINES)
                problems.append((outcome, nodeid, message[:MESSAGE_CHARS], traceback))
    return counts, seconds, problems


def build_summary(results_dir, suites, failed_steps=None, max_chars=30000):
    head = ["===== E2E TEST SUMMARY ====="]
    if failed_steps and os.path.isfile(failed_steps):
        with open(failed_steps) as f:
            steps = [line.strip() for line in f if line.strip()]
        if steps:
            head.append("Failed build steps: " + ", ".join(steps))
    head.append("")
    head.append(
        f"{'suite':<12}{'passed':>8}{'failed':>8}{'error':>8}{'skipped':>9}{'time':>10}"
    )

    # (suite, full detail, one-line fallback) for each problem, in suite order.
    problems = []
    for suite in suites:
        xml_path = os.path.join(results_dir, f"{suite}.xml")
        log_path = os.path.join(results_dir, f"{suite}.log")
        try:
            counts, seconds, suite_problems = parse_junit(xml_path)
        except (OSError, ET.ParseError) as e:
            reason = (
                "no JUnit results"
                if isinstance(e, OSError)
                else "unreadable JUnit results"
            )
            head.append(f"{suite:<12}  {reason}")
            if os.path.isfile(log_path):
                # Keep only the last lines in memory; a crashed suite's log can be large.
                with open(log_path, errors="replace") as f:
                    log_tail = "".join(deque(f, maxlen=LOG_TAIL_LINES))
                body = f"Last {LOG_TAIL_LINES} lines of {suite}.log:\n"
                body += _tail(log_tail, LOG_TAIL_LINES)
            else:
                body = "The suite did not run; see its step log above."
            problems.append(
                (suite, f"--- {suite}: {reason} ---\n{body}", f"{suite}: {reason}")
            )
            continue

        head.append(
            f"{suite:<12}{counts['passed']:>8}{counts['failed']:>8}"
            f"{counts['error']:>8}{counts['skipped']:>9}{seconds:>9.0f}s"
        )
        for outcome, nodeid, message, traceback in suite_problems:
            detail = f"--- {suite}: {outcome} {nodeid} ---\n{message}\n{traceback}"
            one_line = f"{suite}: {outcome} {nodeid}"[:ONE_LINE_CHARS]
            problems.append((suite, detail, one_line))

    if not problems:
        head.append("")
        head.append("No failures or errors.")

    # Fill the budget round-robin across suites (each suite's 1st problem, then each 2nd, ...) so a
    # suite with hundreds of failures cannot crowd out the other suites' failures.
    next_rank = {}
    ranked = []
    for i, (suite, _, _) in enumerate(problems):
        ranked.append((next_rank.get(suite, 0), i))
        next_rank[suite] = next_rank.get(suite, 0) + 1
    order = [i for _, i in sorted(ranked)]

    parts = ["\n".join(head)]
    used = len(parts[0]) + len(FOOTER) + NOTE_RESERVE
    detailed = set()
    suites_out_of_room = set()
    for i in order:
        suite, detail, _ = problems[i]
        if suite not in suites_out_of_room and used + len(detail) + 2 <= max_chars:
            detailed.add(i)
            used += len(detail) + 2
        else:
            suites_out_of_room.add(suite)

    listed = set()
    omitted = 0
    for i in order:
        if i in detailed:
            continue
        one_line = problems[i][2]
        if used + len(one_line) + 1 <= max_chars:
            listed.add(i)
            used += len(one_line) + 1
        else:
            omitted += 1

    # Render in suite order.
    parts.extend(problems[i][1] for i in sorted(detailed))
    if listed or omitted:
        lines = ["Not shown in detail (see the full logs below):"]
        lines.extend(problems[i][2] for i in sorted(listed))
        if omitted:
            lines.append(f"... and {omitted} more")
        parts.append("\n".join(lines))

    parts.append(FOOTER)
    return "\n\n".join(parts)[:max_chars]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "results_dir", help="Directory with <suite>.xml and <suite>.log"
    )
    parser.add_argument("--suites", nargs="+", required=True)
    parser.add_argument("--failed-steps", help="File listing failed build steps")
    parser.add_argument("--max-chars", type=int, default=30000)
    args = parser.parse_args(argv)
    print(
        build_summary(args.results_dir, args.suites, args.failed_steps, args.max_chars)
    )


if __name__ == "__main__":
    main()
