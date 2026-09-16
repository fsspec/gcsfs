import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

# Add the directory containing summarize_test_results.py to the path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
import summarize_test_results

TAGS = {"failed": "failure", "error": "error", "skipped": "skipped"}


def write_junit(path, cases, time=12.0):
    """Write a pytest-style JUnit XML file from (classname, name, outcome, message, text)."""
    root = ET.Element("testsuites", name="pytest tests")
    suite = ET.SubElement(root, "testsuite", name="pytest", time=str(time))
    for classname, name, outcome, message, text in cases:
        case = ET.SubElement(suite, "testcase", classname=classname, name=name)
        if outcome != "passed":
            child = ET.SubElement(case, TAGS[outcome], message=message)
            child.text = text
    ET.ElementTree(root).write(path)


def row(out, suite):
    match = re.search(
        rf"^{re.escape(suite)}\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s", out, re.M
    )
    assert match, out
    return tuple(int(n) for n in match.groups())


def test_all_pass_counts(tmp_path):
    write_junit(
        tmp_path / "standard.xml",
        [
            ("gcsfs.tests.test_core", "test_a", "passed", "", ""),
            ("gcsfs.tests.test_core", "test_b", "passed", "", ""),
            ("gcsfs.tests.test_core", "test_c", "passed", "", ""),
            ("gcsfs.tests.test_core", "test_d", "skipped", "not on zonal", ""),
        ],
    )

    out = summarize_test_results.build_summary(str(tmp_path), ["standard"])

    assert row(out, "standard") == (3, 0, 0, 1)
    assert "No failures or errors." in out


def test_failures_and_errors_show_message_and_traceback_tail(tmp_path):
    traceback = (
        "\n".join(f"tb-{i:03d}" for i in range(100)) + "\nE   AssertionError: boom"
    )
    write_junit(
        tmp_path / "zonal-core.xml",
        [
            ("gcsfs.tests.test_core", "test_ok", "passed", "", ""),
            (
                "gcsfs.tests.test_core",
                "test_bad",
                "failed",
                "AssertionError: boom\nmore",
                traceback,
            ),
            (
                "gcsfs.tests.test_core",
                "test_setup",
                "error",
                "failed on setup",
                "RuntimeError: x",
            ),
        ],
    )

    out = summarize_test_results.build_summary(str(tmp_path), ["zonal-core"])

    assert row(out, "zonal-core") == (1, 1, 1, 0)
    assert (
        "--- zonal-core: FAILED gcsfs.tests.test_core::test_bad ---\nAssertionError: boom\n"
        in out
    )
    assert "E   AssertionError: boom" in out
    assert "tb-099" in out
    assert "tb-060" not in out
    assert "--- zonal-core: ERROR gcsfs.tests.test_core::test_setup ---" in out
    assert "RuntimeError: x" in out
    assert "No failures or errors." not in out


def test_real_pytest_junit_output(tmp_path):
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    (tmp_path / "test_sample.py").write_text(
        "import pytest\n"
        "\n"
        "@pytest.fixture\n"
        "def broken():\n"
        "    raise RuntimeError('fixture exploded')\n"
        "\n"
        "def test_pass():\n"
        "    pass\n"
        "\n"
        "def test_fail():\n"
        "    assert 1 == 2, 'numbers differ'\n"
        "\n"
        "def test_error(broken):\n"
        "    pass\n"
        "\n"
        "@pytest.mark.skip(reason='unsupported')\n"
        "def test_skip():\n"
        "    pass\n"
    )
    results = tmp_path / "results"
    results.mkdir()
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "no:cacheprovider",
            "-c",
            str(tmp_path / "pytest.ini"),
            "--rootdir",
            str(tmp_path),
            f"--junitxml={results / 'hns.xml'}",
            str(tmp_path / "test_sample.py"),
        ],
        cwd=tmp_path,
        capture_output=True,
    )

    out = summarize_test_results.build_summary(str(results), ["hns"])

    assert row(out, "hns") == (1, 1, 1, 1)
    assert "--- hns: FAILED test_sample::test_fail ---" in out
    assert "AssertionError: numbers differ" in out
    assert "--- hns: ERROR test_sample::test_error ---" in out
    assert "RuntimeError: fixture exploded" in out


def test_missing_xml_prints_log_tail(tmp_path):
    log = "\n".join(f"line-{i:03d}" for i in range(100))
    (tmp_path / "zonal.log").write_text(log)

    out = summarize_test_results.build_summary(str(tmp_path), ["zonal"])

    assert re.search(r"^zonal\s+no JUnit results$", out, re.M)
    assert "--- zonal: no JUnit results ---" in out
    assert "line-099" in out
    assert "Last 60 lines of zonal.log:\nline-040\n" in out
    assert "line-039" not in out


def test_missing_suite_without_log(tmp_path):
    out = summarize_test_results.build_summary(str(tmp_path / "absent"), ["hns"])

    assert (
        "--- hns: no JUnit results ---\nThe suite did not run; see its step log above."
        in out
    )


def test_unparseable_xml(tmp_path):
    (tmp_path / "standard.xml").write_text("<testsuites><testsuite")

    out = summarize_test_results.build_summary(str(tmp_path), ["standard"])

    assert "--- standard: unreadable JUnit results ---" in out


def test_failed_steps_listed(tmp_path):
    write_junit(tmp_path / "standard.xml", [("m", "test_a", "passed", "", "")])
    failed = tmp_path / "FAILED"
    failed.write_text("run-zonal-core-tests\nrun-hns-tests\n")

    out = summarize_test_results.build_summary(str(tmp_path), ["standard"], str(failed))

    assert "Failed build steps: run-zonal-core-tests, run-hns-tests" in out


def test_output_never_exceeds_budget_and_keeps_counts(tmp_path):
    traceback = "\n".join("E   " + "x" * 200 for _ in range(60))
    for suite in ("standard", "hns"):
        write_junit(
            tmp_path / f"{suite}.xml",
            [
                (f"gcsfs.tests.test_{suite}", f"test_{i}", "failed", "boom", traceback)
                for i in range(300)
            ],
        )

    out = summarize_test_results.build_summary(
        str(tmp_path), ["standard", "hns"], max_chars=20000
    )

    assert len(out) <= 20000
    assert row(out, "standard") == (0, 300, 0, 0)
    assert row(out, "hns") == (0, 300, 0, 0)
    assert "--- standard: FAILED gcsfs.tests.test_standard::test_0 ---" in out
    assert "--- hns: FAILED gcsfs.tests.test_hns::test_0 ---" in out
    assert "Not shown in detail (see the full logs below):" in out
    assert "\nstandard: FAILED gcsfs.tests.test_standard::test_299\n" not in out
    assert "\nhns: FAILED gcsfs.tests.test_hns::test_5\n" in out
    assert re.search(r"^\.\.\. and \d+ more$", out, re.M)
    assert out.endswith(summarize_test_results.FOOTER)


def test_every_suite_shows_a_detail_when_an_earlier_suite_has_many_failures(tmp_path):
    traceback = "\n".join("E   " + "x" * 200 for _ in range(60))
    write_junit(
        tmp_path / "standard.xml",
        [
            ("gcsfs.tests.test_core", f"test_{i}", "failed", "boom", traceback)
            for i in range(300)
        ],
    )
    write_junit(
        tmp_path / "zonal.xml",
        [
            (
                "gcsfs.tests.test_zonal_file",
                "test_z",
                "failed",
                "zonal boom",
                "E   zonal boom",
            )
        ],
    )
    (tmp_path / "hns.log").write_text("collection crashed\n")

    out = summarize_test_results.build_summary(
        str(tmp_path), ["standard", "zonal", "zonal-core", "hns"]
    )

    assert len(out) <= 30000
    standard = out.index("--- standard: FAILED gcsfs.tests.test_core::test_0 ---")
    zonal = out.index(
        "--- zonal: FAILED gcsfs.tests.test_zonal_file::test_z ---\nzonal boom\nE   zonal boom"
    )
    zonal_core = out.index(
        "--- zonal-core: no JUnit results ---\nThe suite did not run"
    )
    hns = out.index(
        "--- hns: no JUnit results ---\nLast 60 lines of hns.log:\ncollection crashed"
    )
    # Details are still rendered in suite order.
    assert standard < zonal < zonal_core < hns


def test_tiny_budget_is_still_capped(tmp_path):
    write_junit(
        tmp_path / "standard.xml", [("m", "test_a", "failed", "boom", "E   boom")]
    )

    out = summarize_test_results.build_summary(
        str(tmp_path), ["standard"], max_chars=40
    )

    assert len(out) == 40
    assert out.startswith("===== E2E TEST SUMMARY =====")


def test_main_cli(tmp_path, capsys):
    write_junit(tmp_path / "zonal.xml", [("m", "test_a", "failed", "boom", "E   boom")])

    summarize_test_results.main(
        [str(tmp_path), "--suites", "zonal", "standard", "--max-chars", "5000"]
    )

    out = capsys.readouterr().out
    assert row(out, "zonal") == (0, 1, 0, 0)
    assert "--- zonal: FAILED m::test_a ---" in out
    assert "--- standard: no JUnit results ---" in out
