"""Offline tests for the ADK artifact benchmark driver.

They run every scenario at reduced scale against ADK's ``InMemoryArtifactService``,
so no bucket is needed. Skipped when google-adk is not installed (it is not a
gcsfs test dependency).
"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("google.adk")

HELM_CHART_DIR = Path(__file__).resolve().parents[1] / "helm_chart"
METRICS_DIR = Path(__file__).resolve().parents[7] / "cloudbuild" / "macrobenchmarks"
sys.path.insert(0, str(HELM_CHART_DIR))
sys.path.insert(0, str(METRICS_DIR))

import adk_bench  # noqa: E402
from google.adk.artifacts.in_memory_artifact_service import (  # noqa: E402
    InMemoryArtifactService,
)
from google.genai import types  # noqa: E402
from metrics.parsers import adk as adk_parser  # noqa: E402
from metrics.parsers.common import LogEntry  # noqa: E402


class CorruptingService(InMemoryArtifactService):
    """Flips one byte of every inline payload it returns."""

    async def load_artifact(self, **kwargs):
        part = await super().load_artifact(**kwargs)
        if part is not None and part.inline_data is not None and part.inline_data.data:
            data = bytearray(part.inline_data.data)
            data[0] ^= 0xFF
            part = part.model_copy(deep=True)
            part.inline_data.data = bytes(data)
        return part


@pytest.fixture
def in_memory(monkeypatch, tmp_path):
    """Routes the driver to one in-memory store with small unit-test scale."""
    store = {"service": InMemoryArtifactService()}
    monkeypatch.setattr(adk_bench, "build_service", lambda base_url: store["service"])
    monkeypatch.setattr(adk_bench, "KIB", 16)
    monkeypatch.setattr(adk_bench, "MIB", 16 * 1024)
    monkeypatch.setattr(
        adk_bench,
        "SCENARIOS",
        {
            "iterative-workspace": adk_bench.Scenario(
                sessions=2, turns=10, think_time_s=0.0
            ),
            "genmedia-pipeline": adk_bench.Scenario(
                sessions=2, turns=7, think_time_s=0.0
            ),
            "hitl-handoff": adk_bench.Scenario(
                sessions=2,
                turns=3,
                think_time_s=0.0,
                users=2,
                files_min=3,
                files_max=5,
            ),
        },
    )

    async def resume_in_process(args, bench):
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps(adk_bench.state_to_json(bench)))
        resume_args = adk_bench.parse_args(
            [
                "--scenario",
                args.scenario,
                "--base-url",
                args.base_url,
                "--run-tag",
                args.run_tag,
                "--phase",
                "resume",
                "--manifest",
                str(manifest),
            ]
        )
        return await adk_bench.run_scenario(
            resume_args, adk_bench.SCENARIOS[args.scenario]
        )

    monkeypatch.setattr(adk_bench, "run_resume_process", resume_in_process)
    return store


def _run(scenario, capsys):
    code = adk_bench.main(
        ["--scenario", scenario, "--base-url", "memory://bench", "--run-tag", "t1"]
    )
    return code, capsys.readouterr().out.splitlines()


@pytest.mark.parametrize("scenario", sorted(adk_bench.SCENARIOS))
def test_scenarios_pass_validation(in_memory, capsys, scenario):
    code, lines = _run(scenario, capsys)

    assert code == 0, "\n".join(lines[-20:])
    op_lines = [line for line in lines if line.startswith("Artifact Op")]
    assert op_lines
    probes = [line for line in lines if line.startswith("Artifact Probe")]
    assert [line.split("Op: ")[1].split(" :")[0] for line in probes] == list(
        adk_bench.OPS
    )
    (workload,) = [line for line in lines if line.startswith("Artifact Workload")]
    assert workload.endswith("Failures: 0")
    # The scrape parser reads every line the driver printed.
    parsed = adk_parser.parse_entries(
        LogEntry(timestamp=0.0, message=line) for line in lines
    )
    assert len(parsed.artifact_op_metrics) == len(op_lines)
    assert len(parsed.artifact_probe_metrics) == len(adk_bench.OPS)
    (row,) = parsed.artifact_workload_metrics
    assert row.ops == len(op_lines)
    assert row.validation_checks > 0
    assert row.validation_failures == 0


def test_hitl_reports_the_resume_process_cold_op(in_memory, capsys):
    _, lines = _run("hitl-handoff", capsys)

    (workload,) = [line for line in lines if line.startswith("Artifact Workload")]
    assert "Cold First Op: N/A" not in workload


def test_corrupted_payloads_fail_validation(in_memory, capsys):
    in_memory["service"] = CorruptingService()

    code, lines = _run("genmedia-pipeline", capsys)

    assert code == 1
    (workload,) = [line for line in lines if line.startswith("Artifact Workload")]
    assert not workload.endswith("Failures: 0")


def test_payloads_are_deterministic_and_typed():
    spec = adk_bench.PayloadSpec(
        seed="s", size=1000, kind="inline", mime_type="image/png", display_name="a.png"
    )

    assert spec.payload() == spec.payload()
    assert len(spec.payload()) == 1000
    assert adk_bench.check_part(spec.to_part(), spec) is None
    text = adk_bench.PayloadSpec(seed="s", size=999, kind="text")
    assert len(text.payload()) == 999
    assert adk_bench.check_part(text.to_part(), text) is None


def test_check_part_reports_each_mismatch():
    spec = adk_bench.PayloadSpec(
        seed="s", size=64, kind="inline", mime_type="image/png", display_name="a.png"
    )
    wrong_mime = types.Part(
        inline_data=types.Blob(
            data=spec.payload(), mime_type="image/jpeg", display_name="a.png"
        )
    )
    wrong_bytes = types.Part(
        inline_data=types.Blob(
            data=b"x" * 64, mime_type="image/png", display_name="a.png"
        )
    )

    assert "mime_type" in adk_bench.check_part(wrong_mime, spec)
    assert "sha256" in adk_bench.check_part(wrong_bytes, spec)
    assert adk_bench.check_part(None, spec) == "load returned None"
    tombstone = adk_bench.PayloadSpec(seed="", size=0, kind="tombstone")
    assert adk_bench.check_part(None, tombstone) is None


def test_bench_names_and_times_ops(capsys):
    bench = adk_bench.Bench(
        InMemoryArtifactService(),
        adk_bench.Scenario(sessions=1, turns=1, think_time_s=0.0),
        seed="s",
        uri_scheme="memory",
    )
    state = bench.new_session(0, 0)
    spec = adk_bench.PayloadSpec(seed="s", size=5, kind="text")

    async def calls():
        await bench.save(state, "f.md", spec)
        await bench.load_latest(state, "f.md")
        await bench.load_version(state, "f.md", 0)
        await bench.list_keys(state)
        await bench.list_versions(state, "f.md")
        await bench.get_artifact_version(state, "f.md", 0)
        await bench.delete(state, "f.md")

    asyncio.run(calls())

    lines = capsys.readouterr().out.splitlines()
    assert [line.split("Op: ")[1].split(" :")[0] for line in lines] == list(
        adk_bench.OPS
    )
    assert lines[0].startswith("Artifact Op : Seq: main:1 : Op: save : Latency: ")
    assert lines[0].endswith("ms : Bytes: 5 : Ok: true")
