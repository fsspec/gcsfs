"""Tests for the ADK artifact benchmark driver.

Covers all scenarios against ``InMemoryArtifactService`` plus end-to-end
integration against ``FsspecArtifactService`` on ``gcsfs`` and
``fake-gcs-server``. Skipped only when ``google-adk`` is not installed.
"""

import asyncio
import json
import os
import sys
from pathlib import Path

import fsspec
import pytest

pytest.importorskip("google.adk")

HELM_CHART_DIR = Path(__file__).resolve().parents[1] / "helm_chart"
MICROBENCH_DIR = Path(__file__).resolve().parents[4] / "microbenchmarks"
REPO_ROOT = Path(__file__).resolve().parents[7]
METRICS_DIR = REPO_ROOT / "cloudbuild" / "macrobenchmarks"
for _extra_path in (HELM_CHART_DIR, MICROBENCH_DIR, METRICS_DIR):
    if str(_extra_path) not in sys.path:
        sys.path.insert(0, str(_extra_path))

import adk_bench  # noqa: E402
from google.adk.artifacts.in_memory_artifact_service import (  # noqa: E402
    InMemoryArtifactService,
)
from google.adk.integrations.fsspec import FsspecArtifactService  # noqa: E402
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
    monkeypatch.setattr(adk_bench, "CONCURRENT_SAVES", 3)
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
                "--seed",
                args.seed,
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
    bench.close()

    lines = capsys.readouterr().out.splitlines()
    assert [line.split("Op: ")[1].split(" :")[0] for line in lines] == list(
        adk_bench.OPS
    )
    assert lines[0].startswith("Artifact Op : Seq: main:1 : Op: save : Latency: ")
    assert lines[0].endswith("ms : Bytes: 5 : Ok: true")


def test_seed_is_independent_of_run_tag(in_memory, capsys):
    code_a = adk_bench.main(
        ["--scenario", "hitl-handoff", "--base-url", "memory://bench", "--run-tag", "a"]
    )
    lines_a = capsys.readouterr().out.splitlines()
    in_memory["service"] = InMemoryArtifactService()
    code_b = adk_bench.main(
        ["--scenario", "hitl-handoff", "--base-url", "memory://bench", "--run-tag", "b"]
    )
    lines_b = capsys.readouterr().out.splitlines()

    assert code_a == 0 and code_b == 0
    parsed_a = adk_parser.parse_entries(
        LogEntry(timestamp=0.0, message=line) for line in lines_a
    )
    parsed_b = adk_parser.parse_entries(
        LogEntry(timestamp=0.0, message=line) for line in lines_b
    )
    assert sorted((m.op, m.bytes) for m in parsed_a.artifact_op_metrics) == sorted(
        (m.op, m.bytes) for m in parsed_b.artifact_op_metrics
    )


def test_rewind_handles_missing_historical_version_without_aborting():
    class MissingVersionService(InMemoryArtifactService):
        async def load_artifact(self, **kwargs):
            if kwargs.get("version") is not None:
                return None
            return await super().load_artifact(**kwargs)

    bench = adk_bench.Bench(
        MissingVersionService(),
        adk_bench.Scenario(sessions=1, turns=1, think_time_s=0.0),
        seed="s",
        uri_scheme="memory",
    )
    state = bench.new_session(0, 0)

    async def calls():
        await bench.save(
            state, "doc.txt", adk_bench.PayloadSpec(seed="v0", size=8, kind="text")
        )
        snapshot = {"doc.txt": state.session_scope.latest_version("doc.txt")}
        await bench.save(
            state, "doc.txt", adk_bench.PayloadSpec(seed="v1", size=8, kind="text")
        )
        await bench.rewind(state, snapshot)
        await bench.validate_final_state([state])

    asyncio.run(calls())
    bench.close()

    assert len(bench.checks.failures) == 1
    assert "load returned None" in bench.checks.failures[0]


def test_corrupted_user_artifact_fails_final_validation(in_memory, capsys):
    class CorruptUserOnlyService(InMemoryArtifactService):
        async def load_artifact(self, **kwargs):
            part = await super().load_artifact(**kwargs)
            if (
                kwargs.get("session_id") is None
                and part is not None
                and part.inline_data is not None
                and part.inline_data.data
            ):
                data = bytearray(part.inline_data.data)
                data[0] ^= 0xFF
                part = part.model_copy(deep=True)
                part.inline_data.data = bytes(data)
            return part

    in_memory["service"] = CorruptUserOnlyService()

    code, lines = _run("hitl-handoff", capsys)

    assert code == 1
    (workload,) = [line for line in lines if line.startswith("Artifact Workload")]
    assert not workload.endswith("Failures: 0")


def test_driver_runs_against_fsspec_artifact_service_and_gcsfs(
    gcs_factory, flat_bucket, monkeypatch, capsys
):
    monkeypatch.setattr(
        adk_bench,
        "build_service",
        lambda base_url: FsspecArtifactService(
            base_url, fs=gcs_factory(use_listings_cache=False)
        ),
    )
    monkeypatch.setattr(adk_bench, "KIB", 16)
    monkeypatch.setattr(adk_bench, "CONCURRENT_SAVES", 3)
    monkeypatch.setattr(
        adk_bench,
        "SCENARIOS",
        {
            **adk_bench.SCENARIOS,
            "iterative-workspace": adk_bench.Scenario(
                sessions=2, turns=10, think_time_s=0.0
            ),
        },
    )

    code = adk_bench.main(
        [
            "--scenario",
            "iterative-workspace",
            "--base-url",
            f"gs://{flat_bucket}/adk",
            "--run-tag",
            "emu",
        ]
    )
    lines = capsys.readouterr().out.splitlines()

    assert code == 0, "\n".join(lines[-20:])
    parsed = adk_parser.parse_entries(
        LogEntry(timestamp=0.0, message=line) for line in lines
    )
    assert any(m.op == "list_versions" for m in parsed.artifact_op_metrics)
    assert any(m.op == "save" and m.bytes == 0 for m in parsed.artifact_op_metrics)
    probes = {p.op: p.requests_per_op for p in parsed.artifact_probe_metrics}
    assert all(count > 0 for count in probes.values()), probes
    assert probes["list_keys"] == 2.0
    (workload,) = parsed.artifact_workload_metrics
    assert workload.validation_failures == 0


def test_hitl_handoff_runs_real_resume_subprocess(
    docker_gcs, gcs_factory, flat_bucket, monkeypatch
):
    emulator_opts = {"token": "anon", "endpoint_url": docker_gcs}
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.setenv("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "false")
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", docker_gcs)
    monkeypatch.setenv("FSSPEC_GCS", json.dumps(emulator_opts))
    monkeypatch.setenv("FSSPEC_GS", json.dumps(emulator_opts))
    monkeypatch.setitem(fsspec.config.conf, "gcs", emulator_opts)
    monkeypatch.setitem(fsspec.config.conf, "gs", emulator_opts)
    monkeypatch.setenv(
        "PYTHONPATH",
        os.pathsep.join(
            [
                str(REPO_ROOT),
                str(HELM_CHART_DIR),
                str(MICROBENCH_DIR),
                os.environ.get("PYTHONPATH", ""),
            ]
        ),
    )
    args = adk_bench.parse_args(
        [
            "--scenario",
            "hitl-handoff",
            "--base-url",
            f"gs://{flat_bucket}/adk",
            "--run-tag",
            "subproc",
        ]
    )
    base_url = f"{args.base_url.rstrip('/')}/{args.scenario}-{args.run_tag}"
    scenario = adk_bench.Scenario(
        sessions=1, turns=1, think_time_s=0.0, users=1, files_min=1, files_max=1
    )
    bench = adk_bench.Bench(
        FsspecArtifactService(base_url, fs=gcs_factory(use_listings_cache=False)),
        scenario,
        seed=args.seed,
        uri_scheme="gs",
    )
    state = bench.new_session(0, 0)

    async def handoff():
        await bench.save(
            state,
            "user:profile.json",
            adk_bench.PayloadSpec(
                seed="profile",
                size=32,
                kind="inline",
                mime_type="application/json",
            ),
        )
        await bench.save(
            state,
            "summary.json",
            adk_bench.PayloadSpec(
                seed="summary",
                size=32,
                kind="inline",
                mime_type="application/json",
            ),
        )
        resume = await adk_bench.run_resume_process(args, bench)
        assert resume["failures"] == [], resume
        adk_bench.state_from_json(bench, resume["state"])
        await bench.validate_final_state(bench.sessions)
        return resume

    try:
        resume = asyncio.run(handoff())
    finally:
        bench.close()

    assert resume["cold_first_op_ms"] is not None
    assert resume["ops"] == 3
    assert bench.checks.failures == []


def test_run_resume_process_reports_nonzero_exit_and_corrupt_json(monkeypatch):
    bench = adk_bench.Bench(
        InMemoryArtifactService(),
        adk_bench.SCENARIOS["hitl-handoff"],
        seed="42",
        uri_scheme="memory",
    )
    args = adk_bench.parse_args(
        ["--scenario", "hitl-handoff", "--base-url", "memory://b", "--run-tag", "t"]
    )

    class StubProc:
        def __init__(self, returncode):
            self.returncode = returncode

        async def wait(self):
            return self.returncode

    async def nonzero_with_result(*command):
        result_file = command[command.index("--result-file") + 1]
        Path(result_file).write_text(json.dumps({"failures": []}))
        return StubProc(2)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", nonzero_with_result)
    resume = asyncio.run(adk_bench.run_resume_process(args, bench))
    assert any("exited 2" in f for f in resume["failures"])
    assert any("omitted session state" in f for f in resume["failures"])

    async def corrupt_result(*command):
        result_file = command[command.index("--result-file") + 1]
        Path(result_file).write_text("{truncated")
        return StubProc(0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", corrupt_result)
    resume_corrupt = asyncio.run(adk_bench.run_resume_process(args, bench))
    assert any("unreadable result" in f for f in resume_corrupt["failures"])
    bench.close()


def test_duplicate_and_gapped_save_versions_fail_validation(in_memory, capsys):
    class DuplicateUserVersionService(InMemoryArtifactService):
        async def save_artifact(self, **kwargs):
            version = await super().save_artifact(**kwargs)
            if kwargs["filename"].startswith("user:") and version > 0:
                key = self._artifact_path(
                    kwargs["app_name"],
                    kwargs["user_id"],
                    kwargs["filename"],
                    kwargs.get("session_id"),
                )
                self.artifacts[key] = [self.artifacts[key][-1]]
                return 0
            return version

    in_memory["service"] = DuplicateUserVersionService()
    code, lines = _run("hitl-handoff", capsys)
    assert code == 1
    (workload,) = [line for line in lines if line.startswith("Artifact Workload")]
    assert not workload.endswith("Failures: 0")

    class GappedSessionVersionService(InMemoryArtifactService):
        async def save_artifact(self, **kwargs):
            version = await super().save_artifact(**kwargs)
            if not kwargs["filename"].startswith("user:") and version == 1:
                return 2
            return version

    in_memory["service"] = GappedSessionVersionService()
    code_gap, lines_gap = _run("iterative-workspace", capsys)
    assert code_gap == 1
    (workload_gap,) = [
        line for line in lines_gap if line.startswith("Artifact Workload")
    ]
    assert not workload_gap.endswith("Failures: 0")


def test_hitl_resume_does_not_serialize_other_sessions_behind_session0_think(
    tmp_path, monkeypatch
):
    events = []
    service = InMemoryArtifactService()
    scenario = adk_bench.Scenario(
        sessions=2, turns=1, think_time_s=0.05, users=1, files_min=1, files_max=1
    )
    monkeypatch.setattr(adk_bench, "build_service", lambda base_url: service)
    monkeypatch.setattr(adk_bench, "KIB", 16)
    monkeypatch.setattr(adk_bench, "MIB", 16 * 1024)

    upload = adk_bench.Bench(service, scenario, seed="42", uri_scheme="memory")
    states = [upload.new_session(0, index) for index in range(2)]

    async def seed_uploads():
        await asyncio.gather(*(upload.hitl_upload_session(s) for s in states))

    asyncio.run(seed_uploads())
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(adk_bench.state_to_json(upload)))
    upload.close()

    orig_list_keys = adk_bench.Bench.list_keys

    async def tracked_list_keys(self, state):
        events.append(("list_keys", state.session_id))
        return await orig_list_keys(self, state)

    async def tracked_think(self, rng):
        events.append(("think_start", len(events)))
        await asyncio.sleep(0.05)
        events.append(("think_end", len(events)))

    monkeypatch.setattr(adk_bench.Bench, "list_keys", tracked_list_keys)
    monkeypatch.setattr(adk_bench.Bench, "think", tracked_think)

    resume_args = adk_bench.parse_args(
        [
            "--scenario",
            "hitl-handoff",
            "--base-url",
            "memory://bench",
            "--run-tag",
            "t1",
            "--phase",
            "resume",
            "--manifest",
            str(manifest),
        ]
    )
    resume = asyncio.run(adk_bench.run_scenario(resume_args, scenario))

    assert resume["failures"] == []
    assert resume["cold_first_op_ms"] is not None
    assert events[0] == ("list_keys", "s0000")
    s1_list_idx = events.index(("list_keys", "s0001"))
    first_think_end_idx = next(
        idx for idx, item in enumerate(events) if item[0] == "think_end"
    )
    assert s1_list_idx < first_think_end_idx
