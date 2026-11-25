"""ADK ``FsspecArtifactService`` macrobenchmark driver.

Replays artifact call sequences taken from ``google/adk-recipes`` against
``FsspecArtifactService`` on a real bucket. Every service call is timed and
printed as one log line, which ``cloudbuild/macrobenchmarks/metrics/parsers/
adk.py`` scrapes from Cloud Logging. Correctness checks run outside the timed
calls, and any mismatch makes the process exit non-zero.

Usage::

    python adk_bench.py --scenario iterative-workspace --base-url gs://<bucket>/<prefix>
"""

import argparse
import asyncio
import dataclasses
import datetime
import hashlib
import json
import math
import os
import random
import sys
import tempfile
import time
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional

os.environ.setdefault("ADK_ENABLE_FSSPEC_ARTIFACT_SERVICE", "1")

from google.adk.artifacts import artifact_util  # noqa: E402
from google.genai import types  # noqa: E402

KIB = 1024
MIB = 1024 * KIB

# Ops reported per summary row.
OPS = (
    "save",
    "load_latest",
    "load_version",
    "list_keys",
    "list_versions",
    "get_version",
    "delete",
)

APP_NAME = "adkbench"
PROBE_ROUNDS = 3
CONCURRENT_SAVES = 8


def emit(line: str) -> None:
    """Prints one metric line. Cloud Logging stores each line as one entry."""
    print(line, flush=True)


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------
@dataclasses.dataclass(frozen=True)
class Scenario:
    sessions: int
    turns: int
    think_time_s: float
    users: int = 0
    files_min: int = 0
    files_max: int = 0


SCENARIOS: Dict[str, Scenario] = {
    "iterative-workspace": Scenario(sessions=32, turns=50, think_time_s=1.0),
    "genmedia-pipeline": Scenario(sessions=8, turns=7, think_time_s=5.0),
    "hitl-handoff": Scenario(
        sessions=2, turns=3, think_time_s=1.0, users=16, files_min=50, files_max=200
    ),
}


# ---------------------------------------------------------------------------
# Payloads
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class PayloadSpec:
    """What one saved version must load back as.

    The bytes are derived from ``seed``, so checks regenerate them instead of
    keeping every payload in memory.
    """

    seed: str
    size: int
    kind: str  # "inline", "text" or "tombstone"
    mime_type: Optional[str] = None
    display_name: Optional[str] = None
    custom_metadata: Optional[Dict[str, str]] = None
    verified: bool = False

    def payload(self) -> bytes:
        if self.kind == "tombstone":
            return b""
        if self.kind == "text":
            return (
                hashlib.shake_256(self.seed.encode())
                .hexdigest((self.size + 1) // 2)[: self.size]
                .encode()
            )
        return hashlib.shake_256(self.seed.encode()).digest(self.size)

    def sha256(self) -> str:
        return hashlib.sha256(self.payload()).hexdigest()

    def to_part(self) -> types.Part:
        if self.kind == "tombstone":
            return artifact_util._new_rewind_tombstone()
        if self.kind == "text":
            return types.Part(text=self.payload().decode())
        return types.Part(
            inline_data=types.Blob(
                data=self.payload(),
                mime_type=self.mime_type,
                display_name=self.display_name,
            )
        )


def lognormal_size(rng: random.Random, median: int, low: int, high: int) -> int:
    """A size around ``median`` clipped to ``[low, high]``."""
    size = median * math.exp(rng.gauss(0.0, 0.8))
    return max(1, int(min(max(size, low), high)))


def part_size(part: Optional[types.Part]) -> int:
    if part is None:
        return 0
    if part.text is not None:
        return len(part.text.encode())
    if part.inline_data is not None and part.inline_data.data is not None:
        return len(part.inline_data.data)
    return 0


def check_part(
    part: Optional[types.Part], spec: Optional[PayloadSpec]
) -> Optional[str]:
    """Returns an error message, or None when ``part`` matches ``spec``."""
    if spec is None or spec.kind == "tombstone":
        return None if part is None else "expected no artifact"
    if part is None:
        return "load returned None"
    if spec.kind == "text":
        if part.text is None:
            return "expected a text part"
        loaded = part.text.encode()
    else:
        if part.inline_data is None or part.inline_data.data is None:
            return "expected an inline_data part"
        loaded = part.inline_data.data
        if part.inline_data.mime_type != spec.mime_type:
            return f"mime_type {part.inline_data.mime_type!r} != {spec.mime_type!r}"
        if (part.inline_data.display_name or None) != spec.display_name:
            return (
                f"display_name {part.inline_data.display_name!r} != "
                f"{spec.display_name!r}"
            )
    if hashlib.sha256(loaded).hexdigest() != spec.sha256():
        return "payload sha256 mismatch"
    return None


# ---------------------------------------------------------------------------
# Expected state and checks
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class Scope:
    """Expected versions per filename (dict order = first-save order)."""

    versions: Dict[str, Dict[int, PayloadSpec]] = dataclasses.field(
        default_factory=dict
    )

    def record(self, filename: str, version: int, spec: PayloadSpec) -> None:
        self.versions.setdefault(filename, {})[version] = spec

    def latest_version(self, filename: str) -> Optional[int]:
        saved = self.versions.get(filename)
        return max(saved) if saved else None

    def latest(self, filename: str) -> Optional[PayloadSpec]:
        version = self.latest_version(filename)
        return None if version is None else self.versions[filename][version]

    def live_keys(self) -> set:
        return {name for name, saved in self.versions.items() if saved}


@dataclasses.dataclass
class SessionState:
    user_id: str
    session_id: str
    session_scope: Scope
    user_scope: Scope

    def scope_for(self, filename: str) -> Scope:
        return self.user_scope if filename.startswith("user:") else self.session_scope


class Checks:
    """Counts correctness checks and keeps the first failures."""

    def __init__(self):
        self.count = 0
        self.failures: List[str] = []

    def expect(self, error: Optional[str], context: str) -> None:
        self.count += 1
        if error:
            self.failures.append(f"{context}: {error}")
            if len(self.failures) <= 20:
                print(f"VALIDATION FAILURE {context}: {error}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Scenario runner
# ---------------------------------------------------------------------------
class Bench:
    """Runs one scenario against an artifact service and checks expected state."""

    def __init__(
        self,
        service: Any,
        scenario: Scenario,
        seed: str,
        *,
        uri_scheme: str = "gs",
        phase: str = "main",
    ):
        self.service = service
        self.scenario = scenario
        self.seed = seed
        self.uri_scheme = uri_scheme
        self.phase = phase
        self.emit_lines = True
        self.first_latency_ms: Optional[float] = None
        self.emitted = 0
        self.checks = Checks()
        # Bytes read outside timed calls (probe and final validation).
        self.untimed_read_bytes = 0
        self.user_scopes: Dict[str, Scope] = {}
        self.sessions: List[SessionState] = []

    def new_session(self, user_index: int, session_index: int) -> SessionState:
        user_id = f"u{user_index:03d}"
        user_scope = self.user_scopes.setdefault(user_id, Scope())
        state = SessionState(
            user_id=user_id,
            session_id=f"s{session_index:04d}",
            session_scope=Scope(),
            user_scope=user_scope,
        )
        self.sessions.append(state)
        return state

    def rng(self, *parts: Any) -> random.Random:
        return random.Random("/".join(str(p) for p in (self.seed, *parts)))

    async def think(self, rng: random.Random) -> None:
        if self.scenario.think_time_s > 0:
            await asyncio.sleep(rng.expovariate(1.0 / self.scenario.think_time_s))

    # -- timed service calls with post-call checks ---------------------------
    async def _timed(
        self,
        op: str,
        call: Awaitable[Any],
        *,
        sent_bytes: int = 0,
        loaded_bytes: Optional[Callable[[Any], int]] = None,
    ):
        start = time.perf_counter()
        try:
            result = await call
        except Exception:
            self._record(op, (time.perf_counter() - start) * 1000, 0, ok=False)
            raise
        latency_ms = (time.perf_counter() - start) * 1000
        size = loaded_bytes(result) if loaded_bytes else sent_bytes
        if loaded_bytes and not self.emit_lines:
            self.untimed_read_bytes += size
        self._record(op, latency_ms, size, ok=True)
        return result

    def _record(self, op: str, latency_ms: float, size: int, *, ok: bool) -> None:
        if self.first_latency_ms is None:
            self.first_latency_ms = latency_ms
        if self.emit_lines:
            self.emitted += 1
            emit(
                f"Artifact Op : Seq: {self.phase}:{self.emitted} : Op: {op} : "
                f"Latency: {latency_ms:.3f} ms : Bytes: {size} : "
                f"Ok: {'true' if ok else 'false'}"
            )

    async def save(self, state: SessionState, filename: str, spec: PayloadSpec) -> int:
        part = spec.to_part()
        version = await self._timed(
            "save",
            self.service.save_artifact(
                app_name=APP_NAME,
                user_id=state.user_id,
                session_id=state.session_id,
                filename=filename,
                artifact=part,
                custom_metadata=spec.custom_metadata,
            ),
            sent_bytes=part_size(part),
        )
        state.scope_for(filename).record(filename, version, spec)
        return version

    async def load_latest(self, state: SessionState, filename: str):
        part = await self._timed(
            "load_latest",
            self.service.load_artifact(
                app_name=APP_NAME,
                user_id=state.user_id,
                session_id=state.session_id,
                filename=filename,
            ),
            loaded_bytes=part_size,
        )
        # Concurrent sessions of one user can add user: versions meanwhile.
        if not filename.startswith("user:"):
            spec = state.scope_for(filename).latest(filename)
            error = check_part(part, spec)
            if error is None and spec is not None:
                spec.verified = True
            self.checks.expect(error, f"{state.session_id}/{filename} latest")
        return part

    async def load_version(self, state: SessionState, filename: str, version: int):
        part = await self._timed(
            "load_version",
            self.service.load_artifact(
                app_name=APP_NAME,
                user_id=state.user_id,
                session_id=state.session_id,
                filename=filename,
                version=version,
            ),
            loaded_bytes=part_size,
        )
        spec = state.scope_for(filename).versions.get(filename, {}).get(version)
        error = check_part(part, spec)
        if error is None and spec is not None:
            spec.verified = True
        self.checks.expect(error, f"{state.session_id}/{filename} v{version}")
        return part

    async def list_keys(self, state: SessionState) -> List[str]:
        user_before = state.user_scope.live_keys()
        keys = await self._timed(
            "list_keys",
            self.service.list_artifact_keys(
                app_name=APP_NAME, user_id=state.user_id, session_id=state.session_id
            ),
        )
        listed = set(keys)
        session_keys = {k for k in listed if not k.startswith("user:")}
        user_keys = listed - session_keys
        error = None
        if session_keys != state.session_scope.live_keys():
            error = (
                f"session keys {sorted(session_keys)} != "
                f"{sorted(state.session_scope.live_keys())}"
            )
        elif not user_before <= user_keys:
            error = f"user keys {sorted(user_keys)} miss {sorted(user_before)}"
        self.checks.expect(error, f"{state.session_id} list_keys")
        return keys

    async def list_versions(self, state: SessionState, filename: str) -> List[int]:
        versions = await self._timed(
            "list_versions",
            self.service.list_versions(
                app_name=APP_NAME,
                user_id=state.user_id,
                session_id=state.session_id,
                filename=filename,
            ),
        )
        if not filename.startswith("user:"):
            expected = sorted(state.scope_for(filename).versions.get(filename, {}))
            self.checks.expect(
                None if versions == expected else f"{versions} != {expected}",
                f"{state.session_id}/{filename} list_versions",
            )
        return versions

    async def get_artifact_version(
        self, state: SessionState, filename: str, version: int
    ) -> None:
        info = await self._timed(
            "get_version",
            self.service.get_artifact_version(
                app_name=APP_NAME,
                user_id=state.user_id,
                session_id=state.session_id,
                filename=filename,
                version=version,
            ),
        )
        self.checks.expect(
            (
                None
                if info is not None and info.version == version
                else f"artifact version {info}"
            ),
            f"{state.session_id}/{filename} get_artifact_version v{version}",
        )

    async def delete(self, state: SessionState, filename: str) -> None:
        await self._timed(
            "delete",
            self.service.delete_artifact(
                app_name=APP_NAME,
                user_id=state.user_id,
                session_id=state.session_id,
                filename=filename,
            ),
        )
        state.scope_for(filename).versions[filename] = {}

    async def rewind(self, state: SessionState, snapshot: Dict[str, int]) -> None:
        """Replays ``_rewind_utils``: restore rewind-point versions, tombstone the rest."""
        scope = state.session_scope
        for filename in list(scope.versions):
            current = scope.latest_version(filename)
            target = snapshot.get(filename)
            if current is None or current == target:
                continue
            if target is None:
                await self.save(
                    state, filename, PayloadSpec(seed="", size=0, kind="tombstone")
                )
                continue
            part = await self.load_version(state, filename, target)
            version = await self._timed(
                "save",
                self.service.save_artifact(
                    app_name=APP_NAME,
                    user_id=state.user_id,
                    session_id=state.session_id,
                    filename=filename,
                    artifact=part,
                ),
                sent_bytes=part_size(part),
            )
            rewound_spec = dataclasses.replace(
                scope.versions[filename][target], verified=False
            )
            scope.record(filename, version, rewound_spec)

    # -- scenarios -----------------------------------------------------------
    def spec(
        self,
        rng: random.Random,
        name: str,
        kind: str,
        mime_type: Optional[str],
        median: int,
        low: int,
        high: int,
        display_name: Optional[str] = None,
        custom_metadata: Optional[Dict[str, str]] = None,
    ) -> PayloadSpec:
        return PayloadSpec(
            seed=f"{self.seed}/{name}/{rng.random()}",
            size=lognormal_size(rng, median, low, high),
            kind=kind,
            mime_type=mime_type if kind == "inline" else None,
            display_name=display_name,
            custom_metadata=custom_metadata,
        )

    async def iterative_workspace_session(self, state: SessionState) -> None:
        """brand-aligned-presentations / ambient-quality-agent pattern."""
        rng = self.rng(state.session_id)
        turns = self.scenario.turns
        notes_turn = max(1, int(turns * 0.6))
        rewind_to = max(1, int(turns * 0.5))
        rewind_at = max(rewind_to + 1, int(turns * 0.8))
        files = {
            "outline.md": ("text", None, None, None),
            "deck.json": ("inline", "application/json", None, None),
            "slide.png": ("inline", "image/png", "slide.png", None),
            "report.html": ("inline", "text/html", None, {"source": "adkbench"}),
        }

        def file_spec(name: str) -> PayloadSpec:
            kind, mime_type, display_name, metadata = files[name]
            return self.spec(
                rng,
                name,
                kind,
                mime_type,
                32 * KIB,
                4 * KIB,
                256 * KIB,
                display_name,
                metadata,
            )

        # Turn 0: LoadArtifactsTool lists keys before both model requests.
        await self.list_keys(state)
        await self.think(rng)
        for name in files:
            await self.save(state, name, file_spec(name))
        await self.save(
            state,
            "user:style_guide.md",
            self.spec(rng, "style", "inline", "text/markdown", 8 * KIB, KIB, 64 * KIB),
        )
        await self.list_keys(state)
        await self.think(rng)
        snapshot: Dict[str, int] = {}
        for turn in range(1, turns + 1):
            live = sorted(
                n
                for n in state.session_scope.live_keys()
                if state.session_scope.latest(n).kind != "tombstone"
            )
            # Model request 1: the model asks for two artifacts.
            await self.list_keys(state)
            await self.think(rng)
            # Model request 2: LoadArtifactsTool lists and loads them.
            await self.list_keys(state)
            for name in rng.sample(live, min(2, len(live))):
                await self.load_latest(state, name)
            await self.think(rng)
            for name in rng.sample(sorted(files), rng.randint(1, 2)):
                await self.save(state, name, file_spec(name))
            if turn == notes_turn:
                await self.save(
                    state,
                    "notes.md",
                    self.spec(rng, "notes", "text", None, 4 * KIB, KIB, 16 * KIB),
                )
            if turn % 10 == 0:
                deepest = max(
                    state.session_scope.versions,
                    key=lambda n: state.session_scope.latest_version(n) or 0,
                )
                await self.list_versions(state, deepest)
            # Model request 3: the final answer.
            await self.list_keys(state)
            await self.think(rng)
            if turn == rewind_to:
                snapshot = {
                    name: state.session_scope.latest_version(name)
                    for name in state.session_scope.versions
                }
            if turn == rewind_at:
                await self.rewind(state, snapshot)

    async def genmedia_session(self, state: SessionState) -> None:
        """genmedia-for-commerce / retail-virtual-tryon pattern."""
        rng = self.rng(state.session_id)
        generated = [f"gen_{index}.png" for index in range(4)]
        await self.think(rng)
        for index in range(3):
            name = f"input_{index}.png"
            await self.save(
                state,
                name,
                self.spec(
                    rng, name, "inline", "image/png", 4 * MIB, 2 * MIB, 8 * MIB, name
                ),
            )
        await self.think(rng)
        for name in generated:
            await self.save(
                state,
                name,
                self.spec(
                    rng, name, "inline", "image/png", 10 * MIB, 5 * MIB, 20 * MIB, name
                ),
            )
        for refine in range(3):
            await self.think(rng)
            name = generated[refine % len(generated)]
            await self.load_latest(state, name)
            await self.save(
                state,
                name,
                self.spec(
                    rng, name, "inline", "image/png", 10 * MIB, 5 * MIB, 20 * MIB, name
                ),
            )
        await self.think(rng)
        await self.save(
            state,
            "video.mp4",
            self.spec(
                rng,
                "video",
                "inline",
                "video/mp4",
                75 * MIB,
                50 * MIB,
                100 * MIB,
                "video.mp4",
            ),
        )
        await self.think(rng)
        await self.load_latest(state, "video.mp4")
        for name in generated:
            await self.delete(state, name)

    async def hitl_upload_session(self, state: SessionState) -> None:
        """small-business-loan-agent pattern, phase 1."""
        rng = self.rng(state.session_id)
        count = rng.randint(self.scenario.files_min, self.scenario.files_max)
        for index in range(count):
            name = f"doc_{index:03d}.pdf"
            version = await self.save(
                state,
                name,
                self.spec(
                    rng,
                    name,
                    "inline",
                    "application/pdf",
                    500 * KIB,
                    100 * KIB,
                    20 * MIB,
                    name,
                ),
            )
            # SaveFilesAsArtifactsPlugin (attach_file_reference=True default)
            # reads each new version's canonical URI to attach a reference.
            await self.get_artifact_version(state, name, version)
        await self.think(rng)
        keys = await self.list_keys(state)
        for name in sorted(k for k in keys if k.startswith("doc_")):
            await self.load_latest(state, name)
        await self.think(rng)
        await self.save(
            state,
            "user:profile.json",
            self.spec(
                rng,
                "profile",
                "inline",
                "application/json",
                64 * KIB,
                8 * KIB,
                256 * KIB,
            ),
        )
        await self.save(
            state,
            "summary.json",
            self.spec(
                rng,
                "summary",
                "inline",
                "application/json",
                32 * KIB,
                4 * KIB,
                128 * KIB,
            ),
        )

    async def hitl_resume_session(self, state: SessionState) -> None:
        """Phase 2, in a fresh process: list, read the summary, save a decision."""
        rng = self.rng(state.session_id, "resume")
        await self.list_keys(state)
        await self.load_latest(state, "summary.json")
        await self.think(rng)
        await self.save(
            state,
            "decision.json",
            self.spec(
                rng, "decision", "inline", "application/json", 8 * KIB, KIB, 32 * KIB
            ),
        )

    # -- probe and validation ------------------------------------------------
    async def probe(self) -> Dict[str, float]:
        """Requests per op, measured serially with ``RequestCounter``."""
        from request_counter import RequestCounter

        state = self.new_session(999, 9999)
        self.sessions.remove(state)
        rng = self.rng("probe")

        def small(name: str) -> PayloadSpec:
            return self.spec(
                rng,
                name,
                "inline",
                "application/octet-stream",
                4 * KIB,
                4 * KIB,
                4 * KIB,
            )

        self.emit_lines = False
        try:
            for version in range(3):
                await self.save(
                    state, "probe_existing.bin", small(f"existing{version}")
                )
            for index in range(PROBE_ROUNDS):
                await self.save(
                    state, f"probe_delete_{index}.bin", small(f"del{index}")
                )
            calls = {
                "save": lambda i: self.save(
                    state, f"probe_new_{i}.bin", small(f"new{i}")
                ),
                "load_latest": lambda i: self.load_latest(state, "probe_existing.bin"),
                "load_version": lambda i: self.load_version(
                    state, "probe_existing.bin", 0
                ),
                "list_keys": lambda i: self.list_keys(state),
                "list_versions": lambda i: self.list_versions(
                    state, "probe_existing.bin"
                ),
                "get_version": lambda i: self.get_artifact_version(
                    state, "probe_existing.bin", 0
                ),
                "delete": lambda i: self.delete(state, f"probe_delete_{i}.bin"),
            }
            requests_per_op = {}
            for op in OPS:
                with RequestCounter() as counter:
                    for index in range(PROBE_ROUNDS):
                        await calls[op](index)
                requests_per_op[op] = counter.total / PROBE_ROUNDS
                emit(
                    f"Artifact Probe : Op: {op} : "
                    f"Requests Per Op: {requests_per_op[op]:.3f}"
                )
            return requests_per_op
        finally:
            self.emit_lines = True

    async def validate_final_state(self, sessions: List[SessionState]) -> None:
        """Check version contiguity and read any latest version not yet verified."""
        await asyncio.gather(*(self._validate_session(s) for s in sessions))
        for user_id, scope in self.user_scopes.items():
            for filename, saved in scope.versions.items():
                versions = await self.service.list_versions(
                    app_name=APP_NAME, user_id=user_id, filename=filename
                )
                expected = sorted(saved)
                self.checks.expect(
                    None if versions == expected else f"{versions} != {expected}",
                    f"{user_id}/{filename} final versions",
                )

    async def _validate_session(self, state: SessionState) -> None:
        scope = state.session_scope
        for filename, saved in scope.versions.items():
            expected = sorted(saved)
            versions = await self.service.list_versions(
                app_name=APP_NAME,
                user_id=state.user_id,
                session_id=state.session_id,
                filename=filename,
            )
            self.checks.expect(
                None if versions == expected else f"{versions} != {expected}",
                f"{state.session_id}/{filename} final versions",
            )
            if expected != list(range(len(expected))):
                self.checks.expect(
                    f"versions {expected} are not contiguous",
                    f"{state.session_id}/{filename}",
                )
            latest = scope.latest(filename)
            if not saved or (latest is not None and latest.verified):
                continue
            part = await self.service.load_artifact(
                app_name=APP_NAME,
                user_id=state.user_id,
                session_id=state.session_id,
                filename=filename,
            )
            self.untimed_read_bytes += part_size(part)
            self.checks.expect(
                check_part(part, latest),
                f"{state.session_id}/{filename} final latest",
            )

    async def validate_metadata_and_concurrency(self) -> None:
        """Check custom_metadata persistence and K-way concurrent exclusive creates."""
        state = self.new_session(998, 9998)
        self.sessions.remove(state)
        session = {
            "app_name": APP_NAME,
            "user_id": state.user_id,
            "session_id": state.session_id,
        }
        target = PayloadSpec(
            seed=f"{self.seed}/target",
            size=2048,
            kind="inline",
            mime_type="image/png",
            display_name="target.png",
        )
        await self.service.save_artifact(
            filename="target.png",
            artifact=target.to_part(),
            custom_metadata={"owner": "adkbench", "stage": "final"},
            **session,
        )
        version_info = await self.service.get_artifact_version(
            filename="target.png", **session
        )
        self.checks.expect(
            (
                None
                if version_info is not None
                and version_info.custom_metadata
                == {"owner": "adkbench", "stage": "final"}
                and version_info.mime_type == "image/png"
                and str(version_info.canonical_uri).startswith(f"{self.uri_scheme}://")
                else f"artifact version {version_info}"
            ),
            "custom_metadata and ArtifactVersion",
        )
        specs = [
            PayloadSpec(
                seed=f"{self.seed}/race{index}",
                size=1024,
                kind="inline",
                mime_type="application/octet-stream",
            )
            for index in range(CONCURRENT_SAVES)
        ]
        versions = await asyncio.gather(
            *(
                self.service.save_artifact(
                    filename="race.bin", artifact=spec.to_part(), **session
                )
                for spec in specs
            )
        )
        self.checks.expect(
            (
                None
                if sorted(versions) == list(range(CONCURRENT_SAVES))
                else f"concurrent save versions {sorted(versions)}"
            ),
            "concurrent saves",
        )
        for version, spec in zip(versions, specs):
            part = await self.service.load_artifact(
                filename="race.bin", version=version, **session
            )
            self.untimed_read_bytes += part_size(part)
            self.checks.expect(check_part(part, spec), f"race.bin v{version}")
        await self.service.delete_artifact(filename="race.bin", **session)
        remaining = await self.service.list_versions(filename="race.bin", **session)
        self.checks.expect(
            None if remaining == [] else f"versions after delete {remaining}",
            "delete completeness",
        )


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------
def build_service(base_url: str):
    from google.adk.integrations.fsspec import FsspecArtifactService

    return FsspecArtifactService(base_url)


def state_to_json(bench: Bench) -> Dict[str, Any]:
    def scope_json(scope: Scope) -> Dict[str, Any]:
        return {
            name: {str(v): dataclasses.asdict(s) for v, s in saved.items()}
            for name, saved in scope.versions.items()
        }

    return {
        "users": {uid: scope_json(s) for uid, s in bench.user_scopes.items()},
        "sessions": [
            {
                "user_id": state.user_id,
                "session_id": state.session_id,
                "scope": scope_json(state.session_scope),
            }
            for state in bench.sessions
        ],
    }


def state_from_json(bench: Bench, data: Dict[str, Any]) -> None:
    def load_scope(raw: Dict[str, Any]) -> Scope:
        return Scope(
            versions={
                name: {int(v): PayloadSpec(**spec) for v, spec in saved.items()}
                for name, saved in raw.items()
            }
        )

    bench.user_scopes = {uid: load_scope(raw) for uid, raw in data["users"].items()}
    bench.sessions = [
        SessionState(
            user_id=raw["user_id"],
            session_id=raw["session_id"],
            session_scope=load_scope(raw["scope"]),
            user_scope=bench.user_scopes.setdefault(raw["user_id"], Scope()),
        )
        for raw in data["sessions"]
    ]


async def run_scenario(args, scenario: Scenario) -> Dict[str, Any]:
    """Runs the measured scenario and its checks. Returns the result summary."""
    base_url = f"{args.base_url.rstrip('/')}/{args.scenario}-{args.run_tag}"
    scheme = base_url.split("://", 1)[0]
    bench = Bench(
        build_service(base_url),
        scenario,
        seed=args.run_tag,
        uri_scheme=scheme,
        phase=args.phase,
    )
    result: Dict[str, Any] = {"cold_first_op_ms": None}

    if args.phase == "resume":
        with open(args.manifest) as fh:
            state_from_json(bench, json.load(fh))
        sessions = list(bench.sessions)
        # The first call of a fresh process and service instance is the cold op.
        await bench.hitl_resume_session(sessions[0])
        result["cold_first_op_ms"] = bench.first_latency_ms
        await asyncio.gather(*(bench.hitl_resume_session(s) for s in sessions[1:]))
        await bench.validate_final_state(sessions)
        result["checks"], result["failures"] = bench.checks.count, bench.checks.failures
        result["ops"] = bench.emitted
        result["untimed_read_bytes"] = bench.untimed_read_bytes
        return result

    await bench.probe()
    bench.first_latency_ms = None
    result["window_start"] = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    start = time.perf_counter()
    if args.scenario == "iterative-workspace":
        states = [bench.new_session(i, i) for i in range(scenario.sessions)]
        await asyncio.gather(*(bench.iterative_workspace_session(s) for s in states))
    elif args.scenario == "genmedia-pipeline":
        states = [bench.new_session(i, i) for i in range(scenario.sessions)]
        await asyncio.gather(*(bench.genmedia_session(s) for s in states))
    else:
        states = [
            bench.new_session(user, user * scenario.sessions + index)
            for user in range(scenario.users)
            for index in range(scenario.sessions)
        ]
        await asyncio.gather(*(bench.hitl_upload_session(s) for s in states))
        resume = await run_resume_process(args, bench)
        result["cold_first_op_ms"] = resume.get("cold_first_op_ms")
        result["resume_ops"] = resume.get("ops", 0)
        bench.untimed_read_bytes += resume.get("untimed_read_bytes", 0)
        bench.checks.count += resume.get("checks", 0)
        bench.checks.failures += resume.get("failures", ["resume process failed"])
    result["total_s"] = time.perf_counter() - start
    result["window_end"] = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    if args.scenario != "hitl-handoff":
        await bench.validate_final_state(states)
    await bench.validate_metadata_and_concurrency()
    result["checks"], result["failures"] = bench.checks.count, bench.checks.failures
    result["ops"] = bench.emitted + result.get("resume_ops", 0)
    result["untimed_read_bytes"] = bench.untimed_read_bytes
    return result


async def run_resume_process(args, bench: Bench) -> Dict[str, Any]:
    """Runs the HITL resume phase in a fresh Python process."""
    with tempfile.TemporaryDirectory() as tmp:
        manifest = os.path.join(tmp, "manifest.json")
        output = os.path.join(tmp, "result.json")
        with open(manifest, "w") as fh:
            json.dump(state_to_json(bench), fh)
        command = [
            sys.executable,
            os.path.abspath(__file__),
            "--scenario",
            args.scenario,
            "--base-url",
            args.base_url,
            "--run-tag",
            args.run_tag,
            "--phase",
            "resume",
            "--manifest",
            manifest,
            "--result-file",
            output,
        ]
        process = await asyncio.create_subprocess_exec(*command)
        returncode = await process.wait()
        if not os.path.exists(output):
            return {
                "failures": [f"resume process exited {returncode} without a result"]
            }
        with open(output) as fh:
            return json.load(fh)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    parser.add_argument("--base-url", required=True, help="gs://bucket/prefix")
    parser.add_argument("--run-tag", default=uuid.uuid4().hex[:8])
    parser.add_argument("--phase", default="main", choices=["main", "resume"])
    parser.add_argument("--manifest", help=argparse.SUPPRESS)
    parser.add_argument("--result-file", help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    scenario = SCENARIOS[args.scenario]
    result = asyncio.run(run_scenario(args, scenario))
    if args.phase == "resume":
        with open(args.result_file, "w") as fh:
            json.dump(result, fh)
        return 0
    sessions = scenario.sessions * max(scenario.users, 1)
    cold = result["cold_first_op_ms"]
    emit(
        f"Artifact Workload : Start: {result['window_start']}"
        f" : End: {result['window_end']}"
        f" : Total: {result['total_s']:.3f} s"
        f" : Cold First Op: {'N/A' if cold is None else f'{cold:.3f}'} ms"
        f" : Sessions: {sessions} : Turns: {scenario.turns}"
        f" : Ops: {result['ops']}"
        f" : Untimed Read Bytes: {result['untimed_read_bytes']}"
        f" : Checks: {result['checks']}"
        f" : Failures: {len(result['failures'])}"
    )
    return 1 if result["failures"] else 0


if __name__ == "__main__":
    sys.exit(main())
