"""Visualizer and offline analytics engine for gcsfs.iotracer."""

from __future__ import annotations

import contextlib
import glob
import json
import os
from pathlib import Path
from typing import Any

# Maximum number of scatter plot points rendered in Canvas to prevent browser freezes.
MAX_CANVAS_POINTS = 20_000


def _short_filename(path: str) -> str:
    parts = path.rstrip("/").split("/")
    if len(parts) >= 2:
        return "/".join(parts[-2:])
    return parts[-1] if parts else path


def _normalize_event(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize raw event dictionary between standard schema and prototype schema."""
    t = float(raw.get("t", raw.get("ts", 0.0)))
    dur = float(raw.get("dur", 0.0))
    if "te" in raw and "dur" not in raw:
        dur = max(0.0, float(raw["te"]) - t)
    te = round(t + dur, 6)

    path = str(raw.get("path", raw.get("file", "unknown")))
    off = int(raw.get("off", 0))
    length = int(raw.get("len", 0))
    layer = str(raw.get("layer", "logical"))
    op = str(raw.get("op", "read"))
    pid = int(raw.get("pid", 0))
    ppid = int(raw.get("ppid", 0))
    tid = int(raw.get("tid", 0))
    tname = str(raw.get("tname", ""))
    cnt = int(raw.get("cnt", 1))

    # Cache hit resolution
    hit_state = raw.get("hit_state")
    if hit_state is None:
        if raw.get("hit") is True:
            hit_state = "full"
        elif raw.get("hit") is False:
            hit_state = "miss"
        else:
            hit_state = "full" if raw.get("cached_len", 0) >= length and length > 0 else "miss"

    is_hit = hit_state in ("full", "partial") or bool(raw.get("hit", False))

    norm: dict[str, Any] = {
        "t": t,
        "ts": t,
        "dur": dur,
        "te": te,
        "path": path,
        "file": path,
        "off": off,
        "len": length,
        "layer": layer,
        "op": op,
        "pid": pid,
        "ppid": ppid,
        "tid": tid,
        "tname": tname,
        "cnt": cnt,
        "hit_state": hit_state,
        "hit": is_hit,
        "cached_len": int(raw.get("cached_len", length if is_hit else 0)),
        "fsize": int(raw.get("fsize", 0)),
    }
    if "wire_origin" in raw:
        norm["wire_origin"] = raw["wire_origin"]
    if "transport" in raw:
        norm["transport"] = raw["transport"]
    if "req_id" in raw:
        norm["req_id"] = raw["req_id"]
    return norm


def decimate_events_for_canvas(events: list[dict[str, Any]], max_points: int = MAX_CANVAS_POINTS) -> list[dict[str, Any]]:
    """Downsample events for Canvas rendering while preserving wire fetches and outlier bounds."""
    if len(events) <= max_points:
        return events

    # Separate wire vs logical events
    wire_events = [e for e in events if e.get("layer") == "wire"]
    logical_events = [e for e in events if e.get("layer") == "logical"]

    # Always keep all wire events (they represent actual network RPCs and are usually low count)
    budget_for_logical = max(1000, max_points - len(wire_events))
    if len(logical_events) <= budget_for_logical:
        combined = wire_events + logical_events
        combined.sort(key=lambda e: (e.get("t", 0.0), 0 if e.get("layer") == "wire" else 1))
        return combined

    # Stratified decimation of logical events across files and cache hit states
    # Downsample uniformly by stride, but preserve cache misses over cache hits
    misses = [e for e in logical_events if not e.get("hit")]
    hits = [e for e in logical_events if e.get("hit")]

    retained_logical: list[dict[str, Any]] = []
    if len(misses) <= budget_for_logical // 2:
        retained_logical.extend(misses)
        hit_budget = budget_for_logical - len(misses)
        step = max(1, len(hits) // hit_budget)
        retained_logical.extend(hits[::step][:hit_budget])
    else:
        # Sample proportionally
        ratio = budget_for_logical / float(len(logical_events))
        step_miss = max(1, int(1.0 / ratio))
        step_hit = max(1, int(1.0 / ratio))
        retained_logical.extend(misses[::step_miss])
        retained_logical.extend(hits[::step_hit])

    combined = wire_events + retained_logical[:budget_for_logical]
    combined.sort(key=lambda e: (e.get("t", 0.0), 0 if e.get("layer") == "wire" else 1))
    return combined


def export_chrome_trace(
    events: list[dict[str, Any]],
    output_json: str | Path,
) -> Path:
    """Export complete microsecond-resolution event history in Chrome Trace Event format (Perfetto)."""
    out_path = Path(output_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    trace_events: list[dict[str, Any]] = []

    # Process and thread metadata
    pids_seen = set()
    threads_seen = set()

    for ev in events:
        pid = ev.get("pid", 0)
        tid = ev.get("tid", 0)
        if pid not in pids_seen:
            pids_seen.add(pid)
            trace_events.append({
                "name": "process_name",
                "ph": "M",
                "pid": pid,
                "tid": 0,
                "args": {"name": f"Process {pid}"},
            })
        if (pid, tid) not in threads_seen:
            threads_seen.add((pid, tid))
            tname = ev.get("tname") or f"Thread {tid}"
            trace_events.append({
                "name": "thread_name",
                "ph": "M",
                "pid": pid,
                "tid": tid,
                "args": {"name": tname},
            })

        # Event span: Complete event 'X'
        ts_us = int(float(ev.get("t", 0.0)) * 1_000_000)
        dur_us = max(1, int(float(ev.get("dur", 0.0)) * 1_000_000))
        layer = ev.get("layer", "logical")
        op = ev.get("op", "read")
        path = _short_filename(ev.get("path", ev.get("file", "unknown")))

        cat = "gcsfs.wire" if layer == "wire" else "gcsfs.logical"
        name = f"Wire {op} [{path}]" if layer == "wire" else f"Read {op} [{path}]"

        args = {
            "path": ev.get("path", ev.get("file")),
            "offset": ev.get("off", 0),
            "length": ev.get("len", 0),
            "layer": layer,
            "op": op,
        }
        if layer == "logical":
            args["hit_state"] = ev.get("hit_state")
            args["cached_len"] = ev.get("cached_len", 0)
        elif layer == "wire":
            args["wire_origin"] = ev.get("wire_origin", "direct_miss")
            args["transport"] = ev.get("transport", "http")
            if "req_id" in ev:
                args["req_id"] = ev["req_id"]

        trace_events.append({
            "name": name,
            "cat": cat,
            "ph": "X",
            "ts": ts_us,
            "dur": dur_us,
            "pid": pid,
            "tid": tid,
            "args": args,
        })

    payload = {"traceEvents": trace_events, "displayTimeUnit": "ms"}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
    return out_path


def render_html_dashboard(
    events: list[dict[str, Any]],
    *,
    output_html: str | Path,
    title: str = "Workload I/O Pattern Trace",
    main_pid: int | None = None,
) -> Path:
    """Compile trace events into a self-contained interactive HTML5 Canvas dashboard."""
    out_path = Path(output_html)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    normalized_events = [_normalize_event(e) for e in events]
    file_sizes: dict[str, int] = {}
    for ev in normalized_events:
        f = ev.get("file", "unknown")
        end_byte = int(ev.get("off", 0)) + int(ev.get("len", 0))
        fsize = max(int(ev.get("fsize", 0)), end_byte)
        if f not in file_sizes or fsize > file_sizes[f]:
            file_sizes[f] = fsize

    files_meta = [
        {"path": f, "short": _short_filename(f), "size": max(1, sz)}
        for f, sz in sorted(file_sizes.items())
    ]

    pids_seen: list[int] = []
    pid_to_ppid: dict[int, int] = {}
    for ev in normalized_events:
        p = int(ev.get("pid", 0))
        if p not in pids_seen:
            pids_seen.append(p)
        if "ppid" in ev and ev["ppid"]:
            pid_to_ppid[p] = int(ev["ppid"])
    if main_pid and main_pid in pids_seen:
        pids_seen.remove(main_pid)
        pids_seen.insert(0, main_pid)

    traced_set = set(pids_seen)
    rank_pids: list[int] = []
    for p in pids_seen:
        if p == main_pid:
            continue
        ppid = pid_to_ppid.get(p, 0)
        if ppid not in traced_set or ppid == main_pid:
            rank_pids.append(p)

    rank_idx_map = {rp: idx for idx, rp in enumerate(rank_pids)}
    rank_worker_counts: dict[int, int] = {}
    pids_meta = []
    generic_idx = 0
    for p in pids_seen:
        if main_pid is not None and p == main_pid:
            role = "Main"
        elif p in rank_idx_map:
            role = (
                f"Rank {rank_idx_map[p]}"
                if len(rank_pids) > 1
                else f"Worker {generic_idx}"
            )
            generic_idx += 1
        else:
            ppid = pid_to_ppid.get(p, 0)
            if ppid in rank_idx_map:
                r_idx = rank_idx_map[ppid]
                w_idx = rank_worker_counts.get(ppid, 0)
                rank_worker_counts[ppid] = w_idx + 1
                role = f"R{r_idx}-W{w_idx}"
            else:
                role = f"Worker {generic_idx}"
                generic_idx += 1
        pids_meta.append({"pid": p, "role": role})

    # Summary metrics calculation
    logical_bytes = sum(int(e.get("len", 0)) for e in normalized_events if e.get("layer") == "logical")
    logical_calls = sum(int(e.get("cnt", 1)) for e in normalized_events if e.get("layer") == "logical")
    logical_hits = sum(
        int(e.get("cnt", 1))
        for e in normalized_events
        if e.get("layer") == "logical" and e.get("hit")
    )
    wire_bytes = sum(int(e.get("len", 0)) for e in normalized_events if e.get("layer") == "wire")
    wire_calls = sum(int(e.get("cnt", 1)) for e in normalized_events if e.get("layer") == "wire")
    max_t = max((float(e.get("te", 0.0)) for e in normalized_events), default=0.0)

    summary_path = out_path.with_suffix(".summary.json")
    with contextlib.suppress(Exception):
        summary_path.write_text(
            json.dumps(
                {
                    "title": title,
                    "duration_s": round(max_t, 4),
                    "total_events": len(normalized_events),
                    "active_pids": len(pids_meta),
                    "pids": pids_meta,
                    "files_touched": len(files_meta),
                    "total_file_bytes": sum(f["size"] for f in files_meta),
                    "logical_calls": logical_calls,
                    "logical_bytes": logical_bytes,
                    "logical_hits": logical_hits,
                    "cache_hit_pct": round(100.0 * logical_hits / logical_calls, 2)
                    if logical_calls
                    else 0.0,
                    "wire_calls": wire_calls,
                    "wire_bytes": wire_bytes,
                    "read_amplification": round(wire_bytes / logical_bytes, 4)
                    if logical_bytes
                    else 0.0,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    # Apply visual Level-of-Detail downsampling for Canvas rendering
    decimated_events = decimate_events_for_canvas(normalized_events, max_points=MAX_CANVAS_POINTS)

    payload_json = json.dumps(
        {
            "title": title,
            "main_pid": main_pid,
            "files": files_meta,
            "pids": pids_meta,
            "events": decimated_events,
            "total_events": len(normalized_events),
            "decimated": len(decimated_events) < len(normalized_events),
        },
        separators=(",", ":"),
    )

    from .template import _HTML_TEMPLATE
    html = _HTML_TEMPLATE.replace("__TRACE_PAYLOAD_JSON__", payload_json).replace(
        "__TRACE_TITLE__", title
    )
    out_path.write_text(html, encoding="utf-8")
    return out_path


def render_reports(
    run_dir: str | Path,
    *,
    output_html: str | Path | None = "io_trace.html",
    export_trace: str | Path | None = None,
    title: str = "Workload I/O Pattern Trace",
    main_pid: int | None = None,
) -> tuple[Path | None, Path | None]:
    """Aggregate per-process event logs in run_dir and generate reports."""
    run_path = Path(run_dir)
    events: list[dict[str, Any]] = []

    for path in sorted(glob.glob(str(run_path / "events_*.jsonl"))):
        with contextlib.suppress(Exception):
            with open(path, "r", encoding="utf-8") as fp:
                for line in fp:
                    line = line.strip()
                    if line:
                        events.append(json.loads(line))

    events.sort(key=lambda e: (float(e.get("t", e.get("ts", 0.0))), 0 if e.get("layer") == "wire" else 1))

    html_out: Path | None = None
    trace_out: Path | None = None

    if output_html:
        html_out = render_html_dashboard(
            events,
            output_html=output_html,
            title=title,
            main_pid=main_pid,
        )

    if export_trace:
        trace_out = export_chrome_trace(
            events,
            output_json=export_trace,
        )

    return html_out, trace_out
