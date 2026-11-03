"""Unit tests for gcsfs.iotracer diagnostic subsystem.

Follows Arrange-Act-Assert (AAA) pattern, testing:
- ProcessEventLogger buffering, coalescing, and truncation limits.
- Logical read and wire request monkey-patch interception with reentrancy protection.
- Fork/spawn multi-process logging without deadlock or dropped wire events.
- Visualizer HTML dashboard generation and Chrome Trace Event format export.
- CLI argument parsing and execution.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from gcsfs.iotracer import (
    DEFAULT_BUFFER_SIZE,
    DEFAULT_MAX_EVENTS,
    DEFAULT_MAX_SIZE_MB,
    install_hooks_in_process,
    is_tracing_enabled,
    trace_io,
)
from gcsfs.iotracer.tracer import _LOGGER, _ProcessEventLogger
from gcsfs.iotracer.visualizer import (
    decimate_events_for_canvas,
    export_chrome_trace,
    render_html_dashboard,
    render_reports,
)
from gcsfs.cli.iotracer import main as cli_main


class TestProcessEventLogger(unittest.TestCase):
    """Verify event buffering, cache-hit coalescing, and truncation sentinels."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="test_logger_")
        os.environ["GCSFS_IO_TRACE_DIR"] = self.test_dir
        os.environ["GCSFS_IO_TRACE_T0"] = f"{time.monotonic():.6f}"

    def tearDown(self):
        if hasattr(self, "logger") and self.logger:
            self.logger.close()
        os.environ.pop("GCSFS_IO_TRACE_DIR", None)
        os.environ.pop("GCSFS_IO_TRACE_T0", None)
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_logger_writes_jsonl(self):
        # Arrange
        self.logger = _ProcessEventLogger()
        self.logger.configure(max_events=100, max_size_mb=1)

        # Act
        self.logger.log(
            layer="logical",
            op="read",
            file="bucket/data.bin",
            offset=0,
            length=1024,
            t_start=time.monotonic(),
            t_end=time.monotonic() + 0.001,
            hit_state="miss",
        )
        self.logger.flush()

        # Assert
        log_files = list(Path(self.test_dir).glob("events_*.jsonl"))
        self.assertEqual(len(log_files), 1)
        lines = log_files[0].read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 1)
        record = json.loads(lines[0])
        self.assertEqual(record["layer"], "logical")
        self.assertEqual(record["op"], "read")
        self.assertEqual(record["path"], "bucket/data.bin")
        self.assertEqual(record["off"], 0)
        self.assertEqual(record["len"], 1024)
        self.assertEqual(record["hit_state"], "miss")

    def test_cache_hit_coalescing(self):
        # Arrange
        self.logger = _ProcessEventLogger()
        self.logger.configure(max_events=100, max_size_mb=1)
        now = time.monotonic()

        # Act: Write 5 contiguous sequential cache hits within 2ms window
        for i in range(5):
            self.logger.log(
                layer="logical",
                op="read",
                file="bucket/stream.bin",
                offset=i * 100,
                length=100,
                t_start=now + (i * 0.0001),
                t_end=now + ((i + 1) * 0.0001),
                hit_state="full",
            )
        self.logger.flush()

        # Assert: All 5 reads should be coalesced into a single record with len=500 and cnt=5
        log_files = list(Path(self.test_dir).glob("events_*.jsonl"))
        self.assertEqual(len(log_files), 1)
        lines = log_files[0].read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 1)
        record = json.loads(lines[0])
        self.assertEqual(record["len"], 500)
        self.assertEqual(record["cnt"], 5)
        self.assertEqual(record["hit_state"], "full")

    def test_max_events_truncation_sentinel(self):
        # Arrange
        self.logger = _ProcessEventLogger()
        self.logger.configure(max_events=3, max_size_mb=10)

        # Act: Log 5 events when limit is 3
        now = time.monotonic()
        for i in range(5):
            self.logger.log(
                layer="wire",
                op="fetch_range",
                file=f"bucket/file_{i}.bin",
                offset=0,
                length=1024,
                t_start=now + i,
                t_end=now + i + 0.1,
            )
        self.logger.flush()

        # Assert: 3 events + 1 truncation sentinel record
        log_files = list(Path(self.test_dir).glob("events_*.jsonl"))
        self.assertEqual(len(log_files), 1)
        lines = log_files[0].read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 4)

        sentinel = json.loads(lines[-1])
        self.assertEqual(sentinel["op"], "trace_truncated")
        self.assertEqual(sentinel["reason"], "max_events")


class TestRuntimeInterception(unittest.TestCase):
    """Verify monkey patching, reentrancy guards, and context manager."""

    def test_reentrancy_guard_prevents_duplicate_logical_event(self):
        # Arrange
        install_hooks_in_process()
        test_dir = tempfile.mkdtemp(prefix="test_reentrant_")
        out_html = Path(test_dir) / "trace.html"

        # Act
        with trace_io(output_html=out_html, trace_dir=test_dir):
            from fsspec.spec import AbstractBufferedFile

            mock_fs = MagicMock()
            mock_fs.size.return_value = 10000

            class DummyFile(AbstractBufferedFile):
                def _fetch_range(self, start, end):
                    return b"hello world\nsecond line\n"[start:end]

            f = DummyFile(mock_fs, "bucket/test.txt", mode="rb", size=10000, cache_type="bytes")
            # readline calls read internally
            f.readline()
            f.close()

        # Assert
        summary_file = out_html.with_suffix(".summary.json")
        self.assertTrue(summary_file.is_file())
        summary = json.loads(summary_file.read_text(encoding="utf-8"))
        # Should record readline without duplicate sub-read calls
        self.assertEqual(summary["logical_calls"], 1)
        shutil.rmtree(test_dir, ignore_errors=True)


def _worker_process_task(trace_dir: str):
    """Helper for multiprocessing test that terminates via standard exit."""
    os.environ["GCSFS_IO_TRACE_DIR"] = trace_dir
    os.environ["GCSFS_IO_TRACE_T0"] = f"{time.monotonic():.6f}"
    logger = _ProcessEventLogger()
    now = time.monotonic()
    logger.log(
        layer="wire",
        op="cat_file_sequential",
        file="bucket/multi_proc.bin",
        offset=0,
        length=4096,
        t_start=now,
        t_end=now + 0.05,
        wire_origin="direct_miss",
    )
    logger.flush()


class TestMultiprocessingSafety(unittest.TestCase):
    """Verify trace collection across forked/spawned processes."""

    def test_multi_process_logging_and_report_aggregation(self):
        # Arrange
        test_dir = tempfile.mkdtemp(prefix="test_mp_")
        ctx = mp.get_context("spawn")

        # Act: Launch 2 worker processes
        workers = [
            ctx.Process(target=_worker_process_task, args=(test_dir,))
            for _ in range(2)
        ]
        for w in workers:
            w.start()
        for w in workers:
            w.join()

        out_html = Path(test_dir) / "multi_trace.html"
        out_trace = Path(test_dir) / "trace.json"
        render_reports(
            test_dir,
            output_html=out_html,
            export_trace=out_trace,
            title="Multiprocessing Test",
        )

        # Assert
        self.assertTrue(out_html.is_file())
        self.assertTrue(out_trace.is_file())

        summary_file = out_html.with_suffix(".summary.json")
        self.assertTrue(summary_file.is_file())
        summary = json.loads(summary_file.read_text(encoding="utf-8"))
        self.assertEqual(summary["wire_calls"], 2)
        self.assertEqual(summary["wire_bytes"], 8192)
        self.assertEqual(summary["active_pids"], 2)

        # Assert Chrome Trace Event format validity
        trace_data = json.loads(out_trace.read_text(encoding="utf-8"))
        self.assertIn("traceEvents", trace_data)
        event_names = [ev["name"] for ev in trace_data["traceEvents"] if ev.get("ph") == "X"]
        self.assertEqual(len(event_names), 2)

        shutil.rmtree(test_dir, ignore_errors=True)


class TestVisualizerAndDecimation(unittest.TestCase):
    """Verify Level-of-Detail downsampling and HTML report generation."""

    def test_decimate_events_preserves_wire_and_bounds_points(self):
        # Arrange: 30,000 events (100 wire, 29,900 logical)
        events = []
        now = time.monotonic()
        for i in range(100):
            events.append({
                "t": now + i * 0.01,
                "dur": 0.005,
                "layer": "wire",
                "op": "fetch_range",
                "path": "file1.bin",
                "off": i * 1000,
                "len": 1000,
                "pid": 101,
            })
        for i in range(29_900):
            events.append({
                "t": now + i * 0.001,
                "dur": 0.0001,
                "layer": "logical",
                "op": "read",
                "path": "file1.bin",
                "off": i * 10,
                "len": 10,
                "pid": 101,
                "hit": (i % 2 == 0),
            })

        # Act
        decimated = decimate_events_for_canvas(events, max_points=20_000)

        # Assert
        self.assertLessEqual(len(decimated), 20_000)
        wire_decimated = [e for e in decimated if e["layer"] == "wire"]
        # All 100 wire events must be retained
        self.assertEqual(len(wire_decimated), 100)

    def test_export_chrome_trace_structure(self):
        # Arrange
        events = [
            {
                "t": 0.1,
                "dur": 0.05,
                "layer": "wire",
                "op": "cat_file_sequential",
                "path": "bucket/obj.parquet",
                "off": 100,
                "len": 500,
                "pid": 42,
                "tid": 10,
                "req_id": "test-req-123",
            }
        ]
        tmp_json = Path(tempfile.mktemp(suffix=".json"))

        # Act
        export_chrome_trace(events, tmp_json)

        # Assert
        self.assertTrue(tmp_json.is_file())
        data = json.loads(tmp_json.read_text(encoding="utf-8"))
        self.assertIn("traceEvents", data)
        item = next(e for e in data["traceEvents"] if e.get("ph") == "X")
        self.assertEqual(item["cat"], "gcsfs.wire")
        self.assertEqual(item["ts"], 100_000)  # 0.1s -> 100,000 us
        self.assertEqual(item["dur"], 50_000)  # 0.05s -> 50,000 us
        self.assertEqual(item["args"]["req_id"], "test-req-123")
        tmp_json.unlink(missing_ok=True)


class TestCLICommands(unittest.TestCase):
    """Verify CLI entry point command handling."""

    def test_cli_report_generation(self):
        # Arrange: create a mock run directory with 1 event file
        test_dir = tempfile.mkdtemp(prefix="test_cli_")
        run_dir = Path(test_dir) / "run_logs"
        run_dir.mkdir(parents=True)
        log_file = run_dir / "events_123_456.jsonl"
        ev = {
            "t": 0.01,
            "dur": 0.002,
            "layer": "wire",
            "op": "fetch_range",
            "path": "bucket/test.bin",
            "off": 0,
            "len": 2048,
            "pid": 123,
            "tid": 1,
        }
        log_file.write_text(json.dumps(ev) + "\n", encoding="utf-8")
        out_html = Path(test_dir) / "report.html"

        # Act
        exit_code = cli_main(["report", str(run_dir), "--output-html", str(out_html)])

        # Assert
        self.assertEqual(exit_code, 0)
        self.assertTrue(out_html.is_file())
        shutil.rmtree(test_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
