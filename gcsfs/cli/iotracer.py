"""CLI Runner and Report Generator for gcsfs.iotracer."""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Sequence


def _get_default_trace_dir() -> Path:
    target_base = Path("/tmp")
    try:
        usage = shutil.disk_usage(target_base)
        # If /tmp has less than 2 GiB free space, fall back to ./.gcsfs_traces
        if usage.free < 2 * 1024 * 1024 * 1024:
            target_base = Path("./.gcsfs_traces")
            target_base.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return Path(tempfile.mkdtemp(prefix="gcsfs_iotrace_", dir=str(target_base)))


def run_command(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="gcsfs-iotrace run",
        description="Run a target Python script with transparent gcsfs I/O tracing.",
    )
    parser.add_argument(
        "--output-html",
        "-o",
        default="io_trace.html",
        help="Path for generated interactive HTML dashboard (default: io_trace.html). Set empty to disable.",
    )
    parser.add_argument(
        "--export-trace",
        default=None,
        help="Path for exported Chrome Trace Event JSON (Perfetto) format (e.g. trace.json).",
    )
    parser.add_argument(
        "--trace-dir",
        default=None,
        help="Custom directory to store per-process JSONL trace logs.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=500_000,
        help="Maximum events per process before truncation (default: 500,000).",
    )
    parser.add_argument(
        "--max-size-mb",
        type=int,
        default=100,
        help="Maximum trace file size in MB per process (default: 100 MB).",
    )
    parser.add_argument(
        "--title",
        default="Workload I/O Pattern Trace",
        help="Title displayed in the generated HTML dashboard.",
    )
    parser.add_argument(
        "target",
        help="Target Python script to execute.",
    )
    parser.add_argument(
        "target_args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded directly to the target script.",
    )

    args = parser.parse_args(argv)

    if args.trace_dir:
        run_dir = Path(args.trace_dir)
        run_dir.mkdir(parents=True, exist_ok=True)
        cleanup_dir = False
    else:
        run_dir = _get_default_trace_dir()
        cleanup_dir = False  # Keep raw run_dir if user wants post-mortem inspection or reports

    bootstrap_dir = run_dir / "_bootstrap"
    bootstrap_dir.mkdir(parents=True, exist_ok=True)

    # Write sitecustomize.py to ensure child processes (fork/spawn/torch) auto-install hooks
    sitecustomize_code = f"""import os, sys
try:
    import gcsfs.iotracer
    gcsfs.iotracer.install_hooks_in_process()
except Exception:
    pass
"""
    (bootstrap_dir / "sitecustomize.py").write_text(sitecustomize_code, encoding="utf-8")

    t0 = time.monotonic()
    os.environ["GCSFS_IO_TRACE"] = "1"
    os.environ["GCSFS_IO_TRACE_DIR"] = str(run_dir)
    os.environ["GCSFS_IO_TRACE_T0"] = f"{t0:.6f}"
    os.environ["GCSFS_IO_TRACE_MAX_EVENTS"] = str(args.max_events)
    os.environ["GCSFS_IO_TRACE_MAX_SIZE_MB"] = str(args.max_size_mb)

    existing_pp = os.environ.get("PYTHONPATH", "")
    os.environ["PYTHONPATH"] = f"{bootstrap_dir}:{existing_pp}" if existing_pp else str(bootstrap_dir)

    target_script = os.path.abspath(args.target)
    forwarded_argv = [target_script] + args.target_args

    sys.argv = forwarded_argv

    exit_code = 0
    try:
        # Pre-install in the main process before loading target script
        from gcsfs.iotracer.tracer import install_hooks_in_process, _LOGGER
        _LOGGER.configure(max_events=args.max_events, max_size_mb=args.max_size_mb)
        install_hooks_in_process()

        import runpy
        runpy.run_path(target_script, run_name="__main__")
    except SystemExit as e:
        exit_code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
    except Exception as e:
        sys.stderr.write(f"[gcsfs-iotrace] Target execution failed: {e}\n")
        import traceback
        traceback.print_exc()
        exit_code = 1
    finally:
        from gcsfs.iotracer.tracer import _LOGGER
        _LOGGER.flush()

        from gcsfs.iotracer.visualizer import render_reports
        html_out, trace_out = render_reports(
            run_dir,
            output_html=args.output_html if args.output_html else None,
            export_trace=args.export_trace,
            title=args.title,
            main_pid=os.getpid(),
        )
        if html_out:
            sys.stdout.write(f"[gcsfs-iotrace] HTML report generated: {html_out}\n")
        if trace_out:
            sys.stdout.write(f"[gcsfs-iotrace] Chrome trace JSON generated: {trace_out}\n")
        sys.stdout.write(f"[gcsfs-iotrace] Raw per-process traces saved in: {run_dir}\n")

    return exit_code


def report_command(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="gcsfs-iotrace report",
        description="Aggregate per-process JSONL trace logs and generate interactive reports.",
    )
    parser.add_argument(
        "trace_dir",
        help="Directory containing events_*.jsonl files.",
    )
    parser.add_argument(
        "--output-html",
        "-o",
        default="io_trace.html",
        help="Path for generated interactive HTML dashboard (default: io_trace.html). Set empty to disable.",
    )
    parser.add_argument(
        "--export-trace",
        default=None,
        help="Path for exported Chrome Trace Event JSON (Perfetto) format (e.g. trace.json).",
    )
    parser.add_argument(
        "--title",
        default="Workload I/O Pattern Trace",
        help="Title displayed in the generated HTML dashboard.",
    )

    args = parser.parse_args(argv)

    run_dir = Path(args.trace_dir)
    if not run_dir.is_dir():
        sys.stderr.write(f"Error: Trace directory '{run_dir}' does not exist or is not a directory.\n")
        return 1

    from gcsfs.iotracer.visualizer import render_reports
    html_out, trace_out = render_reports(
        run_dir,
        output_html=args.output_html if args.output_html else None,
        export_trace=args.export_trace,
        title=args.title,
    )
    if html_out:
        sys.stdout.write(f"[gcsfs-iotrace] HTML report generated: {html_out}\n")
    if trace_out:
        sys.stdout.write(f"[gcsfs-iotrace] Chrome trace JSON generated: {trace_out}\n")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    if not argv:
        sys.stderr.write("Usage: gcsfs-iotrace {run,report} [options]\n")
        return 1

    subcommand = argv[0]
    rest = argv[1:]

    if subcommand == "run":
        return run_command(rest)
    elif subcommand == "report":
        return report_command(rest)
    elif subcommand in ("-h", "--help"):
        sys.stdout.write("Usage:\n  gcsfs-iotrace run [options] <script.py> [args...]\n  gcsfs-iotrace report <trace-dir> [options]\n")
        return 0
    else:
        # Default shorthand: if first arg is not report or run, treat as target script for 'run'
        return run_command(argv)


if __name__ == "__main__":
    sys.exit(main())
