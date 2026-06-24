"""On-disk raw-metric store: the single owner of the CSV directory layout.

The parser (``parsers/hf.py``) emits in-memory metrics and the calculator
(``calculate.py``) consumes flat row dicts; both used to assemble the
tessellations-compatible directory tree themselves from the path constants in
``schema.py``. That layout is now a concept with a home: ``write_raw_metrics``
lays the tree down, ``read_raw_metrics`` reads it back, and nothing else needs
to know where a metric's CSV lives. Change the layout here and both sides
follow.
"""

import csv
import os
from dataclasses import asdict, dataclass, field
from typing import List

from metrics import schema


@dataclass
class RawMetricTables:
    """Flat row dicts read back from the raw-metric tree, by metric kind."""

    step_rows: List[dict] = field(default_factory=list)
    write_rows: List[dict] = field(default_factory=list)
    restore_rows: List[dict] = field(default_factory=list)
    delete_rows: List[dict] = field(default_factory=list)
    dl_rows: List[dict] = field(default_factory=list)


def write_raw_metrics(
    parsed, out_dir: str, *, run_type: str = "perf_optimization"
) -> None:
    """Write parsed metrics to the tessellations-compatible relative layout.

    ``parsed`` is any object exposing the ``ParsedRawMetrics`` attributes
    (``step_metrics``, ``write_metrics`` and so on); it is duck-typed so this
    module carries no dependency on the parser.
    """
    if parsed.step_metrics:
        _write_csv(
            os.path.join(
                out_dir, schema.STEP_METRICS_DIRECTORY, schema.STEP_METRICS_FILE
            ),
            schema.StepMetrics,
            parsed.step_metrics,
        )

    for rank, rows in parsed.write_metrics.items():
        _write_csv(
            os.path.join(
                out_dir,
                schema.WRITE_DURATION_DIRECTORY,
                schema.PERSISTENT_STORAGE_DIRECTORY,
                schema.PER_ACCELERATOR_DIRECTORY,
                f"{rank}.csv",
            ),
            schema.WriteDurationMetrics,
            rows,
        )

    for rank, rows in parsed.restore_metrics.items():
        _write_csv(
            os.path.join(
                out_dir,
                schema.RESTORE_DURATION_DIRECTORY,
                schema.PERSISTENT_STORAGE_DIRECTORY,
                schema.PER_ACCELERATOR_DIRECTORY,
                f"{rank}.csv",
            ),
            schema.RestoreDurationMetrics,
            rows,
        )

    for rank, rows in parsed.delete_metrics.items():
        _write_csv(
            os.path.join(
                out_dir, run_type, schema.DELETE_DURATION_DIRECTORY, f"{rank}.csv"
            ),
            schema.DeleteDurationMetrics,
            rows,
        )

    if parsed.data_loading_metrics:
        _write_csv(
            os.path.join(
                out_dir,
                schema.CALCULATED_METRICS_DIRECTORY,
                schema.DATA_LOADING_METRICS_FILE,
            ),
            schema.DataLoadingMetrics,
            parsed.data_loading_metrics,
        )


def read_raw_metrics(
    in_dir: str, *, run_type: str = "perf_optimization"
) -> RawMetricTables:
    """Read the raw-metric tree under ``in_dir`` into flat row dicts."""
    return RawMetricTables(
        step_rows=_read_csv(
            os.path.join(
                in_dir, schema.STEP_METRICS_DIRECTORY, schema.STEP_METRICS_FILE
            )
        ),
        write_rows=_read_all_csvs_in(
            os.path.join(
                in_dir,
                schema.WRITE_DURATION_DIRECTORY,
                schema.PERSISTENT_STORAGE_DIRECTORY,
                schema.PER_ACCELERATOR_DIRECTORY,
            )
        ),
        restore_rows=_read_all_csvs_in(
            os.path.join(
                in_dir,
                schema.RESTORE_DURATION_DIRECTORY,
                schema.PERSISTENT_STORAGE_DIRECTORY,
                schema.PER_ACCELERATOR_DIRECTORY,
            )
        ),
        delete_rows=_read_all_csvs_in(
            os.path.join(in_dir, run_type, schema.DELETE_DURATION_DIRECTORY)
        ),
        dl_rows=_read_csv(
            os.path.join(
                in_dir,
                schema.CALCULATED_METRICS_DIRECTORY,
                schema.DATA_LOADING_METRICS_FILE,
            )
        ),
    )


def _write_csv(path: str, dataclass_type, rows) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=schema.fieldnames(dataclass_type))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def _read_csv(path: str) -> List[dict]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, newline="") as fh:
        for raw in csv.DictReader(fh):
            row = {}
            for k, v in raw.items():
                if v is None or v == "" or v == "N/A":
                    row[k] = None
                else:
                    try:
                        row[k] = float(v) if ("." in v or "e" in v.lower()) else int(v)
                    except ValueError:
                        row[k] = v
            out.append(row)
    return out


def _read_all_csvs_in(dir_path: str) -> List[dict]:
    rows = []
    if os.path.isdir(dir_path):
        for name in sorted(os.listdir(dir_path)):
            if name.endswith(".csv"):
                rows.extend(_read_csv(os.path.join(dir_path, name)))
    return rows
