import csv
import json

import pytest

from gcsfs.tests.perf.subsystembenchmarks._common import report


def test_generate_csv_returns_none_for_empty_json(tmp_path):
    # pytest-benchmark 5.x creates the --benchmark-json file even when every case was
    # skipped, leaving it EMPTY. run_suite must get csv_path=None back (the signal run.py's
    # no-results guard consumes), not a JSONDecodeError crash that buries the real problem.
    p = tmp_path / "results.json"
    p.write_text("")
    assert report.generate_csv(str(p), str(tmp_path)) is None


def test_generate_csv_returns_none_when_no_benchmarks(tmp_path):
    p = tmp_path / "results.json"
    p.write_text('{"benchmarks": []}')
    assert report.generate_csv(str(p), str(tmp_path)) is None


def test_generate_csv_unions_extra_columns_and_computes_percentiles(tmp_path):
    p = tmp_path / "results.json"
    p.write_text(
        json.dumps(
            {
                "benchmarks": [
                    {
                        "name": "a",
                        "group": "read",
                        "extra_info": {"alpha": 1},
                        "stats": {
                            "min": 1,
                            "max": 4,
                            "mean": 2.5,
                            "median": 2.5,
                            "stddev": 1.0,
                            "data": [1, 2, 3, 4],
                        },
                    },
                    {
                        "name": "b",
                        "group": "read",
                        "extra_info": {"beta": 2},
                        "stats": {
                            "min": 5,
                            "max": 8,
                            "mean": 6.5,
                            "median": 6.5,
                            "stddev": 1.0,
                            "data": [5, 6, 7, 8],
                        },
                    },
                ]
            }
        )
    )

    csv_path = report.generate_csv(str(p), str(tmp_path))
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))

    assert rows[0]["alpha"] == "1" and rows[0]["beta"] == ""
    assert rows[1]["alpha"] == "" and rows[1]["beta"] == "2"
    assert float(rows[0]["round_time_p90"]) == pytest.approx(3.7)
