import pytest

from gcsfs.tests.perf.subsystembenchmarks import run

_REQUIRED = [
    "--bucket-prefix=p",
    "--project=pr",
    "--location=us-central1",
]


def test_parse_args_accepts_a_discovered_group(monkeypatch):
    monkeypatch.setattr(run, "discover_groups", lambda: ["dataloading/example"])
    args = run.parse_args(["--group=dataloading/example"] + _REQUIRED)
    assert args.group == "dataloading/example"


def test_parse_args_rejects_negative_amplification_wait(capsys, monkeypatch):
    monkeypatch.setattr(run, "discover_groups", lambda: ["dataloading/example"])
    with pytest.raises(SystemExit):
        run.parse_args(
            ["--group=dataloading/example", "--amplification-wait=-1"] + _REQUIRED
        )
    assert "--amplification-wait must be >= 0" in capsys.readouterr().err


def test_required_amplification_rejects_missing_buckets():
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.amplification import (
        EnrichmentResult,
    )

    result = EnrichmentResult(eligible=2, enriched=1, missing_buckets=("bucket-b",))
    with pytest.raises(RuntimeError, match="bucket-b"):
        run.require_complete_amplification(result)


def test_amplification_retry_waits_once_for_missing_buckets(monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import amplification

    results = iter(
        [
            amplification.EnrichmentResult(1, 0, ("bucket-a",)),
            amplification.EnrichmentResult(1, 1, ()),
        ]
    )
    monkeypatch.setattr(amplification, "enrich_csv", lambda *a, **k: next(results))
    sleeps = []

    result = run.enrich_amplification_with_retry(
        "results.csv", "project", object(), retry_wait=30, sleep=sleeps.append
    )

    assert result.missing_buckets == ()
    assert sleeps == [30]
