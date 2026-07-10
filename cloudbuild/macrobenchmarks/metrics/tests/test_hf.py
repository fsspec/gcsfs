import pytest
from metrics.parsers import hf

# Log lines exactly as llama_3_1_8b_cpu_sim.py emits them (the strings whose
# format these regexes must match). These double as a workload<->parser
# compatibility guard.
STEP_LINE = (
    "Global Rank: 0 | Step: 5 | Loss: 0.1234 | Step Time: 1.5000s | "
    "Throughput: 42.67 samples/s"
)
SAVE_START = (
    "Checkpoint Save : Rank: 0 : Step: 25 : Start time: 100.0 "
    "seconds: Path: gs://b/ckpt/r/llama-00-25.ckpt"
)
SAVE_END = (
    "Finished saving checkpoint to gs://b/ckpt/r/llama-00-25.ckpt in "
    "12.50 seconds for global_step 25 from rank 0"
)
RESTORE_START = (
    "Checkpoint Restore Start : Rank : 0 : Start time: 50.0 "
    "seconds : Path: gs://b/ckpt/r/llama-00-25.ckpt"
)
RESTORE_END = (
    "Finished restoring checkpoint : Rank : 0 : Duration: 8.00 "
    "seconds : End Time: 58.00 seconds : Path: gs://b/ckpt/r/x.ckpt"
)
RESTORE_STEP_50_START = (
    "Checkpoint Restore Start : Rank : 0 : Start time: "
    "60.0 seconds : Path: gs://b/ckpt/r/llama-00-50.ckpt"
)
RESTORE_STEP_50_END = (
    "Finished restoring checkpoint : Rank : 0 : Duration: "
    "9.00 seconds : End Time: 69.00 seconds : Path: "
    "gs://b/ckpt/r/llama-00-50.ckpt"
)
DELETE_LINE = (
    "Finished deleting checkpoint gs://b/ckpt/r/old.ckpt in 3.00 "
    "seconds for global_step 50 from rank 0"
)
BLOCKED_LINE = (
    "[_TrainingEpochLoop].train_dataloader_next  |  100  |  10  |  " "12.5  |  4.2  |"
)


def _parse(lines):
    entries = [hf.LogEntry(timestamp=float(i), message=m) for i, m in enumerate(lines)]
    return hf.parse_entries(entries, run_id="r", checkpoint_location="gs://b/ckpt")


def test_step_metrics_parsed():
    parsed = _parse([STEP_LINE])
    assert len(parsed.step_metrics) == 1
    row = parsed.step_metrics[0]
    assert row.step == 5
    assert row.step_duration == 1.5
    assert row.step_end_time == 0.0  # timestamp of the entry


def test_checkpoint_write_pairing():
    parsed = _parse([SAVE_START, SAVE_END])
    rows = parsed.write_metrics[0]
    assert len(rows) == 1
    assert rows[0].checkpoint_step == 25
    assert rows[0].start_time == 100.0
    assert rows[0].end_time == 112.5  # start + parsed duration; end - start = 12.5


def test_checkpoint_restore_pairing():
    parsed = _parse([RESTORE_START, RESTORE_END])
    rows = parsed.restore_metrics[0]
    assert len(rows) == 1
    # checkpoint_location is the path that was restored (captured at the paired
    # start), so all ranks restoring one checkpoint group into a single
    # distributed datapoint in calc_restore_metrics (see test_parser_io).
    assert rows[0].checkpoint_step == 0
    assert rows[0].checkpoint_location == "gs://b/ckpt/r/llama-00-25.ckpt"
    assert rows[0].start_time == 50.0
    assert rows[0].end_time == 58.0  # end - start = 8.0


def test_distinct_restores_keyed_by_checkpoint_path():
    # Two restores of different checkpoints must keep distinct
    # checkpoint_locations (the loaded path) so the calculator can tell them
    # apart instead of collapsing both into one inflated span.
    parsed = _parse(
        [RESTORE_START, RESTORE_END, RESTORE_STEP_50_START, RESTORE_STEP_50_END]
    )
    rows = parsed.restore_metrics[0]
    assert len(rows) == 2
    assert [r.checkpoint_step for r in rows] == [0, 0]
    assert [r.checkpoint_location for r in rows] == [
        "gs://b/ckpt/r/llama-00-25.ckpt",
        "gs://b/ckpt/r/llama-00-50.ckpt",
    ]


def test_checkpoint_delete_rank0():
    parsed = _parse([DELETE_LINE])
    rows = parsed.delete_metrics[0]
    assert len(rows) == 1
    assert rows[0].checkpoint_step == 50
    assert rows[0].end_time - rows[0].start_time == 3.0


def test_accelerator_blocked_time():
    parsed = _parse([BLOCKED_LINE])
    assert len(parsed.data_loading_metrics) == 1
    dl = parsed.data_loading_metrics[0]
    assert dl.accelerator_blocked_time == 12.5
    assert dl.accelerator_blocked_percent == 4.2


SIZE_LINE = (
    "Checkpoint Size : Rank : 0 : Step : 25 : Bytes : 17179869184 : "
    "Path: gs://b/ckpt/r/llama-00-25.ckpt"
)


def test_step_line_captures_samples_per_second():
    parsed = _parse([STEP_LINE])
    assert parsed.step_metrics[0].samples_per_second == 42.67


def test_checkpoint_size_parsed():
    parsed = _parse([SIZE_LINE])
    assert len(parsed.checkpoint_sizes) == 1
    row = parsed.checkpoint_sizes[0]
    assert row.checkpoint_step == 25
    assert row.size_bytes == 17179869184
    assert row.checkpoint_location == "gs://b/ckpt/r/llama-00-25.ckpt"
    assert row.global_rank == 0


class _FakeGapicPage:
    def __init__(self, entries, next_page_token):
        self.entries = entries
        self.next_page_token = next_page_token


class _FakeGapicPager:
    def __init__(self, pages, fail=False):
        self._pages = pages
        self._fail = fail

    @property
    def pages(self):
        if self._fail:
            from google.api_core import exceptions as gexc

            raise gexc.ResourceExhausted("quota exceeded")
        yield from self._pages


class _FakeClient:
    def __init__(self):
        from datetime import datetime, timezone
        from unittest.mock import MagicMock

        self.project = "test-proj"
        self.calls = 0
        self.logging_api = MagicMock()
        ts1 = datetime.fromtimestamp(1.0, tz=timezone.utc)
        ts2 = datetime.fromtimestamp(2.0, tz=timezone.utc)

        page1 = _FakeGapicPage(
            [
                MagicMock(
                    text_payload="hello",
                    json_payload=None,
                    timestamp=ts1,
                    spec=["text_payload", "json_payload", "timestamp"],
                ),
                MagicMock(
                    text_payload="world",
                    json_payload=None,
                    timestamp=ts2,
                    spec=["text_payload", "json_payload", "timestamp"],
                ),
            ],
            next_page_token="",
        )

        def _list_log_entries(request):
            assert request["page_size"] == hf._MAX_PAGE_SIZE
            fail = self.calls == 0
            self.calls += 1
            return _FakeGapicPager([page1], fail=fail)

        self.list_log_entries = _list_log_entries


def test_iter_log_entries_retries_on_quota_error():
    retries = pytest.importorskip("google.api_core.retry")
    gexc = pytest.importorskip("google.api_core.exceptions")

    fast_retry = retries.Retry(
        predicate=retries.if_exception_type(gexc.ResourceExhausted),
        initial=0.0,
        maximum=0.0,
        multiplier=1.0,
        deadline=30.0,
    )
    client = _FakeClient()
    entries = list(
        hf.iter_log_entries(client, "test-proj", "some-filter", retry=fast_retry)
    )

    assert client.calls == 2
    assert [e.message for e in entries] == ["hello", "world"]
    assert [e.timestamp for e in entries] == [1.0, 2.0]


def test_build_filter_includes_all_patterns():
    filter_str = hf.build_filter(
        project="p",
        run_id="r",
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-01T01:00:00Z",
    )
    for pattern in hf.ALL_PATTERNS:
        assert pattern in filter_str
    assert hf.CHECKPOINT_SIZE_PATTERN in hf.ALL_PATTERNS
