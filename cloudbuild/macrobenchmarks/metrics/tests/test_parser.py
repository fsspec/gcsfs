import os

from metrics.parsers import hf

_WORKLOAD = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "..", "..", "gcsfs", "tests", "perf", "macrobenchmarks", "workloads",
    "hf-pytorch-lightning", "helm_chart", "llama_3_1_8b_cpu_sim.py")


# Log lines exactly as llama_3_1_8b_cpu_sim.py emits them (the strings whose
# format these regexes must match). These double as a workload<->parser
# compatibility guard.
STEP_LINE = ("Global Rank: 0 | Step: 5 | Loss: 0.1234 | Step Time: 1.5000s | "
             "Throughput: 42.67 samples/s")
SAVE_START = ("Checkpoint Save : Rank: 0 : Step: 25 : Start time: 100.0 "
              "seconds: Path: gs://b/ckpt/r/llama-00-25.ckpt")
SAVE_END = ("Finished saving checkpoint to gs://b/ckpt/r/llama-00-25.ckpt in "
            "12.50 seconds for global_step 25 from rank 0")
RESTORE_START = ("Checkpoint Restore Start : Rank : 0 : Start time: 50.0 "
                 "seconds : Path: gs://b/ckpt/r/llama-00-25.ckpt")
RESTORE_END = ("Finished restoring checkpoint : Rank : 0 : Duration: 8.00 "
               "seconds : End Time: 58.00 seconds : Path: gs://b/ckpt/r/x.ckpt")
RESTORE_STEP_50_START = ("Checkpoint Restore Start : Rank : 0 : Start time: "
                         "60.0 seconds : Path: gs://b/ckpt/r/llama-00-50.ckpt")
RESTORE_STEP_50_END = ("Finished restoring checkpoint : Rank : 0 : Duration: "
                       "9.00 seconds : End Time: 69.00 seconds : Path: "
                       "gs://b/ckpt/r/llama-00-50.ckpt")
DELETE_LINE = ("Finished deleting checkpoint gs://b/ckpt/r/old.ckpt in 3.00 "
               "seconds for global_step 50 from rank 0")
BLOCKED_LINE = ("[_TrainingEpochLoop].train_dataloader_next  |  100  |  10  |  "
                "12.5  |  4.2  |")


def _parse(lines):
    entries = [hf.LogEntry(timestamp=float(i), message=m)
               for i, m in enumerate(lines)]
    return hf.parse_entries(entries, run_id="r",
                            checkpoint_location="gs://b/ckpt")


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
    parsed = _parse([RESTORE_START, RESTORE_END,
                     RESTORE_STEP_50_START, RESTORE_STEP_50_END])
    rows = parsed.restore_metrics[0]
    assert len(rows) == 2
    assert [r.checkpoint_step for r in rows] == [0, 0]
    assert [r.checkpoint_location for r in rows] == [
        "gs://b/ckpt/r/llama-00-25.ckpt", "gs://b/ckpt/r/llama-00-50.ckpt"]


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


def _logging_call(src, marker):
    """Text of the logging.info(...) call whose format string contains marker."""
    i = src.index(marker)
    start = src.rindex("logging.info(", 0, i)
    depth, k = 0, src.index("(", start)
    while k < len(src):
        if src[k] == "(":
            depth += 1
        elif src[k] == ")":
            depth -= 1
            if depth == 0:
                return src[start:k + 1]
        k += 1
    raise AssertionError("unbalanced parens after " + marker)


def test_workload_logs_checkpoint_timestamps_in_wallclock():
    # calc_restore_metrics aggregates restore start/end ACROSS ranks (max end -
    # min start), so the absolute Start/End timestamps must be wall-clock
    # time.time(); perf_counter is monotonic-from-boot and per-machine, which
    # makes a cross-node span meaningless in the default 2-node topology.
    with open(_WORKLOAD) as fh:
        src = fh.read()
    for marker in ("Checkpoint Save :", "Checkpoint Restore Start",
                   "Finished restoring checkpoint"):
        call = _logging_call(src, marker)
        assert "time.time()" in call, marker
        assert "perf_counter" not in call, marker


def test_regexes_are_verbatim_hf():
    # Guards the byte-identical-to-hf.py invariant.
    assert hf.STEP_METRICS_PATTERN == (
        r"Global Rank: 0 \| Step: ([0-9]+) \| Loss: [0-9.]+ \| Step Time: "
        r"([0-9.]+)s \| Throughput: [0-9.]+ samples/s")
    assert hf.CHECKPOINT_DELETE_PATTERN == (
        r"Finished deleting checkpoint (.*) in ([0-9.]+) seconds for "
        r"global_step ([0-9]+) from rank ([0-9]+)")
