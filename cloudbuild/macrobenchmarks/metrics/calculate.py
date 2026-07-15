"""Aggregate raw-metric CSVs into one flat summary row.

Computes step-time, checkpoint write/restore/delete, data-loading, and
system-resource metrics for the HF emulated workload. MFU/TFLOPs
intentionally excluded.
"""

import argparse
import csv
import os
import sys
from collections import defaultdict

from metrics import raw_store, stats, summary_schema

PER_STEP_STABILIZATION_STEPS = 0
STABLE_WINDOW_STABILIZATION_STEPS = 10


def calc_step_time_metrics(step_rows: list) -> dict:
    """Step-time metrics: mean, plus stable/training window durations."""
    rows = [
        r
        for r in step_rows
        if r.get("step") is not None and r.get("step_duration") is not None
    ]
    if not rows:
        return {}

    out = {}
    first_step = min(r["step"] for r in rows)

    # mean_step_time: mean of per-step durations after skipping PER_STEP steps.
    per_step_durations = [
        r["step_duration"]
        for r in rows
        if r["step"] >= first_step + PER_STEP_STABILIZATION_STEPS
    ]
    if per_step_durations:
        out["mean_step_time"] = stats.mean(per_step_durations)

    per_step_sps = [
        r["samples_per_second"]
        for r in rows
        if r["step"] >= first_step + PER_STEP_STABILIZATION_STEPS
        and r.get("samples_per_second") is not None
    ]
    if per_step_sps:
        out["mean_samples_per_second"] = stats.mean(per_step_sps)

    stable_sps = [
        r["samples_per_second"]
        for r in rows
        if r["step"] >= first_step + STABLE_WINDOW_STABILIZATION_STEPS
        and r.get("samples_per_second") is not None
    ]
    if stable_sps:
        out["stable_window_avg_samples_per_second"] = stats.mean(stable_sps)

    # window metrics need step_end_time.
    end_rows = [r for r in rows if r.get("step_end_time") is not None]
    for label, skip in (
        ("training_window", PER_STEP_STABILIZATION_STEPS),
        ("stable_window", STABLE_WINDOW_STABILIZATION_STEPS),
    ):
        total, avg = _window_duration(end_rows, first_step + skip)
        if total is not None:
            out[f"{label}_total_step_duration"] = total
            out[f"{label}_avg_step_time"] = avg
    return out


def _window_duration(end_rows: list, first_stable_step: int):
    """total = last.step_end_time - first.step_end_time + first.step_duration."""
    window = sorted(
        (r for r in end_rows if r["step"] >= first_stable_step), key=lambda r: r["step"]
    )
    if not window:
        return None, None
    first, last = window[0], window[-1]
    total = last["step_end_time"] - first["step_end_time"] + first["step_duration"]
    return total, total / len(window)


def _durations_by_group(rows: list, key_fields: tuple) -> dict:
    """duration = max(end_time) - min(start_time) per group key."""
    groups = defaultdict(lambda: {"starts": [], "ends": []})
    for r in rows:
        key = tuple(r[f] for f in key_fields)
        groups[key]["starts"].append(r["start_time"])
        groups[key]["ends"].append(r["end_time"])
    return {
        k: {"duration": max(v["ends"]) - min(v["starts"]), "min_end": min(v["ends"])}
        for k, v in groups.items()
    }


def _prefixed(stats_dict: dict, prefix: str, count: int, count_name: str) -> dict:
    out = {count_name: count}
    for k, v in stats_dict.items():
        out[f"{prefix}_{k}"] = v
    return out


def calc_write_metrics(write_rows: list) -> dict:
    if not write_rows:
        return {}
    groups = _durations_by_group(write_rows, ("checkpoint_step", "checkpoint_location"))
    durations = [g["duration"] for g in groups.values()]
    return _prefixed(
        stats.duration_stats(durations),
        "checkpoint_write_time",
        len(durations),
        "num_checkpoint_write_datapoints",
    )


def calc_delete_metrics(delete_rows: list) -> dict:
    if not delete_rows:
        return {}
    groups = _durations_by_group(
        delete_rows, ("checkpoint_step", "checkpoint_location")
    )
    durations = [g["duration"] for g in groups.values()]
    return _prefixed(
        stats.duration_stats(durations),
        "checkpoint_delete_time",
        len(durations),
        "num_checkpoint_delete_datapoints",
    )


def calc_restore_metrics(restore_rows: list) -> dict:
    if not restore_rows:
        return {}
    # Group by the restored checkpoint (checkpoint_location is the loaded path):
    # all ranks restoring one checkpoint collapse into a single distributed
    # datapoint (max end - min start), while two distinct restores stay
    # separate. Under DDP a normal resume restores one checkpoint, so this is
    # one datapoint.
    groups = _durations_by_group(
        restore_rows, ("checkpoint_step", "checkpoint_location")
    )
    durations = [g["duration"] for g in groups.values()]
    out = _prefixed(
        stats.duration_stats(durations),
        "checkpoint_restore_time",
        len(durations),
        "num_checkpoint_restore_datapoints",
    )
    # checkpoint_restore_time_initial = duration of the earliest-ending restore.
    initial_key = min(groups, key=lambda k: groups[k]["min_end"])
    out["checkpoint_restore_time_initial"] = groups[initial_key]["duration"]
    return out


# Summary column order is owned by macrobenchmarks_schema.json (the BigQuery
# external-table definition); the CSV header derives from it so the two cannot
# drift. See metrics/summary_schema.py.
SUMMARY_FIELDNAMES = summary_schema.fieldnames()


def calc_data_loading_metrics(dl_rows: list) -> dict:
    """Run-wide accelerator-blocked datapoint, taken from the bottleneck rank.

    Every rank emits the Lightning profiler summary, so several run-wide
    (epoch_idx == -1) rows can be present (the log filter spans both node pods).
    The distributed step is gated by the slowest rank, so report the
    bottleneck: the run-wide row with the greatest accelerator_blocked_time,
    passing both of its fields through together. This is deterministic
    regardless of log/ingestion order, unlike picking the first row.
    """
    candidates = [
        r
        for r in dl_rows
        if r.get("epoch_idx") == -1
        and r.get("accelerator_blocked_time") is not None
        and r.get("accelerator_blocked_percent") is not None
    ]
    if not candidates:
        return {}
    row = max(candidates, key=lambda r: r["accelerator_blocked_time"])
    return {
        "accelerator_blocked_time": row["accelerator_blocked_time"],
        "accelerator_blocked_percent": row["accelerator_blocked_percent"],
    }


# The two Lightning profiler actions DataWaitProfiler watches; disjoint and
# jointly exhaustive of the time the fit loop blocks on the train dataloader
# (see the workload's DATA_WAIT_ACTIONS).
_DATA_WAIT_SETUP_ACTION = "setup_train_dataloader"
_DATA_WAIT_FETCH_ACTION = "[_TrainingEpochLoop].train_dataloader_next"


def calc_data_wait_metrics(data_wait_rows: list) -> dict:
    """Total dataloader-blocked time, taken from the bottleneck rank.

    Every rank logs one ``Data Wait`` line per blocking span with a
    monotonically increasing running total, so a rank's blocked time is the
    max ``cumulative_total`` it logged -- accurate as of its last surviving
    line even when tail lines are lost to Cloud Logging lag, unlike a sum of
    ``duration``. The distributed step is gated by the slowest rank, so report
    the rank with the greatest total, passing its setup/fetch split (summed
    from the same rank's per-span durations) through together. The split can
    undercount when that rank's lines were lost; the headline total cannot.
    """
    by_rank = defaultdict(list)
    for r in data_wait_rows:
        if r.get("global_rank") is not None and r.get("cumulative_total") is not None:
            by_rank[r["global_rank"]].append(r)
    if not by_rank:
        return {}
    totals = {
        rank: max(r["cumulative_total"] for r in rows) for rank, rows in by_rank.items()
    }
    bottleneck = max(totals, key=lambda rank: totals[rank])
    rows = by_rank[bottleneck]
    setup = [
        r["duration"]
        for r in rows
        if r.get("action") == _DATA_WAIT_SETUP_ACTION and r.get("duration") is not None
    ]
    fetch = [
        r["duration"]
        for r in rows
        if r.get("action") == _DATA_WAIT_FETCH_ACTION and r.get("duration") is not None
    ]
    out = {
        "data_wait_total_time": totals[bottleneck],
        "num_data_wait_spans": len(rows),
    }
    if setup:
        out["data_wait_iterator_setup_time"] = sum(setup)
    if fetch:
        out["data_wait_batch_fetch_time"] = sum(fetch)
    return out


def calc_dataset_build_metrics(dataset_build_rows: list) -> dict:
    """Dataset-build duration (Parquet glob + shuffle-buffer wiring), bottleneck rank.

    Every rank builds its own streaming dataset once before ``trainer.fit``
    starts, so report the slowest rank's duration -- the metadata-listing call
    that gates the first batch fetch, deterministic regardless of log/ingestion
    order.
    """
    durations = [
        r["duration"] for r in dataset_build_rows if r.get("duration") is not None
    ]
    if not durations:
        return {}
    return {"dataset_build_time": max(durations)}


def calc_throughput_metrics(write_rows: list, size_rows: list) -> dict:
    out = {}

    written_sizes = [
        r["size_bytes"] for r in size_rows if r.get("size_bytes") is not None
    ]
    if written_sizes:
        out["checkpoint_size_bytes"] = max(written_sizes)

    size_by_step = {
        r["checkpoint_step"]: r["size_bytes"]
        for r in size_rows
        if r.get("checkpoint_step") is not None and r.get("size_bytes") is not None
    }
    write_groups = _durations_by_group(
        write_rows, ("checkpoint_step", "checkpoint_location")
    )
    write_tps = [
        size_by_step[step] / g["duration"]
        for (step, _loc), g in write_groups.items()
        if step in size_by_step and g["duration"] > 0
    ]
    if write_tps:
        out["checkpoint_write_throughput_avg_bytes_per_sec"] = stats.mean(write_tps)

    return out


def _restore_throughput(restored_bytes, restore_duration):
    if restored_bytes is not None and restore_duration:
        return restored_bytes / restore_duration
    return None


# Maps series to schema columns. `None` mean-column means the series has no mean.
_SYSTEM_SERIES_COLUMNS = {
    "cpu": ("cpu_usage_peak_cores", "cpu_usage_mean_cores"),
    "memory": ("memory_usage_peak_bytes", "memory_usage_mean_bytes"),
    "network_received": (
        "network_received_peak_bytes_per_sec",
        "network_received_mean_bytes_per_sec",
    ),
    "network_sent": (
        "network_sent_peak_bytes_per_sec",
        "network_sent_mean_bytes_per_sec",
    ),
    "checkpoint_read_bytes": ("checkpoint_read_bytes", None),
    "checkpoint_read_request_count": ("checkpoint_read_request_count", None),
    "checkpoint_restored_bytes": ("checkpoint_restored_bytes", None),
    "dataset_read_bytes": ("dataset_read_bytes", None),
    "dataset_read_request_count": ("dataset_read_request_count", None),
    "dataset_size_bytes": ("dataset_size_bytes", None),
    "dataset_sample_count": ("dataset_sample_count", None),
}

# Columns reported as whole numbers (bytes / counts) rather than floats.
_INT_COLUMNS = {
    "memory_usage_peak_bytes",
    "memory_usage_mean_bytes",
    "checkpoint_read_bytes",
    "checkpoint_read_request_count",
    "checkpoint_restored_bytes",
    "dataset_read_bytes",
    "dataset_read_request_count",
    "dataset_size_bytes",
    "dataset_sample_count",
}


def _amplification(numerator, denominator):
    """numerator / denominator, or None when either is absent/zero."""
    if numerator is not None and denominator:
        return numerator / denominator
    return None


def _max_peak(by_metric: dict, name: str):
    """Max non-null ``peak`` among the rows for series ``name``, or None."""
    peaks = [r["peak"] for r in by_metric.get(name, []) if r.get("peak") is not None]
    return max(peaks) if peaks else None


def executed_step_count(step_rows: list) -> int:
    """Number of distinct optimizer steps observed (deduped across ranks).

    Each rank emits one row per optimizer step, so the count of unique step
    numbers -- not rows -- is how many steps the run actually executed.
    """
    return len(
        {
            r["step"]
            for r in step_rows
            if r.get("step") is not None and r.get("step_duration") is not None
        }
    )


def dataset_read_amplification_ratio(
    *,
    dataset_read_bytes,
    dataset_size_bytes,
    dataset_sample_count,
    executed_steps,
    global_batch_size,
):
    """dataset_read_bytes / ideal_bytes, or None if any input is absent/zero.

    ``ideal_bytes`` is the egress a perfectly sharded single pass over the
    samples actually consumed (``executed_steps * global_batch_size *
    dataset_size_bytes / dataset_sample_count``) would incur. Normalizing by
    samples consumed, not the full dataset, makes the ratio independent of
    dataset size and step count: ~1.0 means each byte was fetched once,
    ~world_size means every rank re-read the same data.
    """
    if None in (
        dataset_read_bytes,
        dataset_size_bytes,
        dataset_sample_count,
        executed_steps,
        global_batch_size,
    ):
        return None
    ideal_bytes = (
        executed_steps * global_batch_size * dataset_size_bytes / dataset_sample_count
        if dataset_sample_count
        else 0
    )
    if not ideal_bytes:
        return None
    return dataset_read_bytes / ideal_bytes


def calc_system_metrics(system_rows: list) -> dict:
    """Reduce per-pod/per-bucket metrics to the bottleneck value and derive ratios."""
    out = {}
    by_metric = defaultdict(list)
    for r in system_rows:
        by_metric[r.get("metric")].append(r)
    for series, (peak_col, mean_col) in _SYSTEM_SERIES_COLUMNS.items():
        rows = by_metric.get(series, [])
        peaks = [r["peak"] for r in rows if r.get("peak") is not None]
        if peaks:
            val = max(peaks)
            out[peak_col] = int(val) if peak_col in _INT_COLUMNS else val
        if mean_col:
            means = [r["mean"] for r in rows if r.get("mean") is not None]
            if means:
                val = max(means)
                out[mean_col] = int(val) if mean_col in _INT_COLUMNS else val
    ratio = _amplification(
        out.get("checkpoint_read_bytes"), out.get("checkpoint_restored_bytes")
    )
    if ratio is not None:
        out["checkpoint_read_amplification_ratio"] = ratio
    # Stands in for GKE's `*/limit_utilization` metrics, which need a container
    # limit we don't set: bottleneck-pod peak usage / node allocatable capacity.
    cpu_util = _amplification(
        out.get("cpu_usage_peak_cores"), _max_peak(by_metric, "node_allocatable_cores")
    )
    if cpu_util is not None:
        out["cpu_limit_utilization_peak"] = cpu_util
    mem_util = _amplification(
        out.get("memory_usage_peak_bytes"),
        _max_peak(by_metric, "node_allocatable_bytes"),
    )
    if mem_util is not None:
        out["memory_limit_utilization_peak"] = mem_util
    # Dataset ratio is derived in build_summary_row; it needs step/batch-size
    # inputs this reducer doesn't have.
    return out


def build_summary_row(
    *,
    run_id: str,
    workload_name: str,
    requirements: str,
    step_rows: list,
    write_rows: list,
    restore_rows: list,
    delete_rows: list,
    dl_rows: list,
    size_rows: list = None,
    system_rows: list = None,
    data_wait_rows: list = None,
    dataset_build_rows: list = None,
    dimensions: dict = None,
) -> dict:
    row = {
        "run_id": run_id,
        "workload_name": workload_name,
        "requirements": requirements,
    }
    if dimensions:
        row.update({k: v for k, v in dimensions.items() if v is not None})
    row.update(calc_step_time_metrics(step_rows))
    row.update(calc_write_metrics(write_rows))
    row.update(calc_restore_metrics(restore_rows))
    row.update(calc_delete_metrics(delete_rows))
    row.update(calc_data_loading_metrics(dl_rows))
    row.update(calc_data_wait_metrics(data_wait_rows or []))
    row.update(calc_dataset_build_metrics(dataset_build_rows or []))
    row.update(calc_throughput_metrics(write_rows, size_rows or []))
    row.update(calc_system_metrics(system_rows or []))
    restore_throughput = _restore_throughput(
        row.get("checkpoint_restored_bytes"),
        row.get("checkpoint_restore_time_initial"),
    )
    if restore_throughput is not None:
        row["checkpoint_restore_throughput_avg_bytes_per_sec"] = restore_throughput
    # Derived here, not in calc_system_metrics, since it needs executed_steps
    # and global_batch_size alongside the raw dataset columns just produced.
    ratio = dataset_read_amplification_ratio(
        dataset_read_bytes=row.get("dataset_read_bytes"),
        dataset_size_bytes=row.get("dataset_size_bytes"),
        dataset_sample_count=row.get("dataset_sample_count"),
        executed_steps=executed_step_count(step_rows),
        global_batch_size=row.get("global_batch_size"),
    )
    if ratio is not None:
        row["dataset_read_amplification_ratio"] = ratio
    return row


def validate_required_metrics(
    *,
    step_rows: list,
    write_rows: list,
    restore_rows: list = None,
    dl_rows: list = None,
    data_wait_rows: list = None,
    expected_steps: int = 0,
    min_write_datapoints: int = 0,
    min_restore_datapoints: int = 0,
    require_data_loading: bool = False,
    require_data_wait: bool = False,
    resume_run: bool = False,
    checkpoint_interval: int = 0,
) -> None:
    """Fail if required benchmark metrics are missing or incomplete."""
    observed_steps = {
        r["step"]
        for r in step_rows
        if r.get("step") is not None and r.get("step_duration") is not None
    }
    if expected_steps:
        required_steps = 1 if expected_steps < 0 else expected_steps
        if resume_run:
            if not observed_steps or max(observed_steps) < required_steps:
                found = max(observed_steps) if observed_steps else "none"
                _fail_validation(
                    f"expected resumed run to reach step {required_steps}, "
                    f"found {found}"
                )
        elif len(observed_steps) < required_steps:
            _fail_validation(
                f"expected at least {required_steps} step metrics, found "
                f"{len(observed_steps)}"
            )

    if min_write_datapoints:
        groups = _durations_by_group(
            write_rows, ("checkpoint_step", "checkpoint_location")
        )
        write_datapoints = len(groups)
        required_write_datapoints = min_write_datapoints
        if resume_run and checkpoint_interval and observed_steps:
            first_step, last_step = min(observed_steps), max(observed_steps)
            required_write_datapoints = sum(
                1
                for step in range(
                    checkpoint_interval, last_step + 1, checkpoint_interval
                )
                if step >= first_step
            )
        if write_datapoints < required_write_datapoints:
            _fail_validation(
                f"expected at least {required_write_datapoints} checkpoint write "
                f"datapoints, found {write_datapoints}"
            )

    if min_restore_datapoints:
        restore_datapoints = len(
            _durations_by_group(
                restore_rows or [], ("checkpoint_step", "checkpoint_location")
            )
        )
        if restore_datapoints < min_restore_datapoints:
            _fail_validation(
                f"expected at least {min_restore_datapoints} checkpoint "
                f"restore datapoints, found {restore_datapoints}"
            )

    if require_data_loading:
        # The profiler summary that carries accelerator_blocked_* is emitted
        # last and is the most likely casualty of Cloud Logging lag / a parser
        # miss. Require a run-wide (epoch_idx == -1) row with both fields
        # populated so we never upload a "successful" summary with N/A
        # data-loading metrics.
        has_run_wide = any(
            r.get("epoch_idx") == -1
            and r.get("accelerator_blocked_time") is not None
            and r.get("accelerator_blocked_percent") is not None
            for r in (dl_rows or [])
        )
        if not has_run_wide:
            _fail_validation(
                "required data-loading metrics missing: no epoch_idx == -1 row "
                "with non-null accelerator_blocked_time and "
                "accelerator_blocked_percent"
            )

    if require_data_wait:
        # Data Wait lines are emitted throughout the run, but the setup span
        # exists only when the image runs the zhixiangli/pytorch-lightning fork
        # (the `setup_train_dataloader` profiler action + epoch-boundary fix).
        # Requiring both span kinds fails loudly when an image was built with
        # stock lightning, which would otherwise silently undercount
        # data_wait_total_time by every worker-spawn/first-prefetch span.
        rows = data_wait_rows or []
        has_setup = any(
            r.get("action") == _DATA_WAIT_SETUP_ACTION
            and r.get("cumulative_total") is not None
            for r in rows
        )
        has_fetch = any(
            r.get("action") == _DATA_WAIT_FETCH_ACTION
            and r.get("cumulative_total") is not None
            for r in rows
        )
        if not (has_setup and has_fetch):
            _fail_validation(
                "required data-wait metrics missing: need at least one "
                f"'{_DATA_WAIT_SETUP_ACTION}' and one '{_DATA_WAIT_FETCH_ACTION}' "
                "Data Wait span (is the image running the patched lightning "
                "fork and the DataWaitProfiler workload?)"
            )


def _fail_validation(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate raw metric CSVs into one summary row."
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--workload-name", required=True)
    parser.add_argument("--requirements", required=True)
    parser.add_argument("--in-dir", required=True)
    parser.add_argument("--out-file", required=True)
    parser.add_argument("--run-type", default="perf_optimization")
    parser.add_argument("--expected-steps", type=int, default=0)
    parser.add_argument("--min-write-datapoints", type=int, default=0)
    parser.add_argument("--min-restore-datapoints", type=int, default=0)
    parser.add_argument(
        "--require-data-loading-metrics",
        action="store_true",
        help="Fail unless a run-wide accelerator-blocked " "datapoint is present.",
    )
    parser.add_argument(
        "--require-data-wait-metrics",
        action="store_true",
        help="Fail unless both Data Wait span kinds (setup + fetch) are present.",
    )
    parser.add_argument(
        "--resume-run",
        action="store_true",
        help="Validate against a resumed run's observed step "
        "range instead of a fresh-run step count.",
    )
    parser.add_argument("--bucket-type")
    parser.add_argument("--zone")
    parser.add_argument("--region")
    parser.add_argument("--machine-type")
    parser.add_argument("--nodes", type=int)
    parser.add_argument("--ranks-per-node", type=int)
    parser.add_argument("--steps", type=int)
    parser.add_argument("--checkpoint-interval", type=int)
    parser.add_argument("--checkpoints-to-keep", type=int)
    parser.add_argument("--dataset-path")
    parser.add_argument("--model-id")
    parser.add_argument("--training-strategy")
    parser.add_argument("--tensor-parallel-size", type=int)
    parser.add_argument("--data-parallel-size", type=int)
    parser.add_argument("--simulated-step-compute-seconds", type=float)
    parser.add_argument("--per-device-batch", type=int)
    parser.add_argument("--grad-accum", type=int)
    parser.add_argument("--dataloader-workers", type=int)
    parser.add_argument("--epochs", type=int)
    parser.add_argument("--shuffle-buffer-size", type=int)
    parser.add_argument("--shuffle-max-buffer-input-shards", type=int)
    parser.add_argument("--dataloader-prefetch-factor", type=int)
    parser.add_argument("--image")
    args = parser.parse_args(argv)

    tables = raw_store.read_raw_metrics(args.in_dir, run_type=args.run_type)
    step_rows = tables.step_rows
    write_rows = tables.write_rows
    restore_rows = tables.restore_rows
    delete_rows = tables.delete_rows
    dl_rows = tables.dl_rows
    size_rows = tables.size_rows
    system_rows = tables.system_rows

    validate_required_metrics(
        step_rows=step_rows,
        write_rows=write_rows,
        dl_rows=dl_rows,
        data_wait_rows=tables.data_wait_rows,
        restore_rows=restore_rows,
        expected_steps=args.expected_steps,
        min_write_datapoints=args.min_write_datapoints,
        min_restore_datapoints=args.min_restore_datapoints,
        require_data_loading=args.require_data_loading_metrics,
        require_data_wait=args.require_data_wait_metrics,
        resume_run=args.resume_run,
        checkpoint_interval=args.checkpoint_interval,
    )

    # global_batch_size = per_device_batch * grad_accum * world_size, with
    # world_size = nodes * ranks_per_node -- mirrors the sim's formula. Derived
    # here (not a flag) so it stays consistent with its components; left N/A
    # when any component is absent rather than reporting a partial product.
    global_batch_size = None
    components = (
        args.per_device_batch,
        args.grad_accum,
        args.nodes,
        args.ranks_per_node,
    )
    if all(c is not None for c in components):
        global_batch_size = (
            args.per_device_batch * args.grad_accum * args.nodes * args.ranks_per_node
        )

    # max_epochs can end a run before --steps is reached; report what ran.
    recorded_steps = args.steps
    if step_rows:
        observed_steps = executed_step_count(step_rows)
        if args.steps is None or args.steps < 0 or observed_steps < args.steps:
            recorded_steps = observed_steps

    dimensions = {
        "bucket_type": args.bucket_type,
        "zone": args.zone,
        "region": args.region,
        "machine_type": args.machine_type,
        "nodes": args.nodes,
        "ranks_per_node": args.ranks_per_node,
        "steps": recorded_steps,
        "checkpoint_interval": args.checkpoint_interval,
        "checkpoints_to_keep": args.checkpoints_to_keep,
        "dataset_path": args.dataset_path,
        "model_id": args.model_id,
        "training_strategy": args.training_strategy,
        "simulated_step_compute_seconds": args.simulated_step_compute_seconds,
        "per_device_train_batch_size": args.per_device_batch,
        "gradient_accumulation_steps": args.grad_accum,
        "global_batch_size": global_batch_size,
        "dataloader_num_workers": args.dataloader_workers,
        "num_train_epochs": args.epochs,
        "shuffle_buffer_size": args.shuffle_buffer_size,
        "shuffle_max_buffer_input_shards": args.shuffle_max_buffer_input_shards,
        "dataloader_prefetch_factor": args.dataloader_prefetch_factor,
        "image": args.image,
    }
    # TP/DP apply to model_parallel only; omitting them for ddp/fsdp lets
    # DictWriter's restval="N/A" mark them not-applicable.
    if (args.training_strategy or "").startswith("model_parallel"):
        dimensions["tensor_parallel_size"] = args.tensor_parallel_size
        dimensions["data_parallel_size"] = args.data_parallel_size
    row = build_summary_row(
        run_id=args.run_id,
        workload_name=args.workload_name,
        requirements=args.requirements,
        step_rows=step_rows,
        write_rows=write_rows,
        restore_rows=restore_rows,
        delete_rows=delete_rows,
        dl_rows=dl_rows,
        size_rows=size_rows,
        system_rows=system_rows,
        data_wait_rows=tables.data_wait_rows,
        dataset_build_rows=tables.dataset_build_rows,
        dimensions=dimensions,
    )

    os.makedirs(os.path.dirname(os.path.abspath(args.out_file)), exist_ok=True)
    with open(args.out_file, "w", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=SUMMARY_FIELDNAMES, restval="N/A", extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerow(row)
    print(f"Wrote summary to {args.out_file}")


if __name__ == "__main__":
    main()
