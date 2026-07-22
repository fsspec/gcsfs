import argparse
import glob
import logging
import os
from datetime import datetime

from gcsfs.tests.perf.subsystembenchmarks._common import cli


def discover_groups():
    """Return runnable ``<subsystem>/<engine>`` benchmark groups."""
    root = os.path.dirname(os.path.abspath(__file__))
    return sorted(
        os.path.relpath(os.path.dirname(path), root)
        for path in glob.glob(os.path.join(root, "*", "*", "requirements.txt"))
    )


def _setup_environment(args):
    """Export the per-case bucket configuration consumed by dataloading."""
    os.environ["GCSFS_SUBSYSTEM_BUCKET_PREFIX"] = args.bucket_prefix
    os.environ["GCSFS_SUBSYSTEM_BUCKET_TYPE"] = args.bucket_type
    os.environ["GCSFS_SUBSYSTEM_PROJECT"] = args.project
    os.environ["GCSFS_SUBSYSTEM_LOCATION"] = args.location
    os.environ["GCSFS_SUBSYSTEM_ZONE"] = args.zone or ""
    os.environ["GCSFS_SUBSYSTEM_SWEEP_AXES"] = args.sweep_axes
    os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = "true"


def _build_parser():
    parser = argparse.ArgumentParser(description="Run gcsfs subsystem benchmarks.")
    parser.add_argument(
        "--group", required=True, help="e.g. dataloading/huggingface_datasets"
    )
    parser.add_argument(
        "--sweep-axes",
        default="",
        help="whitespace-separated config sweep axes; baseline is always included",
    )
    parser.add_argument(
        "--bucket-prefix",
        required=True,
        help="name prefix for the per-case buckets this run creates",
    )
    parser.add_argument(
        "--project", required=True, help="GCP project owning buckets and metrics"
    )
    parser.add_argument(
        "--location", required=True, help="bucket region, e.g. us-central1"
    )
    parser.add_argument(
        "--bucket-type",
        choices=("regional", "zonal", "hns"),
        default="regional",
        help="storage tier used by every case in the run",
    )
    parser.add_argument(
        "--zone", help="placement zone; required when --bucket-type=zonal"
    )
    parser.add_argument(
        "--amplification-wait",
        type=int,
        default=300,
        help="seconds to wait before the read-amplification scrape",
    )
    parser.add_argument(
        "--amplification-retry-wait",
        type=int,
        default=60,
        help="seconds to wait before retrying missing Monitoring metrics",
    )
    parser.add_argument(
        "--require-amplification",
        action="store_true",
        help="fail when eligible rows still lack amplification metrics",
    )
    return parser


def parse_args(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.bucket_type == "zonal" and not args.zone:
        parser.error("--zone is required when --bucket-type=zonal")
    if args.amplification_wait < 0:
        parser.error("--amplification-wait must be >= 0")
    if args.amplification_retry_wait < 0:
        parser.error("--amplification-retry-wait must be >= 0")
    groups = discover_groups()
    if args.group not in groups:
        parser.error(f"unknown --group {args.group!r}; available: {', '.join(groups)}")
    return args


_AMPLIFICATION_COLS = (
    "gcs_bucket_name",
    "measurement_window_start_unix_seconds",
    "measurement_window_end_unix_seconds",
    "dataset_size_bytes",
)


def _csv_has_amplification_inputs(csv_path):
    import csv

    try:
        with open(csv_path, newline="") as file:
            header = next(csv.reader(file), [])
        return all(column in header for column in _AMPLIFICATION_COLS)
    except Exception:
        return False


def enrich_amplification_with_retry(csv_path, project, client, *, retry_wait, sleep):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import amplification

    result = amplification.enrich_csv(csv_path, project, client=client)
    if result.missing_buckets:
        if retry_wait:
            sleep(retry_wait)
        result = amplification.enrich_csv(csv_path, project, client=client)
    return result


def require_complete_amplification(result):
    if result.missing_buckets:
        raise RuntimeError(
            "read-amplification metrics missing for buckets: "
            + ", ".join(result.missing_buckets)
        )


def _scrape_amplification(csv_path, args):
    if not _csv_has_amplification_inputs(csv_path):
        return None
    try:
        import time

        from google.cloud import monitoring_v3

        if args.amplification_wait:
            time.sleep(args.amplification_wait)
        client = monitoring_v3.MetricServiceClient()
        result = enrich_amplification_with_retry(
            csv_path,
            args.project,
            client,
            retry_wait=args.amplification_retry_wait,
            sleep=time.sleep,
        )
        logging.info(
            "amplification enriched %s/%s rows in %s",
            result.enriched,
            result.eligible,
            csv_path,
        )
        if result.missing_buckets:
            logging.warning(
                "amplification still missing for buckets: %s",
                ", ".join(result.missing_buckets),
            )
        if args.require_amplification:
            require_complete_amplification(result)
        return result
    except Exception as exc:
        if args.require_amplification:
            raise RuntimeError(f"required amplification scrape failed: {exc}") from exc
        logging.warning("amplification scrape skipped: %s", exc)
        return None


def main(argv=None):
    args = parse_args(argv)
    _setup_environment(args)
    os.environ["GCSFS_SUBSYSTEM_GROUP"] = args.group
    suite_dir = os.path.join(os.path.dirname(__file__), args.group)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    results_dir = os.path.join(os.path.dirname(__file__), "__run__", timestamp)
    os.makedirs(results_dir, exist_ok=True)

    rc, csv_path = cli.run_suite(suite_dir, results_dir)
    if csv_path is None:
        logging.error("no benchmark results produced by group %s", args.group)
        raise SystemExit(rc or 1)
    _scrape_amplification(csv_path, args)
    logging.info("subsystembenchmarks run rc=%s csv=%s", rc, csv_path)
    raise SystemExit(rc)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    main()
