import argparse
import glob
import logging
import os
from datetime import datetime

from gcsfs.tests.perf.subsystembenchmarks._common import cli, report
from gcsfs.tests.perf.subsystembenchmarks.dataloading.amplification import (
    DEFAULT_SCRAPE_WAIT_SECONDS,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.bucket import BUCKET_TYPES
from gcsfs.tests.perf.subsystembenchmarks.dataloading.rapid_cache import (
    RAPID_CACHE_BUCKET_TYPES,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.rcu import (
    RCU_BUCKET_TYPES,
    rcu_state,
)


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
    os.environ["GCSFS_SUBSYSTEM_DEVICE"] = args.device
    if args.model_id:
        os.environ["GCSFS_SUBSYSTEM_MODEL_ID"] = args.model_id
    else:
        os.environ.pop("GCSFS_SUBSYSTEM_MODEL_ID", None)
    os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = "true"
    os.environ["STORAGE_EMULATOR_HOST"] = "https://storage.googleapis.com"
    if rcu_state(args.bucket_type) == "z":
        # *_rcu_z stages and saves RAPID objects, which gcsfs writes to RCU buckets
        # only with rcu_rapid_write. The suite's processes read it from the env.
        os.environ["FSSPEC_GCS_RCU_RAPID_WRITE"] = "true"
    else:
        os.environ.pop("FSSPEC_GCS_RCU_RAPID_WRITE", None)


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
        "--filter",
        default="",
        help="pytest -k filter expression to limit case execution",
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
        choices=BUCKET_TYPES,
        default="regional",
        help="storage tier used by every case in the run",
    )
    parser.add_argument(
        "--zone",
        help="placement zone; required when --bucket-type is zonal, rapid_cache_* or "
        "*_rcu_*",
    )
    parser.add_argument(
        "--model-id",
        default="",
        help="model id override (e.g. gs://path/to/model or hf-repo-id)",
    )
    parser.add_argument(
        "--device",
        choices=("cpu", "cuda"),
        default="cpu",
        help="execution device for benchmarks that support CPU/CUDA selection",
    )
    parser.add_argument(
        "--amplification-wait",
        type=int,
        default=DEFAULT_SCRAPE_WAIT_SECONDS,
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
        help="fail when no eligible row gets amplification metrics",
    )
    parser.add_argument(
        "--results-dir",
        default="",
        help="directory for results.json/results.csv (default: __run__/<timestamp>); "
        "concurrent runs need distinct directories",
    )
    return parser


def parse_args(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    zone_types = ("zonal", *RAPID_CACHE_BUCKET_TYPES, *RCU_BUCKET_TYPES)
    if args.bucket_type in zone_types and not args.zone:
        parser.error(f"--zone is required when --bucket-type={args.bucket_type}")
    if args.amplification_wait < 0:
        parser.error("--amplification-wait must be >= 0")
    if args.amplification_retry_wait < 0:
        parser.error("--amplification-retry-wait must be >= 0")
    groups = discover_groups()
    if args.group not in groups:
        parser.error(f"unknown --group {args.group!r}; available: {', '.join(groups)}")
    if rcu_state(args.bucket_type) in ("warm", "cold"):
        import yaml

        config_name = os.environ.get("GCSFS_SUBSYSTEM_CONFIG") or "configs.yaml"
        config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), args.group, config_name
        )
        if os.path.isfile(config_path):
            with open(config_path) as f:
                scenarios = (yaml.safe_load(f) or {}).get("scenarios") or []
            if scenarios and all("write" in s.get("scenario", "") for s in scenarios):
                parser.error(
                    f"--group={args.group} has only write scenarios, which are "
                    f"skipped on --bucket-type={args.bucket_type} "
                    "(use *_rcu_z for RCU checkpoint saves)"
                )
    return args


def enrich_amplification_with_retry(csv_path, project, client, *, retry_wait, sleep):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import amplification

    result = amplification.enrich_csv(csv_path, project, client=client)
    if result.missing_buckets:
        if retry_wait:
            sleep(retry_wait)
        result = amplification.enrich_csv(csv_path, project, client=client)
    return result


def require_any_amplification(result):
    """Fail on a scrape error, or when the scrape enriched nothing.

    Cloud Monitoring occasionally drops a DELTA point, and a dropped point never
    comes back on retry, so a few missing rows are expected. Those rows stay blank
    and are logged. A scrape that raised for a bucket, or no enriched row at all,
    means the scrape itself is broken.
    """
    if result.failed_buckets:
        raise RuntimeError(
            "read-amplification scrape errored for buckets: "
            + ", ".join(result.failed_buckets)
        )
    if result.eligible and not result.enriched:
        raise RuntimeError(
            "read-amplification metrics missing for every bucket: "
            + ", ".join(result.missing_buckets)
        )


def _scrape_amplification(csv_path, args):
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import amplification

    if not amplification.has_eligible_rows(csv_path):
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
            require_any_amplification(result)
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
    if args.results_dir:
        results_dir = args.results_dir
    else:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        results_dir = os.path.join(os.path.dirname(__file__), "__run__", timestamp)
    os.makedirs(results_dir, exist_ok=True)

    rc, csv_path = cli.run_suite(suite_dir, results_dir, filter_expr=args.filter)
    if csv_path is None:
        logging.error("no benchmark results produced by group %s", args.group)
        raise SystemExit(rc or 1)
    _scrape_amplification(csv_path, args)
    report.print_csv_to_shell(csv_path)
    logging.info("subsystembenchmarks run rc=%s csv=%s", rc, csv_path)
    raise SystemExit(rc)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    main()
