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


def _build_parser():
    parser = argparse.ArgumentParser(description="Run gcsfs subsystem benchmarks.")
    parser.add_argument(
        "--group", required=True, help="e.g. dataloading/huggingface_datasets"
    )
    return parser


def parse_args(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    groups = discover_groups()
    if args.group not in groups:
        parser.error(f"unknown --group {args.group!r}; available: {', '.join(groups)}")
    return args


def main(argv=None):
    args = parse_args(argv)
    os.environ["GCSFS_SUBSYSTEM_GROUP"] = args.group
    suite_dir = os.path.join(os.path.dirname(__file__), args.group)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    results_dir = os.path.join(os.path.dirname(__file__), "__run__", timestamp)
    os.makedirs(results_dir, exist_ok=True)

    rc, csv_path = cli.run_suite(suite_dir, results_dir)
    if csv_path is None:
        logging.error("no benchmark results produced by group %s", args.group)
        raise SystemExit(rc or 1)
    logging.info("subsystembenchmarks run rc=%s csv=%s", rc, csv_path)
    raise SystemExit(rc)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    main()
