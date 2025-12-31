import argparse
import csv
import json
import logging
import os
import subprocess
import sys
from datetime import datetime

import numpy as np
from conftest import MB
from prettytable import PrettyTable


def _setup_environment(args):
    """
    Validate command-line arguments and configure environment variables.

    This function checks for required arguments (like bucket names) and sets
    up the necessary environment variables for the benchmark execution.

    Args:
        args (argparse.Namespace): The parsed command-line arguments.

    """
    # Validate that at least one bucket is provided
    if not any([args.regional_bucket, args.zonal_bucket, args.hns_bucket]):
        logging.error(
            "At least one of --regional-bucket, --zonal-bucket, or --hns-bucket must be provided."
        )
        sys.exit(1)

    # Set environment variables for buckets
    os.environ["GCSFS_TEST_BUCKET"] = (
        args.regional_bucket if args.regional_bucket else ""
    )
    os.environ["GCSFS_ZONAL_TEST_BUCKET"] = (
        args.zonal_bucket if args.zonal_bucket else ""
    )
    os.environ["GCSFS_HNS_TEST_BUCKET"] = args.hns_bucket if args.hns_bucket else ""
    os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = "true"
    os.environ["STORAGE_EMULATOR_HOST"] = "https://storage.googleapis.com"
    os.environ["GCSFS_BENCHMARK_SKIP_TESTS"] = "false"

    if args.config:
        os.environ["GCSFS_BENCHMARK_FILTER"] = ",".join(args.config)


def _run_benchmarks(results_dir, args):
    """Execute the benchmark suite using pytest.

    This function constructs and runs a pytest command to execute the benchmarks.
    It captures the output in a JSON file and handles logging and test filtering
    based on the provided arguments.

    Args:
        results_dir (str): The directory where benchmark results will be saved.
        args (argparse.Namespace): The parsed command-line arguments.

    Returns:
        str: The path to the generated JSON results file.
    """
    logging.info(f"Starting benchmark run for group: {args.group}")

    base_path = os.path.dirname(__file__)
    if args.group:
        benchmark_path = os.path.join(base_path, args.group)
        if not os.path.isdir(benchmark_path):
            logging.error(f"Benchmark group directory not found: {benchmark_path}")
            sys.exit(1)
    else:
        benchmark_path = base_path

    json_output_path = os.path.join(results_dir, "results.json")

    pytest_command = [
        sys.executable,
        "-m",
        "pytest",
        benchmark_path,
        f"--benchmark-json={json_output_path}",
    ]

    if args.log:
        pytest_command.extend(
            [
                "-o",
                f"log_cli={args.log}",
                "-o",
                f"log_cli_level={args.log_level.upper()}",
            ]
        )

    logging.info(f"Executing command: {' '.join(pytest_command)}")

    try:
        env = os.environ.copy()
        subprocess.run(pytest_command, check=True, env=env, text=True)
        logging.info(f"Benchmark run completed. Results saved to {json_output_path}")
    except subprocess.CalledProcessError as e:
        logging.error(
            f"Benchmark run completed with error: {e}, results saved to {json_output_path}"
        )
    except FileNotFoundError:
        logging.error(
            "pytest not found. Please ensure it is installed in your environment."
        )
        sys.exit(1)

    return json_output_path


def _process_benchmark_result(bench, headers, extra_info_headers, stats_headers):
    """
    Process a single benchmark result and prepare it for CSV reporting.

    This function extracts relevant statistics and metadata from a benchmark
    run, calculates derived metrics like percentiles and throughput, and
    formats it as a dictionary.

    Args:
        bench (dict): The dictionary for a single benchmark from the JSON output.
        headers (list): The list of all header names for the CSV.
        extra_info_headers (list): Headers from the 'extra_info' section.
        stats_headers (list): Headers from the 'stats' section.

    """
    row = {h: "" for h in headers}
    row["name"] = bench["name"]
    row["group"] = bench.get("group", "")

    # Populate extra_info and stats
    for key in extra_info_headers:
        row[key] = bench["extra_info"].get(key)
    for key in stats_headers:
        row[key] = bench["stats"].get(key)

    # Calculate percentiles
    timings = bench["stats"].get("data")
    if timings:
        row["p90"] = np.percentile(timings, 90)
        row["p95"] = np.percentile(timings, 95)
        row["p99"] = np.percentile(timings, 99)

    # Calculate max throughput
    file_size = bench["extra_info"].get("file_size", 0)
    num_files = bench["extra_info"].get("num_files", 1)

    if file_size != "N/A":
        total_bytes = file_size * num_files

        min_time = bench["stats"].get("min")
        if min_time and min_time > 0:
            row["max_throughput_mb_s"] = (total_bytes / min_time) / MB
        else:
            row["max_throughput_mb_s"] = "0.0"
    else:
        row["max_throughput_mb_s"] = "N/A"

    return row


def _generate_report(json_path, results_dir):
    """Generate a CSV summary report from the pytest-benchmark JSON output.

    Args:
        json_path (str): The path to the JSON file containing benchmark results.
        results_dir (str): The directory where the CSV report will be saved.

    Returns:
        str: The path to the generated CSV report file.

    """
    logging.info(f"Generating CSV report from {json_path}")

    with open(json_path, "r") as f:
        data = json.load(f)

    report_path = os.path.join(results_dir, "results.csv")

    # Dynamically get headers from the first benchmark's extra_info and stats
    first_benchmark = data["benchmarks"][0]
    extra_info_headers = sorted(first_benchmark["extra_info"].keys())
    stats_headers = ["min", "max", "mean", "median", "stddev"]
    custom_headers = ["p90", "p95", "p99", "max_throughput_mb_s"]

    headers = ["name", "group"] + extra_info_headers + stats_headers + custom_headers

    with open(report_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for bench in data["benchmarks"]:
            row = _process_benchmark_result(
                bench, headers, extra_info_headers, stats_headers
            )
            writer.writerow([row[h] for h in headers])

    logging.info(f"CSV report generated at {report_path}")

    return report_path


def _format_mb(value):
    if value == "N/A":
        return "N/A"
    return f"{float(value) / MB:.2f}"


def _create_table_row(row):
    """
    Format a dictionary of benchmark results into a list for table display.

    Args:
        row (dict): A dictionary representing a single row from the CSV report.

    Returns:
        list: A list of formatted values ready for printing in a table.

    """
    return [
        row.get("bucket_type", ""),
        row.get("group", ""),
        row.get("pattern", ""),
        row.get("num_files", ""),
        row.get("threads", ""),
        row.get("processes", ""),
        row.get("depth", ""),
        _format_mb(row.get("file_size", 0)),
        _format_mb(row.get("chunk_size", 0)),
        _format_mb(row.get("block_size", 0)),
        f"{float(row.get('min', 0)):.4f}",
        f"{float(row.get('mean', 0)):.4f}",
        float(row.get("max_throughput_mb_s", 0)),
        f"{float(row.get('cpu_max_global', 0)):.2f}",
        f"{float(row.get('mem_max', 0)) / MB:.2f}",
    ]


def _print_csv_to_shell(report_path):
    """Read a CSV report and print it to the console as a formatted table.

    Args:
        report_path (str): The path to the CSV report file.

    """
    try:
        with open(report_path, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            logging.info("No data to display.")
            return

        # Define the headers for the output table
        display_headers = [
            "Bucket Type",
            "Group",
            "Pattern",
            "Files",
            "Threads",
            "Processes",
            "Depth",
            "File Size (MB)",
            "Chunk Size (MB)",
            "Block Size (MB)",
            "Min Latency (s)",
            "Mean Latency (s)",
            "Max Throughput(MB/s)",
            "Max CPU (%)",
            "Max Memory (MB)",
        ]
        table = PrettyTable()
        table.field_names = display_headers

        for row in rows:
            table.add_row(_create_table_row(row))
        print(table)
    except FileNotFoundError:
        logging.error(f"Report file not found at: {report_path}")


def main():
    """
    Parse command-line arguments and orchestrate the benchmark execution.

    This is the main entry point of the script. It sets up the environment,
    runs the benchmarks, generates reports, and prints a summary to the console.

    """
    parser = argparse.ArgumentParser(description="Run GCSFS performance benchmarks.")
    parser.add_argument(
        "--group",
        help="The benchmark group to run (e.g., 'read'). Runs all if not specified.",
    )
    parser.add_argument(
        "--config",
        nargs="+",
        help="The name(s) of the benchmark configuration(s) to run(e.g., --config read_seq_1thread,read_rand_1thread).",
    )
    parser.add_argument(
        "--regional-bucket",
        help="Name of the regional GCS bucket to use for benchmarks.",
    )
    parser.add_argument(
        "--zonal-bucket",
        help="Name of the zonal GCS bucket to use for benchmarks.",
    )
    parser.add_argument(
        "--hns-bucket",
        help="Name of the HNS GCS bucket to use for benchmarks.",
    )
    parser.add_argument(
        "--log",
        default="false",
        help="Enable pytest console logging (log_cli=true).",
    )
    parser.add_argument(
        "--log-level",
        default="DEBUG",
        help="Set pytest console logging level (e.g., DEBUG, INFO, WARNING). Only effective if --log is enabled.",
    )
    args = parser.parse_args()

    _setup_environment(args)

    # Create results directory
    timestamp = datetime.now().strftime("%d%m%Y-%H%M%S")
    results_dir = os.path.join(os.path.dirname(__file__), "__run__", timestamp)
    os.makedirs(results_dir, exist_ok=True)

    # Run benchmarks and generate report
    json_result_path = _run_benchmarks(results_dir, args)
    if json_result_path:
        csv_report_path = _generate_report(json_result_path, results_dir)
        if csv_report_path:
            _print_csv_to_shell(csv_report_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    main()
