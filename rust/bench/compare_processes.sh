#!/usr/bin/env bash
#
# Multi-Process Benchmark comparing Rust SDK vs fsspec (aiohttp) backend
# across 1, 2, 4, 8, 16, 32, 48 processes, each reading a distinct 10GB file from GCS.

set -euo pipefail

PROCS_LIST="${PROCS_LIST:-1 2 4 8 16 32 48}"
BACKENDS="${BACKENDS:-rust fsspec}"
PYTHON="/home/princer_google_com/dev/gcsfs/.env/bin/python"
BENCH_SCRIPT="/home/princer_google_com/dev/gcsfs/rust/bench/bench_multiprocess.py"
URL_PATTERN="gs://princer-bucket/test_10g/file_{}.bin"
export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-4}"

RESULTS_FILE="/tmp/proc_bench_results.txt"
rm -f "$RESULTS_FILE"

echo "=================================================================================================="
echo "GCSFS Multi-Process 10GB Read Benchmark"
echo "Machine: $(hostname) ($(nproc) vCPUs)"
echo "Process Counts: $PROCS_LIST"
echo "Backends: $BACKENDS"
echo "URL Pattern: $URL_PATTERN"
echo "MALLOC_ARENA_MAX: $MALLOC_ARENA_MAX"
echo "=================================================================================================="
echo

for p in $PROCS_LIST; do
    echo ">>> Running benchmark for processes=$p <<<"
    for backend in $BACKENDS; do
        echo -n "Running backend=$backend (processes=$p)... "
        out=$($PYTHON "$BENCH_SCRIPT" \
            --processes "$p" \
            --backend "$backend" \
            --url-pattern "$URL_PATTERN")
        echo "Done."
        echo "$out"
        echo "$out" >> "$RESULTS_FILE"
    done
    echo "--------------------------------------------------------------------------------------------------"
done

echo
echo "=================================================================================================="
echo "COMPLETE MULTI-PROCESS BENCHMARK RESULTS"
echo "=================================================================================================="
cat "$RESULTS_FILE"
