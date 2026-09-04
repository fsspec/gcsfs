#!/usr/bin/env bash
#
# Interleaved throughput/memory/CPU comparison of the gcsfs read backends.
#
# Runs alternate between backends so both see the same network conditions —
# sequential batches are not comparable, because GCS throughput drifts over
# time and the first run of a batch is consistently slower (warm-up).
#
# Usage:
#   rust/bench/compare_backends.sh gs://bucket/object
#
# Environment overrides:
#   RUNS=5                  iterations per backend
#   BACKENDS="rust fsspec"  backends to compare
#   PYTHON=python           interpreter to use
#   CONCURRENCY=16          parallel range requests per fetch
#   IO_SIZE=8388608         read chunk size (8 MiB)
#   PREFETCH=268435456      prefetcher readahead ceiling (256 MiB)
#   MALLOC_ARENA_MAX=4      caps glibc malloc arenas; see rust/RUST_SDK_POC.md

set -euo pipefail

URL="${1:-}"
if [[ -z "$URL" ]]; then
    echo "usage: $0 gs://bucket/object" >&2
    exit 1
fi

RUNS="${RUNS:-5}"
BACKENDS="${BACKENDS:-rust fsspec}"
PYTHON="${PYTHON:-python}"
CONCURRENCY="${CONCURRENCY:-16}"
IO_SIZE="${IO_SIZE:-8388608}"
PREFETCH="${PREFETCH:-268435456}"
export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-4}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
READER="$SCRIPT_DIR/../../data/read_gcs_file.py"
[[ -f "$READER" ]] || { echo "cannot find $READER" >&2; exit 1; }

# GNU time (not the shell builtin) reports peak RSS and CPU%.
TIME_BIN="${TIME_BIN:-/usr/bin/time}"
[[ -x "$TIME_BIN" ]] || { echo "need GNU time at $TIME_BIN" >&2; exit 1; }

RESULTS="$(mktemp)"
trap 'rm -f "$RESULTS"' EXIT

echo "object      : $URL"
echo "config      : concurrency=$CONCURRENCY io_size=$IO_SIZE prefetch=$PREFETCH"
echo "arenas      : MALLOC_ARENA_MAX=$MALLOC_ARENA_MAX"
echo "runs        : $RUNS per backend, interleaved"
echo
printf '%-4s %-8s %14s %14s %8s\n' seq backend throughput peak_rss cpu
printf '%-4s %-8s %14s %14s %8s\n' ---- ------- ---------- -------- ---

for ((i = 1; i <= RUNS; i++)); do
    for backend in $BACKENDS; do
        out="$("$TIME_BIN" "$PYTHON" "$READER" \
            --url "$URL" \
            --backend "$backend" \
            --concurrency "$CONCURRENCY" \
            --io-size "$IO_SIZE" \
            --max-prefetch-size "$PREFETCH" 2>&1)"

        mbps="$(grep -oE 'throughput: [0-9.]+' <<<"$out" | grep -oE '[0-9.]+' || echo 0)"
        rss_kb="$(grep -oE '[0-9]+maxresident' <<<"$out" | grep -oE '[0-9]+' || echo 0)"
        cpu="$(grep -oE '[0-9]+%CPU' <<<"$out" || echo 'n/a')"

        if [[ "$mbps" == "0" ]]; then
            echo "run failed for backend=$backend:" >&2
            echo "$out" >&2
            exit 1
        fi

        printf '%-4s %-8s %11s MB/s %11s MB %8s\n' \
            "$i" "$backend" "$mbps" "$((rss_kb / 1024))" "$cpu"
        echo "$backend $mbps $rss_kb" >>"$RESULTS"
    done
done

echo
echo "mean over $RUNS runs:"
for backend in $BACKENDS; do
    awk -v b="$backend" '
        $1 == b { n++; mb += $2; rss += $3; if (mn == "" || $2 < mn) mn = $2; if ($2 > mx) mx = $2 }
        END {
            if (!n) exit
            m = mb / n
            # spread matters: rust throughput is far noisier than fsspec, so a
            # single number is misleading. See rust/RUST_SDK_POC.md.
            printf "  %-8s %8.1f MB/s (range %.1f-%.1f)   %6.0f MB peak RSS\n",
                   b, m, mn, mx, rss / n / 1024
        }' "$RESULTS"
done
