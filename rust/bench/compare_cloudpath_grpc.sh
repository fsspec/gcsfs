#!/usr/bin/env bash
#
# Benchmark Cloud-Path gRPC scaling across varying channel counts vs HTTP JSON.
# Reads 10 GiB per run on a given GCS object (e.g., gs://princer-bucket/10gfile.bin).
#

set -euo pipefail

BUCKET="${1:-princer-bucket}"
OBJECT="${2:-10gfile.bin}"
SIZE_BYTES="${3:-10737418240}" # 10 GiB default
CHUNK_SIZE="${CHUNK_SIZE:-16777216}" # 16 MiB default

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$SCRIPT_DIR/../bench_rust_read"

echo "Building bench_rust_read in release mode..."
cargo build --release --manifest-path "$BIN_DIR/Cargo.toml"

BIN="$BIN_DIR/target/release/bench_rust_read"

echo
echo "=================================================================================================="
echo " Cloud-Path gRPC (Channel Scaling) vs HTTP JSON Benchmark"
echo " Target: gs://${BUCKET}/${OBJECT} ($(( SIZE_BYTES / 1024 / 1024 / 1024 )) GiB)"
echo " Chunk Size: $(( CHUNK_SIZE / 1024 / 1024 )) MiB"
echo "=================================================================================================="
echo

printf "| %-16s | %-12s | %-12s | %-12s | %-14s | %-12s | %-10s |\n" \
    "Backend / Transp" "Channels" "Concurrency" "Data Read" "Throughput (MB/s)" "Line-Rate" "Peak RSS"
printf "|:%----------------|:-------------|:-------------|:-------------|:---------------|:-------------|:----------:|\n"

# 1. Benchmark HTTP JSON
http_json="$("$BIN" --bucket "$BUCKET" --object "$OBJECT" --size "$SIZE_BYTES" --transport http --concurrency 16 --chunk-size "$CHUNK_SIZE" --json)"
python3 -c "
import json
d = json.loads('''$http_json''')
print(f\"| {'HTTP JSON':<16} | {'-':<12} | {d['concurrency']:<12} | {d['gib_read']:>5.2f} GiB   | {d['throughput_mbps']:>11.2f} MB/s | {d['throughput_gbps']:>6.2f} Gbps | {d['peak_rss_mb']:>7.1f} MB |\")
"

# 2. Benchmark gRPC across channels
for chans in 1 2 4 8 16 32 64; do
    conc=$(( chans > 16 ? chans : 16 ))
    grpc_json="$("$BIN" --bucket "$BUCKET" --object "$OBJECT" --size "$SIZE_BYTES" --transport grpc --channels "$chans" --concurrency "$conc" --chunk-size "$CHUNK_SIZE" --json)"
    python3 -c "
import json
d = json.loads('''$grpc_json''')
print(f\"| {'gRPC Cloud-Path':<16} | {d['channels']:<12} | {d['concurrency']:<12} | {d['gib_read']:>5.2f} GiB   | {d['throughput_mbps']:>11.2f} MB/s | {d['throughput_gbps']:>6.2f} Gbps | {d['peak_rss_mb']:>7.1f} MB |\")
"
done

echo "=================================================================================================="
