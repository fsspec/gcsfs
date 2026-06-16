#!/bin/bash
set -e
# Creates all GCS buckets (and their per-xdist-worker variants) needed by the
# integration tests.
#
# Split out of the former create_resources.sh so that bucket creation runs in
# parallel with VM provisioning/setup rather than serially before it.
#
# Per-suite worker counts: each suite runs `pytest -n <N>` and each xdist worker
# uses its own bucket variant (`<base>-gw<i>`, see gcsfs/tests/settings.py). Each
# bucket group is used by exactly one suite, so we create that group's variants at
# the suite's worker count. These MUST match the per-suite PYTEST_XDIST_WORKERS
# passed to the test steps, or workers will reference buckets that don't exist.
#
# Note: Variables like $PROJECT_ID, $REGION, $ZONE are passed via 'env' in cloudbuild.yaml

WORKERS_STANDARD="${WORKERS_STANDARD:-1}"
WORKERS_HNS="${WORKERS_HNS:-1}"
WORKERS_ZONAL="${WORKERS_ZONAL:-1}"
WORKERS_ZONAL_CORE="${WORKERS_ZONAL_CORE:-1}"
BACKGROUND_PIDS=()

wait_for_background_jobs() {
    local pid
    local status=0

    for pid in "${BACKGROUND_PIDS[@]}"; do
        if ! wait "${pid}"; then
            status=1
        fi
    done

    BACKGROUND_PIDS=()
    return "${status}"
}

worker_suffixes() {
    local workers="$1"
    if [[ "${workers}" =~ ^[0-9]+$ ]] && (( workers > 1 )); then
        for ((i = 0; i < workers; i++)); do
            echo "-gw${i}"
        done
    fi
}

create_bucket_variants() {
    local workers="$1"
    local bucket_base="$2"
    shift 2

    gcloud storage buckets create "gs://${bucket_base}" \
        --project="${PROJECT_ID}" \
        --location="${REGION}" \
        "$@" &
    BACKGROUND_PIDS+=("$!")

    while IFS= read -r suffix; do
        gcloud storage buckets create "gs://${bucket_base}${suffix}" \
            --project="${PROJECT_ID}" \
            --location="${REGION}" \
            "$@" &
        BACKGROUND_PIDS+=("$!")
    done < <(worker_suffixes "${workers}")
}

update_bucket_variants() {
    local workers="$1"
    local bucket_base="$2"
    shift 2

    gcloud storage buckets update "gs://${bucket_base}" "$@" &
    BACKGROUND_PIDS+=("$!")

    while IFS= read -r suffix; do
        gcloud storage buckets update "gs://${bucket_base}${suffix}" "$@" &
        BACKGROUND_PIDS+=("$!")
    done < <(worker_suffixes "${workers}")
}

echo "--- Creating standard bucket ---"
create_bucket_variants "${WORKERS_STANDARD}" "gcsfs-test-standard-${SHORT_BUILD_ID}"

echo "--- Creating standard requester pays bucket ---"
create_bucket_variants "${WORKERS_STANDARD}" "gcsfs-test-standard-req-pay-${SHORT_BUILD_ID}"

echo "--- Creating versioned bucket ---"
create_bucket_variants "${WORKERS_STANDARD}" "gcsfs-test-versioned-${SHORT_BUILD_ID}"

echo "--- Creating HNS bucket ---"
create_bucket_variants "${WORKERS_HNS}" "gcsfs-test-hns-${SHORT_BUILD_ID}" \
    --enable-hierarchical-namespace \
    --uniform-bucket-level-access

echo "--- Creating HNS requester pays bucket ---"
create_bucket_variants "${WORKERS_HNS}" "gcsfs-test-hns-req-pay-${SHORT_BUILD_ID}" \
    --enable-hierarchical-namespace \
    --uniform-bucket-level-access

echo "--- Creating Zonal bucket ---"
create_bucket_variants "${WORKERS_ZONAL}" "gcsfs-test-zonal-${SHORT_BUILD_ID}" \
    --placement="${ZONE}" \
    --default-storage-class=RAPID \
    --enable-hierarchical-namespace \
    --uniform-bucket-level-access

echo "--- Creating standard bucket for Zonal test ---"
create_bucket_variants "${WORKERS_ZONAL}" "gcsfs-test-standard-for-zonal-${SHORT_BUILD_ID}"

# Use a separate bucket for running core tests to avoid exceeding object rate limit
echo "--- Creating Zonal bucket for running core tests ---"
create_bucket_variants "${WORKERS_ZONAL_CORE}" "gcsfs-test-zonal-core-${SHORT_BUILD_ID}" \
    --placement="${ZONE}" \
    --default-storage-class=RAPID \
    --enable-hierarchical-namespace \
    --uniform-bucket-level-access

# Wait for all background bucket creation jobs to finish
wait_for_background_jobs

echo "--- Enabling versioning on versioned bucket ---"
update_bucket_variants "${WORKERS_STANDARD}" "gcsfs-test-versioned-${SHORT_BUILD_ID}" \
    --versioning

echo "--- Enabling requester pays on HNS bucket ---"
update_bucket_variants "${WORKERS_HNS}" "gcsfs-test-hns-req-pay-${SHORT_BUILD_ID}" \
    --requester-pays

echo "--- Enabling requester pays on standard bucket ---"
update_bucket_variants "${WORKERS_STANDARD}" "gcsfs-test-standard-req-pay-${SHORT_BUILD_ID}" \
    --requester-pays

wait_for_background_jobs
