#!/bin/bash
set -e
# Creates all GCS buckets (or their per-xdist-worker variants) needed by the
# integration tests.
#
# Split out of the former create_resources.sh so that bucket creation runs in
# parallel with VM provisioning/setup rather than serially before it.
#
# Per-suite worker counts: each suite runs `pytest -n <N>` and each xdist worker
# uses its own bucket variant (`<base>-gw<i>`, see gcsfs/tests/settings.py). Each
# bucket group is used by exactly one suite, so we create that group's bucket names
# at the suite's worker count. These MUST match the per-suite PYTEST_XDIST_WORKERS
# passed to the test steps, or workers will reference buckets that don't exist.
#
# Initialize gcloud configuration sequentially.
# This prevents a race condition (SQLite DB locking) where multiple parallel
# background tasks attempt to initialize ~/.config/gcloud/ simultaneously,
# which leads to "You do not currently have an active account selected." errors.
gcloud config list > /dev/null

WORKERS_STANDARD="${WORKERS_STANDARD:-1}"
WORKERS_HNS="${WORKERS_HNS:-1}"
WORKERS_ZONAL="${WORKERS_ZONAL:-1}"
WORKERS_ZONAL_CORE="${WORKERS_ZONAL_CORE:-1}"
WORKERS_RCU="${WORKERS_RCU:-1}"
WORKERS_HNS_RCU="${WORKERS_HNS_RCU:-1}"
BUCKET_CREATE_MAX_ATTEMPTS="${BUCKET_CREATE_MAX_ATTEMPTS:-4}"
BUCKET_CREATE_RETRY_MAX_SLEEP="${BUCKET_CREATE_RETRY_MAX_SLEEP:-8}"
BACKGROUND_PIDS=()

worker_count() {
    local workers="$1"
    if [[ "${workers}" =~ ^[0-9]+$ ]] && (( workers > 0 )); then
        echo "${workers}"
    else
        echo "1"
    fi
}

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
    local count
    count="$(worker_count "${workers}")"

    for ((i = 0; i < count; i++)); do
        echo "-gw${i}"
    done
}

create_bucket_with_retry() {
    local bucket_uri="$1"
    shift
    local attempt=0

    until gcloud storage buckets create "${bucket_uri}" \
        --project="${PROJECT_ID}" \
        --location="${REGION}" \
        "$@"; do
        attempt=$((attempt + 1))
        if (( attempt >= BUCKET_CREATE_MAX_ATTEMPTS )); then
            echo "ERROR: Failed to create ${bucket_uri} after ${attempt} attempts." >&2
            return 1
        fi
        echo "Bucket create failed for ${bucket_uri} (attempt ${attempt}/${BUCKET_CREATE_MAX_ATTEMPTS}), retrying..." >&2
        if (( BUCKET_CREATE_RETRY_MAX_SLEEP > 0 )); then
            sleep $((RANDOM % BUCKET_CREATE_RETRY_MAX_SLEEP + 3))
        fi
    done
}

create_bucket_variants() {
    local workers="$1"
    local bucket_base="$2"
    shift 2

    while IFS= read -r suffix; do
        create_bucket_with_retry "gs://${bucket_base}${suffix}" "$@" &
        BACKGROUND_PIDS+=("$!")
    done < <(worker_suffixes "${workers}")
}

create_rapid_cache_with_retry() {
    local bucket_uri="$1"
    local cache_zone="$2"
    local attempt=0
    local output
    local state

    until output="$(gcloud alpha storage buckets rapid-caches create "${bucket_uri}" "${cache_zone}" \
        --project="${PROJECT_ID}" \
        --cache-type=rapid-cache-ultra \
        --admission-policy=admit-on-first-miss \
        --no-async \
        --quiet 2>&1)"; do
        echo "${output}" >&2
        # An earlier attempt may have created the cache before it failed, so
        # wait for that cache to run instead of treating it as ready.
        if (( attempt > 0 )) && grep -qi "already exists" <<< "${output}"; then
            for _ in $(seq 1 60); do
                state="$(gcloud alpha storage buckets rapid-caches describe "${bucket_uri#gs://}/${cache_zone}" \
                    --project="${PROJECT_ID}" --format='value(state)' 2>/dev/null || true)"
                [[ "${state^^}" == RUNNING ]] && return 0
                [[ -n "${state}" && "${state^^}" != CREATING ]] && break
                sleep 5
            done
        fi
        attempt=$((attempt + 1))
        if (( attempt >= BUCKET_CREATE_MAX_ATTEMPTS )); then
            echo "ERROR: Failed to create rapid cache for ${bucket_uri} in ${cache_zone} after ${attempt} attempts." >&2
            return 1
        fi
        echo "Rapid cache create failed for ${bucket_uri} in ${cache_zone} (attempt ${attempt}/${BUCKET_CREATE_MAX_ATTEMPTS}), retrying..." >&2
        if (( BUCKET_CREATE_RETRY_MAX_SLEEP > 0 )); then
            sleep $((RANDOM % BUCKET_CREATE_RETRY_MAX_SLEEP + 3))
        fi
    done
    echo "${output}"
}

create_rcu_bucket() {
    local bucket_uri="$1"
    local cache_zone="$2"
    shift 2

    create_bucket_with_retry "${bucket_uri}" --uniform-bucket-level-access "$@" &&
        create_rapid_cache_with_retry "${bucket_uri}" "${cache_zone}"
}

create_rcu_bucket_variants() {
    local workers="$1"
    local bucket_base="$2"
    local cache_zone="$3"
    shift 3

    while IFS= read -r suffix; do
        create_rcu_bucket "gs://${bucket_base}${suffix}" "${cache_zone}" "$@" &
        BACKGROUND_PIDS+=("$!")
    done < <(worker_suffixes "${workers}")
}

update_bucket_variants() {
    local workers="$1"
    local bucket_base="$2"
    shift 2

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

echo "--- Creating flat RCU bucket ---"
create_rcu_bucket_variants "${WORKERS_RCU}" "gcsfs-test-rcu-${SHORT_BUILD_ID}" "${ZONE}"

echo "--- Creating HNS RCU bucket ---"
create_rcu_bucket_variants "${WORKERS_HNS_RCU}" "gcsfs-test-hns-rcu-${SHORT_BUILD_ID}" "${ZONE}" \
    --enable-hierarchical-namespace

if [[ -n "${RCU_CROSS_ZONE:-}" ]]; then
    echo "--- Creating cross-zone flat RCU bucket ---"
    create_rcu_bucket "gs://gcsfs-test-rcu-cross-zone-${SHORT_BUILD_ID}" "${RCU_CROSS_ZONE}" &
    BACKGROUND_PIDS+=("$!")
fi

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
