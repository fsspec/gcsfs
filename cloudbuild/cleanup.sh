#!/bin/bash
set -e

# Per-suite worker counts must match those used by create_buckets.sh so that every
# worker variant created is also deleted.
WORKERS_STANDARD="${WORKERS_STANDARD:-1}"
WORKERS_HNS="${WORKERS_HNS:-1}"
WORKERS_ZONAL="${WORKERS_ZONAL:-1}"
WORKERS_ZONAL_CORE="${WORKERS_ZONAL_CORE:-1}"
WORKERS_RCU="${WORKERS_RCU:-1}"
WORKERS_HNS_RCU="${WORKERS_HNS_RCU:-1}"
LEAKED_RCU_MAX_AGE_SECS="${LEAKED_RCU_MAX_AGE_SECS:-14400}"
BACKGROUND_PIDS=()

worker_count() {
    local workers="$1"
    if [[ "${workers}" =~ ^[0-9]+$ ]] && (( workers > 0 )); then
        echo "${workers}"
    else
        echo "1"
    fi
}

worker_suffixes() {
    local workers="$1"
    local count
    count="$(worker_count "${workers}")"

    for ((i = 0; i < count; i++)); do
        echo "-gw${i}"
    done
}

wait_for_background_jobs() {
    local pid
    local status=0

    for pid in "${BACKGROUND_PIDS[@]}"; do
        if ! wait "${pid}"; then
            echo "A background job failed!"
            status=1
        fi
    done

    BACKGROUND_PIDS=()
    return "${status}"
}

delete_bucket() {
    local bucket="$1"
    shift
    local err

    if err=$(gcloud storage rm --recursive "${bucket}" --project="${PROJECT_ID}" "$@" 2>&1); then
        return 0
    fi
    # Never created (e.g. create-buckets failed partway): nothing leaked.
    if [[ "${err}" == *"${bucket} not found: 404"* ]]; then
        return 0
    fi
    # gcloud's non-404 errors don't name the bucket, so prefix it here.
    echo "ERROR: failed to delete ${bucket}: ${err}" >&2
    return 1
}

delete_bucket_variants() {
    local workers="$1"
    local bucket_base="$2"
    shift 2

    while IFS= read -r suffix; do
        delete_bucket "gs://${bucket_base}${suffix}" "$@" &
        BACKGROUND_PIDS+=("$!")
    done < <(worker_suffixes "${workers}")
}

delete_rcu_bucket() {
    local bucket_name="$1"
    local cache_zone="$2"

    if gcloud storage buckets describe "gs://${bucket_name}" --project="${PROJECT_ID}" &>/dev/null; then
        gcloud storage rm --recursive "gs://${bucket_name}/*" --project="${PROJECT_ID}" --quiet 2>/dev/null || true
        gcloud alpha storage buckets rapid-caches disable "${bucket_name}/${cache_zone}" --project="${PROJECT_ID}" --quiet || true
        # rapid-caches disable runs asynchronously; if disablement is still in progress
        # when buckets delete runs, cleanup_leaked_rcu_buckets reaps the bucket later.
        gcloud storage buckets delete "gs://${bucket_name}" --project="${PROJECT_ID}" --quiet || true
    fi
}

delete_rcu_bucket_variants() {
    local workers="$1"
    local bucket_base="$2"
    local cache_zone="$3"

    while IFS= read -r suffix; do
        delete_rcu_bucket "${bucket_base}${suffix}" "${cache_zone}" &
        BACKGROUND_PIDS+=("$!")
    done < <(worker_suffixes "${workers}")
}

cleanup_leaked_rcu_buckets() {
    local current_time
    current_time="$(date +%s)"
    local buckets
    buckets="$(gcloud storage buckets list "gs://gcsfs-test-*rcu*" --project="${PROJECT_ID}" --format="value(name,creation_time)" --quiet 2>/dev/null || true)"

    while read -r name creation_time; do
        if [[ -z "${name}" ]]; then
            continue
        fi
        local clean_name="${name#gs://}"
        clean_name="${clean_name%/}"
        if [[ ! "${clean_name}" =~ ^gcsfs-test-(rcu|hns-rcu|rcu-cross-zone)-[0-9a-f]{8}(-gw[0-9]+)?$ ]]; then
            continue
        fi
        if [[ -n "${SHORT_BUILD_ID:-}" && "${clean_name}" == *"-${SHORT_BUILD_ID}"* ]]; then
            continue
        fi

        local creation_seconds
        creation_seconds="$(date -d "${creation_time}" +%s 2>/dev/null)" || continue
        local age=$((current_time - creation_seconds))
        if (( age <= LEAKED_RCU_MAX_AGE_SECS )); then
            continue
        fi

        echo "Deleting leaked RCU bucket: gs://${clean_name} (Age: ${age}s)"
        local caches
        caches="$(gcloud alpha storage buckets rapid-caches list "gs://${clean_name}" --project="${PROJECT_ID}" --format="value(id,state)" --quiet < /dev/null 2>/dev/null || true)"
        while read -r cache_id cache_state; do
            if [[ -z "${cache_id}" ]]; then
                continue
            fi
            if [[ "${cache_state,,}" != "disabled" ]]; then
                gcloud alpha storage buckets rapid-caches disable "${cache_id}" --project="${PROJECT_ID}" --quiet < /dev/null || true
            fi
        done <<< "${caches}"
        gcloud storage rm --recursive "gs://${clean_name}" --project="${PROJECT_ID}" --quiet < /dev/null ||
            gcloud storage buckets delete "gs://${clean_name}" --project="${PROJECT_ID}" --quiet < /dev/null || true
    done <<< "${buckets}"
}

echo "--- Deleting VM ---"
# The delete operation is run in the background so we don't block bucket cleanup on it.
if gcloud compute instances describe "gcsfs-test-vm-${SHORT_BUILD_ID}" --project="${PROJECT_ID}" --zone="${ZONE}" &>/dev/null; then
    gcloud compute instances delete "gcsfs-test-vm-${SHORT_BUILD_ID}" --project="${PROJECT_ID}" --zone="${ZONE}" --quiet &
    BACKGROUND_PIDS+=("$!")
fi

echo "--- Removing SSH key from OS Login ---"
if [[ -f /workspace/.ssh/google_compute_engine.pub ]]; then
  gcloud compute os-login ssh-keys remove --project="${PROJECT_ID}" --key-file=/workspace/.ssh/google_compute_engine.pub --quiet || true
fi

echo "--- Deleting buckets ---"
delete_bucket_variants "${WORKERS_STANDARD}" "gcsfs-test-standard-${SHORT_BUILD_ID}"
delete_bucket_variants "${WORKERS_STANDARD}" "gcsfs-test-versioned-${SHORT_BUILD_ID}"
delete_bucket_variants "${WORKERS_HNS}" "gcsfs-test-hns-${SHORT_BUILD_ID}"
delete_bucket_variants "${WORKERS_ZONAL}" "gcsfs-test-zonal-${SHORT_BUILD_ID}"
delete_bucket_variants "${WORKERS_ZONAL}" "gcsfs-test-standard-for-zonal-${SHORT_BUILD_ID}"
delete_bucket_variants "${WORKERS_ZONAL_CORE}" "gcsfs-test-zonal-core-${SHORT_BUILD_ID}"
delete_bucket_variants "${WORKERS_HNS}" "gcsfs-test-hns-req-pay-${SHORT_BUILD_ID}" --billing-project="${PROJECT_ID}"
delete_bucket_variants "${WORKERS_STANDARD}" "gcsfs-test-standard-req-pay-${SHORT_BUILD_ID}" --billing-project="${PROJECT_ID}"
delete_rcu_bucket_variants "${WORKERS_RCU}" "gcsfs-test-rcu-${SHORT_BUILD_ID}" "${ZONE}"
delete_rcu_bucket_variants "${WORKERS_HNS_RCU}" "gcsfs-test-hns-rcu-${SHORT_BUILD_ID}" "${ZONE}"
if [[ -n "${RCU_CROSS_ZONE:-}" ]]; then
    delete_rcu_bucket "gcsfs-test-rcu-cross-zone-${SHORT_BUILD_ID}" "${RCU_CROSS_ZONE}" &
    BACKGROUND_PIDS+=("$!")
fi

cleanup_leaked_rcu_buckets

if ! wait_for_background_jobs; then
    echo "ALERT: GCSFS E2E Cleanup Failed. Some testing resources may have leaked."
    exit 0
fi
