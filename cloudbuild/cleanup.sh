#!/bin/bash
set -e

# Per-suite worker counts must match those used by create_buckets.sh so that every
# worker variant created is also deleted.
WORKERS_STANDARD="${WORKERS_STANDARD:-1}"
WORKERS_HNS="${WORKERS_HNS:-1}"
WORKERS_ZONAL="${WORKERS_ZONAL:-1}"
WORKERS_ZONAL_CORE="${WORKERS_ZONAL_CORE:-1}"
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
    err="$(mktemp)"

    if gcloud storage rm --recursive "${bucket}" --project="${PROJECT_ID}" "$@" 2> "${err}"; then
        rm -f "${err}"
        return 0
    fi
    # 404: the bucket was never created (e.g. create-buckets failed early).
    if grep -q "not found: 404" "${err}"; then
        rm -f "${err}"
        return 0
    fi
    # Anything else (e.g. a 403 on the requester-pays billing project) means the
    # bucket leaks. Report it instead of swallowing it: leaked buckets that are
    # deleted after more than 14 days get flagged by security scanners as dangling.
    echo "ERROR: failed to delete ${bucket}: $(cat "${err}")" >&2
    rm -f "${err}"
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

if ! wait_for_background_jobs; then
    echo "ALERT: GCSFS E2E Cleanup Failed. Some testing resources may have leaked."
    # Surface the leak in the check-failure step instead of passing silently.
    FAILED_FILE="${FAILED_FILE:-/workspace/FAILED}"
    if [[ -d "$(dirname "${FAILED_FILE}")" ]]; then
        echo "cleanup (leaked resources)" >> "${FAILED_FILE}"
    fi
    exit 0
fi
