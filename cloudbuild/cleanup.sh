#!/bin/bash
set -e

# Per-suite worker counts must match those used by create_buckets.sh so that every
# variant created is also deleted (no orphaned buckets).
WORKERS_STANDARD="${WORKERS_STANDARD:-1}"
WORKERS_HNS="${WORKERS_HNS:-1}"
WORKERS_ZONAL="${WORKERS_ZONAL:-1}"
WORKERS_ZONAL_CORE="${WORKERS_ZONAL_CORE:-1}"

worker_suffixes() {
    local workers="$1"
    if [[ "${workers}" =~ ^[0-9]+$ ]] && (( workers > 1 )); then
        for ((i = 0; i < workers; i++)); do
            echo "-gw${i}"
        done
    fi
}

delete_bucket_variants() {
    local workers="$1"
    local bucket_base="$2"
    shift 2

    gcloud storage rm --recursive "gs://${bucket_base}" "$@" || true &

    while IFS= read -r suffix; do
        gcloud storage rm --recursive "gs://${bucket_base}${suffix}" "$@" || true &
    done < <(worker_suffixes "${workers}")
}

echo "--- Deleting VM ---"
# --async: the delete operation is durable server-side and completes regardless of
# whether this build step is still running, so we don't block cleanup on it. (Bucket
# deletes below must stay synchronous; killing those mid-delete would orphan objects.)
gcloud compute instances delete "gcsfs-test-vm-${SHORT_BUILD_ID}" --zone="${ZONE}" --quiet --async || true

echo "--- Removing SSH key from OS Login ---"
if [[ -f /workspace/.ssh/google_compute_engine.pub ]]; then
  gcloud compute os-login ssh-keys remove --key-file=/workspace/.ssh/google_compute_engine.pub --quiet || true
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
wait
