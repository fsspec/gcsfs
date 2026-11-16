#!/usr/bin/env bash
# delete-buckets: delete the per-run checkpoint and dataset buckets (best-effort).
# The shared results bucket is intentionally left in place.
if [[ "${_SKIP_CLEANUP}" == "true" ]]; then
  echo "Skipping delete-buckets as requested."
  exit 0
fi
source "$(dirname "$0")/lib.sh"
source "${BUILD_VARS_FILE}"
delete_bucket_and_caches "$CHECKPOINT_BUCKET"
delete_bucket_and_caches "$DATASET_BUCKET"
