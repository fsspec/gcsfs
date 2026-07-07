#!/usr/bin/env bash
# delete-buckets: delete the per-run checkpoint and dataset buckets (best-effort).
# The shared results bucket is intentionally left in place.
if [[ "${_SKIP_CLEANUP}" == "true" ]]; then
  echo "Skipping delete-buckets as requested."
  exit 0
fi
source "$(dirname "$0")/lib.sh"
source "${BUILD_VARS_FILE}"
gcloud storage rm --recursive --project="${PROJECT_ID}" gs://$CHECKPOINT_BUCKET || true
gcloud storage rm --recursive --project="${PROJECT_ID}" gs://$DATASET_BUCKET || true
