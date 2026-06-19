#!/usr/bin/env bash
# cleanup-cluster: tear down the ephemeral GKE cluster (best-effort).
if [[ "${_SKIP_CLEANUP}" == "true" ]]; then
  echo "Skipping cleanup-cluster as requested."
  exit 0
fi
source "$(dirname "$0")/lib.sh"
source "${BUILD_VARS_FILE}"
gcloud container clusters delete "$CLUSTER_NAME" --zone="${_ZONE}" --project="${PROJECT_ID}" --quiet || true
