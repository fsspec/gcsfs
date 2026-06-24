#!/usr/bin/env bash
# cleanup-helm: uninstall the workload release (best-effort).
if [[ "${_SKIP_CLEANUP}" == "true" ]]; then
  echo "Skipping cleanup-helm as requested."
  exit 0
fi
source "$(dirname "$0")/lib.sh"
source "${BUILD_VARS_FILE}"
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash || true
gcloud container clusters get-credentials "$CLUSTER_NAME" --zone="${_ZONE}" --project="${PROJECT_ID}" || true
helm uninstall "$RUN_ID" || true
