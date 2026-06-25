#!/usr/bin/env bash
# cleanup-cluster: tear down the ephemeral GKE cluster (best-effort).
if [[ "${_SKIP_CLEANUP}" == "true" ]]; then
  echo "Skipping cleanup-cluster as requested."
  exit 0
fi
source "$(dirname "$0")/lib.sh"
source "${BUILD_VARS_FILE}"
gcloud container clusters delete "$CLUSTER_NAME" --zone="${_ZONE}" --project="${PROJECT_ID}" --quiet || true

echo "--- Deleting dedicated subnetwork: ${SUBNET_NAME} ---"
gcloud compute networks subnets delete "${SUBNET_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" --quiet || true

echo "--- Deleting dedicated VPC network: ${NETWORK_NAME} ---"
gcloud compute networks delete "${NETWORK_NAME}" \
  --project="${PROJECT_ID}" --quiet || true
