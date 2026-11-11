#!/usr/bin/env bash
# cleanup-cluster: tear down the ephemeral GKE cluster (best-effort).
if [[ "${_SKIP_CLEANUP}" == "true" ]]; then
  echo "Skipping cleanup-cluster as requested."
  exit 0
fi
source "$(dirname "$0")/lib.sh"
source "${BUILD_VARS_FILE}"
echo "--- Deleting GKE cluster: ${CLUSTER_NAME} ---"
# create-cluster may have tried several zones, and a failed attempt's delete is
# best-effort, so look the cluster up by name and delete it wherever it exists.
# If the lookup itself fails, fall back to the recorded location. An empty
# lookup means there is nothing to delete.
if LOCATIONS=$(gcloud container clusters list --project="${PROJECT_ID}" \
    --filter="name=${CLUSTER_NAME}" --format="value(location)"); then
  [ -z "$LOCATIONS" ] && echo "No cluster ${CLUSTER_NAME} found; skipping."
else
  LOCATIONS="${CLUSTER_LOCATION:-${_ZONE}}"
fi
# Best-effort: just retry the delete until it succeeds. A delete can transiently
# fail (e.g. FAILED_PRECONDITION while a cluster operation is still settling);
# retrying after a pause handles that without inspecting cluster/operation state.
# Retry generously -- this is the last cleanup step and blocks nothing, so it's
# worth being patient to release the cluster's resources rather than leaking them.
for loc in $LOCATIONS; do
  for i in {1..20}; do
    if gcloud container clusters delete "$CLUSTER_NAME" --zone="${loc}" --project="${PROJECT_ID}" --quiet; then
      echo "Cluster ${CLUSTER_NAME} in ${loc} deleted successfully."
      break
    fi
    echo "Attempt $i to delete cluster ${CLUSTER_NAME} in ${loc} failed. Retrying in 30 seconds..."
    sleep 30
  done
done

echo "--- Deleting dedicated subnetwork: ${SUBNET_NAME} ---"
gcloud compute networks subnets delete "${SUBNET_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" --quiet || true

echo "--- Deleting firewall rules on network: ${NETWORK_NAME} ---"
FIREWALLS=$(gcloud compute firewall-rules list --project="${PROJECT_ID}" --filter="network=${NETWORK_NAME}" --format="value(name)" | tr '\n' ' ')
if [ -n "$FIREWALLS" ]; then
  echo "Deleting firewall rules: $FIREWALLS"
  gcloud compute firewall-rules delete $FIREWALLS --project="${PROJECT_ID}" --quiet || true
fi

echo "--- Deleting dedicated VPC network: ${NETWORK_NAME} ---"
gcloud compute networks delete "${NETWORK_NAME}" \
  --project="${PROJECT_ID}" --quiet || true
