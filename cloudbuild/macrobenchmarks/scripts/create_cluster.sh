#!/usr/bin/env bash
# create-cluster: create the GKE cluster with a small system pool plus a
# dedicated ${_MACHINE_TYPE} pool (whose name the workload's nodeSelector keys
# off), then install the JobSet controller.
set -e
source "$(dirname "$0")/lib.sh"
trap 'record_failure create-cluster' ERR
skip_if_failed
source "${BUILD_VARS_FILE}"

echo "--- Creating dedicated VPC network: ${NETWORK_NAME} ---"
gcloud compute networks create "${NETWORK_NAME}" \
  --project="${PROJECT_ID}" \
  --subnet-mode=custom --quiet

echo "--- Creating dedicated subnetwork: ${SUBNET_NAME} ---"
gcloud compute networks subnets create "${SUBNET_NAME}" \
  --project="${PROJECT_ID}" \
  --network="${NETWORK_NAME}" \
  --region="${REGION}" \
  --range="10.0.0.0/20" \
  --enable-private-ip-google-access --quiet

# `gcloud container clusters create` has no --node-pool flag and always names
# its initial pool "default-pool", which would not match the workload's
# nodeSelector (cloud.google.com/gke-nodepool=c4-standard-192). Create a small
# system pool for the cluster, then a dedicated ${_MACHINE_TYPE} pool whose name
# the GKE node label (and the chart's nodeSelector) keys off of. --enable-gvnic
# puts the nodes on gVNIC, which C4 requires and which TIER_1 egress (the direct
# high-bandwidth path) is gated on.
#
# The GKE control plane of a zonal cluster runs in the cluster's zone and can hit
# GCE_STOCKOUT there independently of the node pools. If that happens, retry the
# control plane in the region's other zones; the ${_MACHINE_TYPE} pool stays in
# ${_ZONE}.
ZONES="${_ZONE} $(gcloud compute zones list --project="${PROJECT_ID}" --filter="region:${REGION}" --format="value(name)" | grep -vx "${_ZONE}" || true)"
CLUSTER_LOCATION=""
for zone in $ZONES; do
  if gcloud container clusters create "$CLUSTER_NAME" \
    --project="${PROJECT_ID}" --zone="${zone}" \
    --machine-type="e2-standard-4" --num-nodes="1" \
    --service-account="${_GKE_SERVICE_ACCOUNT}" \
    --scopes="https://www.googleapis.com/auth/cloud-platform" \
    --private-ipv6-google-access-type=outbound-only \
    --network="${NETWORK_NAME}" --subnetwork="${SUBNET_NAME}" \
    --enable-autoupgrade --quiet; then
    CLUSTER_LOCATION="$zone"
    break
  fi
  gcloud container clusters delete "$CLUSTER_NAME" --zone="${zone}" --project="${PROJECT_ID}" --quiet || true
done
if [ -z "$CLUSTER_LOCATION" ]; then
  record_failure create-cluster
  exit 1
fi
echo "export CLUSTER_LOCATION=${CLUSTER_LOCATION}" >> "${BUILD_VARS_FILE}"
NODE_POOL_ARGS=(
  --cluster="$CLUSTER_NAME"
  --project="${PROJECT_ID}"
  --zone="${CLUSTER_LOCATION}"
  --node-locations="${_ZONE}"
  --machine-type="${_MACHINE_TYPE}"
  --num-nodes="${_NODES}"
  --disk-size="200"
  --disk-type="hyperdisk-balanced"
  --enable-gvnic
  --service-account="${_GKE_SERVICE_ACCOUNT}"
  --scopes="https://www.googleapis.com/auth/cloud-platform"
  --enable-autoupgrade
  --quiet
)
if [ "${_ENABLE_TIER1_NETWORKING:-true}" = "true" ]; then
  NODE_POOL_ARGS+=(--network-performance-configs="total-egress-bandwidth-tier=TIER_1")
fi
gcloud container node-pools create "${_MACHINE_TYPE}" "${NODE_POOL_ARGS[@]}"
gcloud container clusters get-credentials "$CLUSTER_NAME" --zone="${CLUSTER_LOCATION:-${_ZONE}}" --project="${PROJECT_ID}"
kubectl apply --server-side -f "https://github.com/kubernetes-sigs/jobset/releases/download/${_JOBSET_VERSION}/manifests.yaml"
kubectl rollout status deployment/jobset-controller-manager -n jobset-system --timeout=300s

echo "Waiting for JobSet mutating webhook to accept connections..."
for i in $(seq 1 30); do
  if WEBHOOK_PROBE_OUTPUT=$(kubectl create --dry-run=server -f - 2>&1 <<EOF
apiVersion: jobset.x-k8s.io/v1alpha2
kind: JobSet
metadata:
  name: webhook-probe
  namespace: default
spec:
  replicatedJobs: []
EOF
  ); then
    echo "JobSet webhook is ready and accepting requests."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: Timed out waiting for JobSet webhook readiness" >&2
    printf 'Last probe error:\n%s\n' "$WEBHOOK_PROBE_OUTPUT" >&2
    record_failure create-cluster
    exit 1
  fi
  sleep 2
done
