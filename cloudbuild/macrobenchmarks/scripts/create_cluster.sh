#!/usr/bin/env bash
# create-cluster: create the GKE cluster with a small system pool plus a
# dedicated ${_MACHINE_TYPE} pool (whose name the workload's nodeSelector keys
# off), then install the JobSet controller.
set -e
source "$(dirname "$0")/lib.sh"
trap 'record_failure create-cluster' ERR
skip_if_failed
source "${BUILD_VARS_FILE}"
# `gcloud container clusters create` has no --node-pool flag and always names
# its initial pool "default-pool", which would not match the workload's
# nodeSelector (cloud.google.com/gke-nodepool=c4-standard-192). Create a small
# system pool for the cluster, then a dedicated ${_MACHINE_TYPE} pool whose name
# the GKE node label (and the chart's nodeSelector) keys off of. --enable-gvnic
# puts the nodes on gVNIC, which C4 requires and which TIER_1 egress (the direct
# high-bandwidth path) is gated on.
gcloud container clusters create "$CLUSTER_NAME" \
  --project="${PROJECT_ID}" --zone="${_ZONE}" \
  --machine-type="e2-standard-4" --num-nodes="1" \
  --service-account="${_GKE_SERVICE_ACCOUNT}" \
  --scopes="https://www.googleapis.com/auth/cloud-platform" \
  --private-ipv6-google-access-type=outbound-only \
  --no-enable-autoupgrade --quiet
gcloud container node-pools create "${_MACHINE_TYPE}" \
  --cluster="$CLUSTER_NAME" --project="${PROJECT_ID}" --zone="${_ZONE}" \
  --machine-type="${_MACHINE_TYPE}" --num-nodes="${_NODES}" \
  --disk-size="200" \
  --disk-type="hyperdisk-balanced" \
  --enable-gvnic \
  --network-performance-configs="total-egress-bandwidth-tier=TIER_1" \
  --service-account="${_GKE_SERVICE_ACCOUNT}" \
  --scopes="https://www.googleapis.com/auth/cloud-platform" \
  --no-enable-autoupgrade --quiet
gcloud container clusters get-credentials "$CLUSTER_NAME" --zone="${_ZONE}" --project="${PROJECT_ID}"
kubectl apply --server-side -f "https://github.com/kubernetes-sigs/jobset/releases/download/${_JOBSET_VERSION}/manifests.yaml"
kubectl rollout status deployment/jobset-controller-manager -n jobset-system --timeout=300s
