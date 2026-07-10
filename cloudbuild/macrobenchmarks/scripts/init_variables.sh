#!/usr/bin/env bash
# init-variables: validate the substitutions and operator-supplied buckets,
# derive the region and run identifiers, and write them to
# /workspace/build_vars.env for later steps. Everything here fails fast, before
# any GKE cluster or bucket is provisioned (and billed).
source "$(dirname "$0")/lib.sh"

SHORT_BUILD_ID=${BUILD_ID:0:8}
SAFE_BRANCH=$(echo -n "${BRANCH_NAME:-unknown}" | tr -c 'a-zA-Z0-9_.-' '-')
if [ "${SAFE_BRANCH}" = "unknown" ]; then
  echo "ERROR: BRANCH_NAME unset."; exit 1
fi
if [ -z "${_INFRA_PREFIX}" ] || [ -z "${_ZONE}" ] || [ -z "${_GKE_SERVICE_ACCOUNT}" ] || [ -z "${_DATASET_PATH}" ] || [ -z "${_REQUIREMENTS}" ]; then
  echo "ERROR: required substitution missing (_INFRA_PREFIX,_ZONE,_GKE_SERVICE_ACCOUNT,_DATASET_PATH,_REQUIREMENTS)."; exit 1
fi
# Validate config before provisioning anything: an unsupported _BUCKET_TYPE
# silently skips checkpoint-bucket creation, and a non-positive _CHECKPOINT_INTERVAL
# divides-by-zero in scrape-metrics. Fail fast here so misconfig never reaches
# (and bills) GKE.
case "${_BUCKET_TYPE}" in
  regional|zonal|hns) ;;
  *) echo "ERROR: _BUCKET_TYPE must be regional|zonal|hns (got '${_BUCKET_TYPE}')."; exit 1 ;;
esac
# Reject an unknown parallel strategy before provisioning anything.
case "${_TRAINING_STRATEGY:-ddp}" in
  ddp|fsdp_sharded|fsdp_full) ;;
  *) echo "ERROR: _TRAINING_STRATEGY must be ddp, fsdp_sharded, or fsdp_full (got '${_TRAINING_STRATEGY}')."; exit 1 ;;
esac
# Reject an unknown seed-checkpoint toggle before provisioning anything.
case "${_SEED_CHECKPOINT:-true}" in
  true|false) ;;
  *) echo "ERROR: _SEED_CHECKPOINT must be true|false (got '${_SEED_CHECKPOINT}')."; exit 1 ;;
esac
# Reject an unknown TIER_1-networking toggle before provisioning anything.
case "${_ENABLE_TIER1_NETWORKING:-true}" in
  true|false) ;;
  *) echo "ERROR: _ENABLE_TIER1_NETWORKING must be true|false (got '${_ENABLE_TIER1_NETWORKING}')."; exit 1 ;;
esac
# An external checkpoint, if supplied, takes precedence and the seed step
# no-ops; note it so a run that set both does not look mis-wired.
if [ "${_SEED_CHECKPOINT:-true}" = "true" ] && [ -n "${_CHECKPOINT_LOAD_PATH}" ]; then
  echo "NOTE: _CHECKPOINT_LOAD_PATH is set; it overrides _SEED_CHECKPOINT (seed step will be skipped)."
fi
# Reject a non-numeric / negative simulated compute time before provisioning.
if ! echo "${_SIMULATED_STEP_COMPUTE_SECONDS:-1.0}" | grep -Eq '^[0-9]+(\.[0-9]+)?$'; then
  echo "ERROR: _SIMULATED_STEP_COMPUTE_SECONDS must be a non-negative number (got '${_SIMULATED_STEP_COMPUTE_SECONDS}')."; exit 1
fi
# Reject malformed zones before anything keys off them.
if ! echo "${_ZONE}" | grep -Eq '^[a-z]+-[a-z]+[0-9]-[a-z]$'; then
  echo "ERROR: _ZONE must be a GCP zone like us-central1-a (got '${_ZONE}')."; exit 1
fi
# Derive the region from the zone so buckets and cluster co-locate; do NOT use
# the worker-pool LOCATION, which may differ from _ZONE.
ZONE="${_ZONE}"
REGION="${ZONE%-*}"
for pair in "_NODES=${_NODES}" "_RANKS_PER_NODE=${_RANKS_PER_NODE}" "_STEPS=${_STEPS}" \
  "_CHECKPOINT_INTERVAL=${_CHECKPOINT_INTERVAL}" "_CKPT_TO_KEEP=${_CKPT_TO_KEEP}" \
  "_PER_DEVICE_BATCH=${_PER_DEVICE_BATCH}" "_GRAD_ACCUM=${_GRAD_ACCUM}" \
  "_DATALOADER_WORKERS=${_DATALOADER_WORKERS}"; do
  key=${pair%%=*}; val=${pair#*=}
  if ! echo "$val" | grep -Eq '^[1-9][0-9]*$'; then
    echo "ERROR: $key must be a positive integer (got '$val')."; exit 1
  fi
done
# Validate that operator-supplied buckets actually match _BUCKET_TYPE and the
# run's region/zone. Project-owned buckets are describable by the build SA
# (storage admin), so these are hard fail-fast (nothing is provisioned yet).
# Field-name-robust: one JSON describe, then grep the JSON for RAPID / the zone
# and read location explicitly.
validate_bucket() {
  BPATH="$1"; KIND="$2"
  BUCKET=$(echo "$BPATH" | sed -E 's#^gs://([^/]+).*#\1#')
  JSON=$(gcloud storage buckets describe "gs://$BUCKET" --project=${PROJECT_ID} --format=json 2>/dev/null) || {
    echo "ERROR: cannot describe $KIND bucket gs://$BUCKET (missing or no read access)."; exit 1; }
  LOC=$(echo "$JSON" | python3 -c "import sys,json;print((json.load(sys.stdin).get('location') or '').lower())")
  IS_RAPID=no; echo "$JSON" | grep -qiF 'RAPID' && IS_RAPID=yes
  if [ "$KIND" = dataset ]; then
    echo "export DATASET_SRC_IS_RAPID=${IS_RAPID}" >> "${BUILD_VARS_FILE}"
    echo "OK: dataset source gs://$BUCKET ($LOC, rapid=$IS_RAPID) will be copied into a fresh ${_BUCKET_TYPE} bucket in ${REGION}."
    return 0
  fi
  case "$LOC" in
    $REGION|$REGION-*) : ;;
    *) echo "ERROR: $KIND bucket gs://$BUCKET is in '$LOC', not region '$REGION'."; exit 1 ;;
  esac
  case "${_BUCKET_TYPE}" in
    regional)
      if [ "$IS_RAPID" = yes ]; then echo "ERROR: regional run but $KIND bucket gs://$BUCKET is RAPID/zonal."; exit 1; fi ;;
    zonal)
      if [ "$IS_RAPID" != yes ]; then echo "ERROR: zonal run requires a RAPID $KIND bucket (gs://$BUCKET)."; exit 1; fi
      if ! echo "$JSON" | grep -qiF "${_ZONE}"; then echo "ERROR: zonal $KIND bucket gs://$BUCKET is not placed in zone ${_ZONE}."; exit 1; fi ;;
    hns)
      if [ "$IS_RAPID" = yes ]; then echo "ERROR: hns run but $KIND bucket gs://$BUCKET is RAPID/zonal."; exit 1; fi ;;
  esac
  echo "OK: $KIND bucket gs://$BUCKET matches ${_BUCKET_TYPE} in $LOC."
}
validate_bucket "${_DATASET_PATH}" dataset
if [ -n "${_CHECKPOINT_LOAD_PATH}" ]; then validate_bucket "${_CHECKPOINT_LOAD_PATH}" checkpoint-load; fi


# Machine-type availability is best-effort (container.admin lacks
# compute.machineTypes.get); the node-pool create fails fast if wrong.
gcloud compute machine-types describe "${_MACHINE_TYPE}" --zone=${_ZONE} --project=${PROJECT_ID} >/dev/null 2>&1 || \
  echo "WARNING: could not verify ${_MACHINE_TYPE} in ${_ZONE} (may be a permissions gap, not a real problem)."
echo "export BRANCH_NAME=${SAFE_BRANCH}" >> "${BUILD_VARS_FILE}"
# Prepend 'buildid-' to ensure RUN_ID starts with a letter, satisfying GKE/K8s
# DNS-1035 naming restrictions (since Cloud Build UUIDs can start with numbers).
echo "export RUN_ID=buildid-${SHORT_BUILD_ID}" >> "${BUILD_VARS_FILE}"
echo "export CLUSTER_NAME=${_INFRA_PREFIX}-gke-${SHORT_BUILD_ID}" >> "${BUILD_VARS_FILE}"
echo "export NETWORK_NAME=${_INFRA_PREFIX}-net-${SHORT_BUILD_ID}" >> "${BUILD_VARS_FILE}"
echo "export SUBNET_NAME=${_INFRA_PREFIX}-subnet-${SHORT_BUILD_ID}" >> "${BUILD_VARS_FILE}"
echo "export CHECKPOINT_BUCKET=${_INFRA_PREFIX}-macrobench-checkpoint-${SHORT_BUILD_ID}" >> "${BUILD_VARS_FILE}"
echo "export DATASET_BUCKET=${_INFRA_PREFIX}-macrobench-dataset-${SHORT_BUILD_ID}" >> "${BUILD_VARS_FILE}"
echo "export RESULTS_BUCKET=${_INFRA_PREFIX}-macrobench-results" >> "${BUILD_VARS_FILE}"
echo "export REGION=${REGION}" >> "${BUILD_VARS_FILE}"
