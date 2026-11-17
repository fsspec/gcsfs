#!/usr/bin/env bash
# create-buckets: create the per-run checkpoint bucket (co-located with the
# cluster's region) and ensure the shared results bucket exists in the
# worker-pool LOCATION so BigQuery ingestion can read it.
# -E: also run the ERR trap for failures inside lib.sh functions.
set -eE
source "$(dirname "$0")/lib.sh"
trap 'record_failure create-buckets' ERR
skip_if_failed
source "${BUILD_VARS_FILE}"
create_typed_bucket "$CHECKPOINT_BUCKET"

# Per-run dataset bucket (same config as CHECKPOINT_BUCKET), populated by an
# in-region copy, so its egress is attributable to one run for the dataset
# read-amplification metric.
create_typed_bucket "$DATASET_BUCKET"
SRC_OBJECT_PATH=$(echo "${_DATASET_PATH}" | sed -E 's#^gs://[^/]+/?##')
# *_rcu_z needs RAPID objects, which gcloud cannot write on an RCU bucket; the
# rcu-stage step copies the dataset with gcsfs instead.
if [ "${RCU_STATE}" != "z" ]; then
  copy_dataset_into_run_bucket
fi
echo "export RUN_DATASET_PATH=gs://${DATASET_BUCKET}/${SRC_OBJECT_PATH}" >> "${BUILD_VARS_FILE}"
if gcloud storage buckets describe gs://$RESULTS_BUCKET --project=${PROJECT_ID} >/dev/null 2>&1; then
  # Reuse only if co-located with this build's LOCATION. The ingestion pipeline
  # builds the BigQuery dataset (and external table) in LOCATION; a results
  # bucket in another region cannot be read by that dataset, so rows would
  # silently stop landing. Fail fast.
  EXISTING_LOC=$(gcloud storage buckets describe gs://$RESULTS_BUCKET --project=${PROJECT_ID} --format="value(location)" | tr 'A-Z' 'a-z')
  WANT_LOC=$(echo "${LOCATION}" | tr 'A-Z' 'a-z')
  if [ "$EXISTING_LOC" != "$WANT_LOC" ]; then
    record_failure create-buckets
    echo "ERROR: results bucket gs://$RESULTS_BUCKET is in '$EXISTING_LOC' but this build's LOCATION is '$WANT_LOC'; ingestion (dataset in ${LOCATION}) cannot read it. Run from a ${LOCATION} worker pool or use a different _INFRA_PREFIX."
    exit 1
  fi
else
  gcloud storage buckets create gs://$RESULTS_BUCKET --project=${PROJECT_ID} --location=${LOCATION} --enable-hierarchical-namespace --uniform-bucket-level-access
fi
