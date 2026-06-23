#!/usr/bin/env bash
# create-buckets: create the per-run checkpoint bucket (co-located with the
# cluster's region) and ensure the shared results bucket exists in the
# worker-pool LOCATION so BigQuery ingestion can read it.
set -e
source "$(dirname "$0")/lib.sh"
trap 'record_failure create-buckets' ERR
skip_if_failed
source "${BUILD_VARS_FILE}"
if [[ "${_BUCKET_TYPE}" == "regional" ]]; then
  gcloud storage buckets create gs://$CHECKPOINT_BUCKET --project=${PROJECT_ID} --location=$REGION
elif [[ "${_BUCKET_TYPE}" == "zonal" ]]; then
  gcloud storage buckets create gs://$CHECKPOINT_BUCKET --project=${PROJECT_ID} --location=$REGION --placement=${_ZONE} --default-storage-class=RAPID --enable-hierarchical-namespace --uniform-bucket-level-access
elif [[ "${_BUCKET_TYPE}" == "hns" ]]; then
  gcloud storage buckets create gs://$CHECKPOINT_BUCKET --project=${PROJECT_ID} --location=$REGION --enable-hierarchical-namespace --uniform-bucket-level-access
fi
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
