#!/usr/bin/env bash
# create-buckets: create the per-run checkpoint bucket (co-located with the
# cluster's region) and ensure the shared results bucket exists in the
# worker-pool LOCATION so BigQuery ingestion can read it.
set -e
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
if [ "${_BUCKET_TYPE}" = "zonal" ]; then
  # RAPID (zonal) objects lack the server-side rewrite rsync uses, so daisy-chain
  # (download+reupload)
  ulimit -n 65536
  DEST_PARENT="${SRC_OBJECT_PATH%/*}"
  [ "$DEST_PARENT" = "$SRC_OBJECT_PATH" ] && DEST_PARENT=""
  # `cp --recursive` on a directory/bucket source without a trailing wildcard
  # copies the source's own name into the destination (e.g. _DATASET_PATH
  # "gs://bucket" would land at "gs://DATASET_BUCKET/bucket/..."); "/*" makes
  # it copy the source's contents instead.
  CLOUDSDK_STORAGE_PROCESS_COUNT=4 CLOUDSDK_STORAGE_THREAD_COUNT=4 \
  CLOUDSDK_STORAGE_ATTEMPT_GRPC_DIRECT_PATH=False \
    gcloud storage cp --recursive --daisy-chain "${_DATASET_PATH%/}/*" "gs://${DATASET_BUCKET}${DEST_PARENT:+/$DEST_PARENT}"
else
  # Regional/HNS support server-side copy; rsync mirrors the source into the dest.
  gcloud storage rsync --recursive "${_DATASET_PATH}" "gs://${DATASET_BUCKET}/${SRC_OBJECT_PATH}"
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
