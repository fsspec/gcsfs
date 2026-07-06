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
# --daisy-chain (download+reupload) because RAPID (zonal) objects don't
# support the rewrite/copy API a plain rsync/cp needs.
#
# `cp --recursive` appends the source's leaf dir to the destination (rsync
# mirrors instead), so the destination must be the leaf's PARENT or the path
# double-nests (.../parquet/parquet/...) and the workload glob matches nothing.
DEST_PARENT="${SRC_OBJECT_PATH%/*}"                          # dirname
[ "$DEST_PARENT" = "$SRC_OBJECT_PATH" ] && DEST_PARENT=""    # leaf has no slash -> bucket root
gcloud storage cp --recursive --daisy-chain "${_DATASET_PATH}" "gs://${DATASET_BUCKET}${DEST_PARENT:+/$DEST_PARENT}"
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
