#!/bin/bash
set -e
# Note: Variables like $PROJECT_ID, $REGION, $ZONE are passed via 'env' in cloudbuild.yaml

echo "--- Creating standard bucket ---"
gcloud storage buckets create "gs://gcsfs-test-standard-${SHORT_BUILD_ID}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" &

echo "--- Creating versioned bucket ---"
gcloud storage buckets create "gs://gcsfs-test-versioned-${SHORT_BUILD_ID}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" &

echo "--- Creating HNS bucket ---"
gcloud storage buckets create "gs://gcsfs-test-hns-${SHORT_BUILD_ID}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --enable-hierarchical-namespace \
    --uniform-bucket-level-access &

echo "--- Creating Zonal bucket ---"
gcloud storage buckets create "gs://gcsfs-test-zonal-${SHORT_BUILD_ID}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --placement="${ZONE}" \
    --default-storage-class=RAPID \
    --enable-hierarchical-namespace \
    --uniform-bucket-level-access &

echo "--- Creating standard bucket for Zonal test ---"
gcloud storage buckets create "gs://gcsfs-test-standard-for-zonal-${SHORT_BUILD_ID}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" &

# Wait for all background bucket creation jobs to finish
wait

echo "--- Enabling versioning on versioned bucket ---"
gcloud storage buckets update "gs://gcsfs-test-versioned-${SHORT_BUILD_ID}" \
    --versioning
