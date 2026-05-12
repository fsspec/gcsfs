#!/bin/bash
set -e
# Note: Variables like $PROJECT_ID, $REGION, $ZONE are passed via 'env' in cloudbuild.yaml

echo "--- Creating standard bucket ---"
gcloud storage buckets create "gs://gcsfs-test-standard-${SHORT_BUILD_ID}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" &

echo "--- Creating standard requester pays bucket ---"
gcloud storage buckets create "gs://gcsfs-test-standard-req-pay-${SHORT_BUILD_ID}" \
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

echo "--- Creating HNS requester pays bucket ---"
gcloud storage buckets create "gs://gcsfs-test-hns-req-pay-${SHORT_BUILD_ID}" \
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

# Use a separate bucket for running core tests to avoid exceeding object rate limit
echo "--- Creating Zonal bucket for running core tests ---"
gcloud storage buckets create "gs://gcsfs-test-zonal-core-${SHORT_BUILD_ID}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --placement="${ZONE}" \
    --default-storage-class=RAPID \
    --enable-hierarchical-namespace \
    --uniform-bucket-level-access &

# The VM is created in the same zone as the zonal bucket to test rapid storage features.
# It's given the 'cloud-platform' scope to allow it to access GCS and other services.
echo "--- Creating GCE VM ---"
gcloud compute instances create "gcsfs-test-vm-${SHORT_BUILD_ID}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type=n2-standard-4 \
    --image-family=debian-13 \
    --image-project=debian-cloud \
    --service-account="${ZONAL_VM_SERVICE_ACCOUNT}" \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --metadata=enable-oslogin=TRUE &

# Wait for all background bucket and VM creation jobs to finish
wait

echo "--- Enabling versioning on versioned bucket ---"
gcloud storage buckets update "gs://gcsfs-test-versioned-${SHORT_BUILD_ID}" \
    --versioning

echo "--- Enabling requester pays on HNS bucket ---"
gcloud storage buckets update "gs://gcsfs-test-hns-req-pay-${SHORT_BUILD_ID}" \
    --requester-pays

echo "--- Enabling requester pays on standard bucket ---"
gcloud storage buckets update "gs://gcsfs-test-standard-req-pay-${SHORT_BUILD_ID}" \
    --requester-pays
