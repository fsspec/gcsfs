#!/bin/bash
set -e
# Creates the GCE VM used to run the integration tests.
#
# Split out of the former create_resources.sh so that VM provisioning + setup
# (the setup-vm step) can run in parallel with bucket creation (create_buckets.sh)
# instead of waiting for every bucket to be created first.
#
# Note: Variables like $PROJECT_ID, $ZONE are passed via 'env' in cloudbuild.yaml

MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-4}"

# The VM is created in the same zone as the zonal bucket to test rapid storage features.
# It's given the 'cloud-platform' scope to allow it to access GCS and other services.
echo "--- Creating GCE VM ---"
gcloud compute instances create "gcsfs-test-vm-${SHORT_BUILD_ID}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --image-family=debian-13 \
    --image-project=debian-cloud \
    --service-account="${ZONAL_VM_SERVICE_ACCOUNT}" \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --metadata=enable-oslogin=TRUE
