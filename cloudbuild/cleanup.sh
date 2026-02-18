#!/bin/bash
set -e

echo "--- Deleting VM ---"
gcloud compute instances delete "gcsfs-test-vm-${SHORT_BUILD_ID}" --zone="${ZONE}" --quiet || true

echo "--- Deleting buckets ---"
gcloud storage rm --recursive "gs://gcsfs-test-standard-${SHORT_BUILD_ID}" || true &
gcloud storage rm --recursive "gs://gcsfs-test-versioned-${SHORT_BUILD_ID}" || true &
gcloud storage rm --recursive "gs://gcsfs-test-hns-${SHORT_BUILD_ID}" || true &
gcloud storage rm --recursive "gs://gcsfs-test-zonal-${SHORT_BUILD_ID}" || true &
gcloud storage rm --recursive "gs://gcsfs-test-standard-for-zonal-${SHORT_BUILD_ID}" || true &
gcloud storage rm --recursive "gs://gcsfs-test-zonal-core-${SHORT_BUILD_ID}" || true &
wait
