#!/usr/bin/env bash
# cleanup-leaked-resources: reap clusters and checkpoint buckets left behind by
# builds that have already ended, without touching resources that might belong
# to a build still in flight.

CURRENT_TIME=$(date +%s)
# Must be >= the build `timeout` (21600s). A concurrently-running build's
# cluster/bucket is always younger than that build's own elapsed time (and thus
# younger than the timeout), so it is never reaped mid-run; only resources from
# a build that has already ended age past this.
THRESHOLD=21600
CLUSTERS=$(gcloud container clusters list --project="${PROJECT_ID}" --filter="name~'${_INFRA_PREFIX}-gke-'" --format="value(name,location,createTime)")
while read -r name location create_time; do
  if [ -z "$name" ]; then continue; fi
  # Skip rather than mis-compute if the create time is empty/unparseable (e.g. a
  # gcloud field-name change): a bad date would otherwise make AGE garbage.
  CREATED=$(date -d "$create_time" +%s 2>/dev/null) || continue
  AGE=$((CURRENT_TIME - CREATED))
  if [ "$AGE" -gt "$THRESHOLD" ]; then
    echo "Deleting leaked cluster $name"
    gcloud container clusters delete "$name" --location="$location" --project="${PROJECT_ID}" --quiet || true
  fi
done <<< "$CLUSTERS"

# Clean up leaked checkpoint buckets. Scope the listing to this prefix's
# ephemeral checkpoint buckets so the shared results bucket is never a
# candidate; the in-loop anchored match is kept as a second guard.
BUCKETS=$(gcloud storage buckets list --project="${PROJECT_ID}" --filter="name~'${_INFRA_PREFIX}-macrobench-checkpoint-'" --format="value(name,creation_time)")
while read -r name creation_time; do
  if [ -z "$name" ]; then continue; fi
  if [[ "$name" =~ ^${_INFRA_PREFIX}-macrobench-checkpoint- ]]; then
    CREATED=$(date -d "$creation_time" +%s 2>/dev/null) || continue
    AGE=$((CURRENT_TIME - CREATED))
    if [ "$AGE" -gt "$THRESHOLD" ]; then
      echo "Deleting leaked bucket gs://$name"
      gcloud storage rm --recursive "gs://$name" || true
    fi
  fi
done <<< "$BUCKETS"
