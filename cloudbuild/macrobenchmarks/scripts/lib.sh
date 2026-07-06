#!/usr/bin/env bash
# Shared helpers for the macrobenchmarks run-pipeline step scripts.
#
# Each step of ../macrobenchmarks-cloudbuild.yaml invokes one script in this
# directory. Cloud Build substitutions reach the scripts as environment
# variables (wired through each step's `env:` block) because Cloud Build does
# not substitute ${...} inside a file read from disk -- so the scripts read
# e.g. ${_ZONE} and ${PROJECT_ID} as ordinary env vars.

# Cross-step state files live on the /workspace volume that Cloud Build shares
# between steps. The defaults are overridable so the scripts can be exercised
# outside Cloud Build (e.g. unit tests) without writing to /workspace.
FAILED_FILE="${FAILED_FILE:-/workspace/FAILED}"
BUILD_VARS_FILE="${BUILD_VARS_FILE:-/workspace/build_vars.env}"

# Record a step id in the failure ledger. The allowFailure provisioning steps
# append here on error so the final check-failure step can fail the build with
# the list of culprits.
record_failure() {
  echo "$1" >> "${FAILED_FILE}"
}

# Short-circuit the rest of a step when an earlier step already failed. Cloud
# Build keeps running later steps after an allowFailure step fails; this turns
# them into no-ops instead of compounding the failure.
skip_if_failed() {
  if [[ -f "${FAILED_FILE}" ]]; then
    echo "Skipping: previous step failed"
    exit 0
  fi
}

# Create a per-run bucket per _BUCKET_TYPE (regional | zonal-RAPID | hns).
create_typed_bucket() {
  local bucket="$1"
  case "${_BUCKET_TYPE}" in
    regional)
      gcloud storage buckets create "gs://$bucket" --project="${PROJECT_ID}" --location="$REGION" ;;
    zonal)
      gcloud storage buckets create "gs://$bucket" --project="${PROJECT_ID}" --location="$REGION" --placement="${_ZONE}" --default-storage-class=RAPID --enable-hierarchical-namespace --uniform-bucket-level-access ;;
    hns)
      gcloud storage buckets create "gs://$bucket" --project="${PROJECT_ID}" --location="$REGION" --enable-hierarchical-namespace --uniform-bucket-level-access ;;
    *)
      echo "ERROR: unknown _BUCKET_TYPE='${_BUCKET_TYPE}' (expected regional|zonal|hns)" >&2
      return 1 ;;
  esac
}

shared_workload_helm_args() {
  SHARED_HELM_ARGS=(
    --set gcsfs.datasetPath="${RUN_DATASET_PATH:-${_DATASET_PATH}}"
    --set workload.modelId="${_MODEL_ID}"
    --set-string workload.image="${_IMAGE}"
    --set workload.hfToken="${_HF_TOKEN}"
    --set workload.nodes="${_NODES}"
    --set workload.ranksPerNode="${_RANKS_PER_NODE}"
    --set workload.requirements="${_REQUIREMENTS}"
    --set workload.trainingStrategy="${_TRAINING_STRATEGY}"
    --set "nodeSelector.cloud\.google\.com/gke-nodepool=${_MACHINE_TYPE}"
    --set serviceAccount=default
  )
}

# Poll a JobSet until it reports Completed (return 0) or Failed/timeout (record
# the failure in the ledger, dump diagnostics, return 1). Shared by the
# seed-checkpoint and run-workload steps so the 240x30s poll lives in one place.
# Usage: wait_for_jobset <jobset-name> <step-id>
wait_for_jobset() {
  local jobset="$1" step="$2" complete failed
  echo "Waiting for JobSet $jobset to complete..."
  for _ in $(seq 1 240); do
    complete=$(kubectl get jobset "$jobset" -o jsonpath='{.status.conditions[?(@.type=="Completed")].status}' 2>/dev/null || echo "")
    failed=$(kubectl get jobset "$jobset" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || echo "")
    if [ "$complete" = "True" ]; then echo "JobSet $jobset completed."; return 0; fi
    if [ "$failed" = "True" ]; then
      echo "JobSet $jobset failed."
      kubectl describe jobset "$jobset" || true
      kubectl get pods -l jobset.sigs.k8s.io/jobset-name="$jobset" -o wide || true
      record_failure "$step"
      return 1
    fi
    sleep 30
  done
  echo "Timed out waiting for JobSet $jobset to complete."
  kubectl describe jobset "$jobset" || true
  kubectl get pods -l jobset.sigs.k8s.io/jobset-name="$jobset" -o wide || true
  record_failure "$step"
  return 1
}
