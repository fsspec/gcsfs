#!/usr/bin/env bash
# run-workload: helm install the workload chart, then poll the JobSet until it
# completes (recording start/end timestamps for the metric scrape) or fails /
# times out.
set -e
source "$(dirname "$0")/lib.sh"
trap 'record_failure run-workload' ERR
skip_if_failed
source "${BUILD_VARS_FILE}"
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
gcloud container clusters get-credentials "$CLUSTER_NAME" --zone="${_ZONE}" --project="${PROJECT_ID}"
CHART="gcsfs/tests/perf/macrobenchmarks/workloads/${_WORKLOAD}/helm_chart"
# Restore precedence: an external checkpoint wins; otherwise, when seeding is on,
# restore the per-run seed produced by the seed-checkpoint step. Empty => fresh
# run (no restore), as before.
EFFECTIVE_LOAD_PATH="${_CHECKPOINT_LOAD_PATH}"
if [ -z "$EFFECTIVE_LOAD_PATH" ] && [ "${_SEED_CHECKPOINT}" = "true" ]; then
  EFFECTIVE_LOAD_PATH="${SEEDED_CKPT_PATH:-}"
fi
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > /workspace/start_time.txt
shared_workload_helm_args
helm install "$RUN_ID" "$CHART" -f "$CHART/values_base.yaml" \
  "${SHARED_HELM_ARGS[@]}" \
  --set gcsfs.ckptWritePath="gs://$CHECKPOINT_BUCKET/checkpoints" \
  --set-string gcsfs.ckptLoadPath="${EFFECTIVE_LOAD_PATH}" \
  --set workload.steps="${_STEPS}" \
  --set workload.ckptWriterInterval="${_CHECKPOINT_INTERVAL}" \
  --set workload.ckptToKeep="${_CKPT_TO_KEEP}" \
  --set workload.perDeviceBatch="${_PER_DEVICE_BATCH}" \
  --set workload.gradAccum="${_GRAD_ACCUM}" \
  --set workload.simulatedStepComputeSeconds="${_SIMULATED_STEP_COMPUTE_SECONDS}"
if ! wait_for_jobset "$RUN_ID" run-workload; then
  exit 1
fi
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > /workspace/end_time.txt
