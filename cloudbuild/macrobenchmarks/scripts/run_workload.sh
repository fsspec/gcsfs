#!/usr/bin/env bash
# run-workload: helm install the workload chart, then poll the JobSet until it
# completes (recording start/end timestamps for the metric scrape) or fails /
# times out.
# -E: also run the ERR trap for failures inside lib.sh functions.
set -eE
source "$(dirname "$0")/lib.sh"
trap 'record_failure run-workload' ERR
skip_if_failed
source "${BUILD_VARS_FILE}"
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
gcloud container clusters get-credentials "$CLUSTER_NAME" --zone="${CLUSTER_LOCATION:-${_ZONE}}" --project="${PROJECT_ID}"
CHART="gcsfs/tests/perf/macrobenchmarks/workloads/${_WORKLOAD}/helm_chart"
# Restore precedence: an external checkpoint wins; otherwise, when seeding is on,
# restore the per-run seed produced by the seed-checkpoint step. Empty => fresh
# run (no restore), as before.
EFFECTIVE_LOAD_PATH="${_CHECKPOINT_LOAD_PATH}"
if [ -z "$EFFECTIVE_LOAD_PATH" ] && [ "${_SEED_CHECKPOINT}" = "true" ]; then
  EFFECTIVE_LOAD_PATH="${SEEDED_CKPT_PATH:-}"
fi
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "${WORKSPACE_DIR}/start_time.txt"
# shared_workload_helm_args sets gcsfs.rcuRapidWrite=true for *_rcu_z so
# checkpoint writes stay RAPID, while *_rcu_warm and *_rcu_cold write STANDARD
# over HTTP by default.
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
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "${WORKSPACE_DIR}/end_time.txt"
# z rows measure RAPID saves; skip if no checkpoint was written (possible on short Ray runs).
if [ "${RCU_STATE}" = "z" ] && gcloud storage objects list "gs://${CHECKPOINT_BUCKET}/checkpoints/**" --format="value(name)" 2>/dev/null | grep -q .; then
  verify_storage_class "gs://${CHECKPOINT_BUCKET}/checkpoints/" RAPID
fi
