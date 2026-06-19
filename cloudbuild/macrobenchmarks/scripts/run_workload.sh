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
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > /workspace/start_time.txt
helm install "$RUN_ID" "$CHART" -f "$CHART/values_base.yaml" \
  --set gcsfs.datasetPath="${_DATASET_PATH}" \
  --set gcsfs.ckptWritePath="gs://$CHECKPOINT_BUCKET/checkpoints" \
  --set-string gcsfs.ckptLoadPath="${_CHECKPOINT_LOAD_PATH}" \
  --set workload.modelId="${_MODEL_ID}" \
  --set workload.hfToken="${_HF_TOKEN}" \
  --set workload.steps="${_STEPS}" \
  --set workload.ckptWriterInterval="${_CHECKPOINT_INTERVAL}" \
  --set workload.nodes="${_NODES}" \
  --set workload.requirements="${_GCSFS_SOURCE}" \
  --set serviceAccount=default
echo "Waiting for JobSet $RUN_ID to complete..."
WORKLOAD_COMPLETED=false
for i in $(seq 1 240); do
  COMPLETE=$(kubectl get jobset "$RUN_ID" -o jsonpath='{.status.conditions[?(@.type=="Completed")].status}' 2>/dev/null || echo "")
  FAILED=$(kubectl get jobset "$RUN_ID" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || echo "")
  if [ "$COMPLETE" = "True" ]; then WORKLOAD_COMPLETED=true; echo "JobSet completed."; break; fi
  if [ "$FAILED" = "True" ]; then
    echo "JobSet failed."
    kubectl describe jobset "$RUN_ID" || true
    kubectl get pods -l jobset.sigs.k8s.io/jobset-name="$RUN_ID" -o wide || true
    record_failure run-workload
    exit 1
  fi
  sleep 30
done
if [ "$WORKLOAD_COMPLETED" != "true" ]; then
  echo "Timed out waiting for JobSet $RUN_ID to complete."
  kubectl describe jobset "$RUN_ID" || true
  kubectl get pods -l jobset.sigs.k8s.io/jobset-name="$RUN_ID" -o wide || true
  record_failure run-workload
  exit 1
fi
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > /workspace/end_time.txt
