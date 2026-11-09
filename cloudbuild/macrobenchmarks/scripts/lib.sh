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
WORKSPACE_DIR="${WORKSPACE_DIR:-/workspace}"
FAILED_FILE="${FAILED_FILE:-${WORKSPACE_DIR}/FAILED}"
BUILD_VARS_FILE="${BUILD_VARS_FILE:-${WORKSPACE_DIR}/build_vars.env}"

# Default the workload in one place so every step script agrees when it is
# unset/empty, then normalize the legacy name so existing callers or triggers
# passing _WORKLOAD=hf-pytorch-lightning-cpu continue to work without breaking.
_WORKLOAD="${_WORKLOAD:-hf-datasets-pytorch-lightning}"
if [[ "${_WORKLOAD}" == "hf-pytorch-lightning-cpu" ]]; then
  _WORKLOAD="hf-datasets-pytorch-lightning"
fi

# Object state the Rapid Cache Ultra (RCU) bucket types measure reads from:
# z (written RAPID), warm (STANDARD, ingested into the cache before the run) or
# cold (STANDARD, never read in the cache zone). Empty for the other types.
RCU_STATE=""
if [[ "${_BUCKET_TYPE:-}" == *_rcu_* ]]; then
  RCU_STATE="${_BUCKET_TYPE##*_rcu_}"
fi

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

# Create a per-run bucket per _BUCKET_TYPE (regional | zonal-RAPID | hns |
# regional or HNS with a Rapid Cache Ultra cache).
create_typed_bucket() {
  local bucket="$1"
  case "${_BUCKET_TYPE}" in
    regional)
      gcloud storage buckets create "gs://$bucket" --project="${PROJECT_ID}" --location="$REGION" ;;
    zonal)
      gcloud storage buckets create "gs://$bucket" --project="${PROJECT_ID}" --location="$REGION" --placement="${_ZONE}" --default-storage-class=RAPID --enable-hierarchical-namespace --uniform-bucket-level-access ;;
    hns)
      gcloud storage buckets create "gs://$bucket" --project="${PROJECT_ID}" --location="$REGION" --enable-hierarchical-namespace --uniform-bucket-level-access ;;
    standard_rcu_*)
      gcloud storage buckets create "gs://$bucket" --project="${PROJECT_ID}" --location="$REGION" --uniform-bucket-level-access
      create_rapid_cache "$bucket" ;;
    hns_rcu_*)
      gcloud storage buckets create "gs://$bucket" --project="${PROJECT_ID}" --location="$REGION" --enable-hierarchical-namespace --uniform-bucket-level-access
      create_rapid_cache "$bucket" ;;
    *)
      echo "ERROR: unknown _BUCKET_TYPE='${_BUCKET_TYPE}' (expected regional|zonal|hns|{standard,hns}_rcu_{z,warm,cold})" >&2
      return 1 ;;
  esac
}

# Add an admit-on-first-miss Rapid Cache Ultra cache in ${_ZONE}, the zone of
# the workload node pool, and wait until it is running: gcsfs only uses the RCU
# data path while it is. Creation is tried up to 3 times.
create_rapid_cache() {
  local bucket="$1" attempt=0 output state="" i
  until output=$(gcloud alpha storage buckets rapid-caches create "gs://$bucket" "${_ZONE}" --project="${PROJECT_ID}" \
      --cache-type=rapid-cache-ultra --admission-policy=admit-on-first-miss --no-async --quiet 2>&1); do
    echo "$output" >&2
    # An earlier attempt may have created the cache before it failed.
    if (( attempt > 0 )) && grep -qi "already exists" <<< "$output"; then
      break
    fi
    attempt=$((attempt + 1))
    if (( attempt >= 3 )); then
      echo "ERROR: could not create a rapid cache for gs://$bucket in ${_ZONE} after $attempt attempts." >&2
      return 1
    fi
    echo "Rapid cache create failed for gs://$bucket in ${_ZONE} (attempt $attempt/3), retrying..." >&2
    sleep $((RANDOM % 8 + 3))
  done
  echo "$output"
  for i in 1 2 3 4 5 6 7 8 9 10; do
    # A failed describe counts as not running yet.
    state=$(gcloud alpha storage buckets rapid-caches describe "$bucket/${_ZONE}" --project="${PROJECT_ID}" --format="value(state)" || true)
    [ "${state,,}" = running ] && return 0
    sleep 30
  done
  echo "ERROR: rapid cache $bucket/${_ZONE} is '$state', not running." >&2
  return 1
}

# Best-effort delete of a per-run bucket of any type: its objects first, then
# its Rapid Cache Ultra caches (disable runs async), then the bucket. The bucket
# delete fails while a disable is still running; cleanup-leaked-resources
# retries it in a later build.
delete_bucket_and_caches() {
  local bucket="$1" cache_id
  gcloud storage rm --recursive --project="${PROJECT_ID}" "gs://$bucket" || true
  while read -r cache_id _; do
    [ -n "$cache_id" ] || continue
    gcloud alpha storage buckets rapid-caches disable "$cache_id" --project="${PROJECT_ID}" --quiet || true
  done <<< "$(gcloud alpha storage buckets rapid-caches list "gs://$bucket" --project="${PROJECT_ID}" --format="value(id,state)" || true)"
  gcloud storage buckets delete "gs://$bucket" --project="${PROJECT_ID}" --quiet || true
}

# Copy _DATASET_PATH into the per-run dataset bucket with gcloud, which writes
# STANDARD objects (RAPID on a zonal bucket).
copy_dataset_into_run_bucket() {
  local src_object_path dest_parent
  src_object_path=$(echo "${_DATASET_PATH}" | sed -E 's#^gs://[^/]+/?##')
  if [ "${_BUCKET_TYPE}" = "zonal" ] || [ "${DATASET_SRC_IS_RAPID:-no}" = "yes" ]; then
    # RAPID (zonal) objects lack the server-side rewrite rsync uses, so daisy-chain
    # (download+reupload)
    ulimit -n 65536
    dest_parent="${src_object_path%/*}"
    [ "$dest_parent" = "$src_object_path" ] && dest_parent=""
    # `cp --recursive` on a directory/bucket source without a trailing wildcard
    # copies the source's own name into the destination (e.g. _DATASET_PATH
    # "gs://bucket" would land at "gs://DATASET_BUCKET/bucket/..."); "/*" makes
    # it copy the source's contents instead.
    CLOUDSDK_STORAGE_PROCESS_COUNT=16 CLOUDSDK_STORAGE_THREAD_COUNT=16 \
    CLOUDSDK_STORAGE_ATTEMPT_GRPC_DIRECT_PATH=False \
      gcloud storage cp --recursive --daisy-chain "${_DATASET_PATH%/}/*" "gs://${DATASET_BUCKET}${dest_parent:+/$dest_parent}"
  else
    # Regional/HNS support server-side copy; rsync mirrors the source into the dest.
    gcloud storage rsync --recursive "${_DATASET_PATH}" "gs://${DATASET_BUCKET}/${src_object_path}"
  fi
}

# Fail unless every object at PATH (an object or a prefix) has storage class
# CLASS. Lists the objects instead of describing each one: a GET from the cache
# zone could start ingesting the object.
verify_storage_class() {
  local path="$1" want="$2" listing
  listing=$(gcloud storage objects list "${path}**" --format="value(name,storage_class)")
  if [ -z "$listing" ]; then
    echo "ERROR: no objects at $path." >&2
    return 1
  fi
  if awk -F'\t' -v want="$want" '$2 != want' <<< "$listing" | grep .; then
    echo "ERROR: the objects above at $path are not $want." >&2
    return 1
  fi
  echo "All $(wc -l <<< "$listing") objects at $path are $want."
}

# Run ../rcu/prepare.py with the given arguments in a Job on the workload node
# pool, which is in ${_ZONE} (the RCU cache zone), and wait for it.
# Usage: run_rcu_job <name> <prepare.py args...>
run_rcu_job() {
  local job="${RUN_ID}-rcu-$1" status="" i
  shift
  gcloud container clusters get-credentials "$CLUSTER_NAME" --zone="${CLUSTER_LOCATION:-${_ZONE}}" --project="${PROJECT_ID}"
  kubectl create configmap "$job" --from-file=prepare.py="$(dirname "${BASH_SOURCE[0]}")/../rcu/prepare.py"
  kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: default
      nodeSelector:
        cloud.google.com/gke-nodepool: "${_MACHINE_TYPE}"
      containers:
        - name: prepare
          image: "${_IMAGE}"
          command:
            - bash
            - -c
            - 'pip3 install --no-cache-dir \$REQUIREMENTS && python3 /rcu/prepare.py "\$@"'
            - prepare
          args:
$(printf '            - "%s"\n' "$@")
          env:
            - name: REQUIREMENTS
              value: "${_REQUIREMENTS}"
          volumeMounts:
            - name: rcu
              mountPath: /rcu
      volumes:
        - name: rcu
          configMap:
            name: $job
EOF
  for ((i = 0; i < 360; i++)); do
    status=$(kubectl get job "$job" -o jsonpath='{.status.succeeded}/{.status.failed}' || true)
    [[ "$status" == 1/* || "$status" == */1 ]] && break
    sleep 10
  done
  kubectl logs "job/$job" || true
  if [[ "$status" != 1/* ]]; then
    echo "ERROR: Job $job did not succeed (succeeded/failed: '$status')." >&2
    return 1
  fi
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
    --set workload.tensorParallelSize="${_TENSOR_PARALLEL_SIZE}"
    --set workload.dataParallelSize="${_DATA_PARALLEL_SIZE}"
    --set workload.epochs="${_EPOCHS}"
    --set workload.shuffleBufferSize="${_SHUFFLE_BUFFER_SIZE}"
    --set workload.shuffleMaxBufferInputShards="${_SHUFFLE_MAX_BUFFER_INPUT_SHARDS}"
    --set workload.dataloaderPrefetchFactor="${_DATALOADER_PREFETCH_FACTOR}"
    --set workload.dataloaderWorkers="${_DATALOADER_WORKERS}"
    --set workload.torchDistributedTimeoutSeconds="${TORCH_DISTRIBUTED_TIMEOUT_SECONDS:-${_TORCH_DISTRIBUTED_TIMEOUT_SECONDS:-900}}"
    --set "nodeSelector.cloud\.google\.com/gke-nodepool=${_MACHINE_TYPE}"
    --set serviceAccount=default
  )
}

# Poll a JobSet until it reports Completed (return 0) or Failed/timeout (record
# the failure in the ledger, dump diagnostics, return 1). Shared by the
# seed-checkpoint and run-workload steps so the polling loop lives in one place.
# Usage: wait_for_jobset <jobset-name> <step-id>
wait_for_jobset() {
  local jobset="$1" step="$2" complete failed i
  local timeout_seconds="${WORKLOAD_TIMEOUT_SECONDS:-${_WORKLOAD_TIMEOUT_SECONDS:-14400}}"
  local max_iterations=$(( (timeout_seconds + 29) / 30 ))
  echo "Waiting for JobSet $jobset to complete (timeout: ${timeout_seconds}s, up to ${max_iterations} checks)..."
  for ((i = 1; i <= max_iterations; i++)); do
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
  echo "Timed out waiting for JobSet $jobset to complete after ${timeout_seconds}s."
  kubectl describe jobset "$jobset" || true
  kubectl get pods -l jobset.sigs.k8s.io/jobset-name="$jobset" -o wide || true
  record_failure "$step"
  return 1
}
