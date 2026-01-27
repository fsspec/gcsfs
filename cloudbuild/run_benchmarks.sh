#!/bin/bash
set -e

# Load build variables (buckets, etc.)
source /workspace/build_vars.env

# Sets up the VM by installing dependencies and cloning the repository.
# Arguments:
#   vm_name: The name of the VM instance.
setup_vm() {
  local vm_name=$1

  # Copy source code
  echo "[$vm_name] Copying source code..."
  gcloud compute scp --recurse . "${vm_name}:~/gcsfs" --project="${PROJECT_ID}" --zone="${ZONE}" --internal-ip --ssh-key-file=/workspace/.ssh/google_compute_engine

  local SETUP_SCRIPT="
    set -e

    export DEBIAN_FRONTEND=noninteractive
    sudo apt-get update > /dev/null
    sudo apt-get install -y python3-pip python3-venv fuse fuse3 libfuse2 git > /dev/null

    cd gcsfs

    python3 -m venv env
    source env/bin/activate

    pip install --upgrade pip > /dev/null
    pip install pytest pytest-timeout pytest-subtests pytest-asyncio fusepy google-cloud-storage > /dev/null
    pip install -e . > /dev/null
    pip install -r gcsfs/tests/perf/microbenchmarks/requirements.txt > /dev/null
  "

  gcloud compute ssh "$vm_name" --project="${PROJECT_ID}" --zone="${ZONE}" --ssh-key-file=/workspace/.ssh/google_compute_engine --command="$SETUP_SCRIPT"
}

# Runs the benchmark on the VM and uploads the results.
# Arguments:
#   vm_name: The name of the VM instance.
#   group: The benchmark group.
#   config: The benchmark config.
run_benchmark() {
  local vm_name=$1
  local group=$2
  local config=$3

  # Run Benchmark
  echo "[$vm_name] Running benchmark..."

  # Construct config arg
  local CONFIG_ARG=""
  if [ -n "$config" ]; then
    CONFIG_ARG="--config=$config"
  fi

  # Construct bucket args based on BUCKET_TYPES
  local BUCKET_ARGS=""
  if [[ " ${BUCKET_TYPES} " =~ " regional " ]]; then
    BUCKET_ARGS="${BUCKET_ARGS} --regional-bucket='${REGIONAL_BUCKET}'"
  fi
  if [[ " ${BUCKET_TYPES} " =~ " zonal " ]]; then
    BUCKET_ARGS="${BUCKET_ARGS} --zonal-bucket='${ZONAL_BUCKET}'"
  fi
  if [[ " ${BUCKET_TYPES} " =~ " hns " ]]; then
    BUCKET_ARGS="${BUCKET_ARGS} --hns-bucket='${HNS_BUCKET}'"
  fi

  local RUN_CMD="
    source gcsfs/env/bin/activate
    python gcsfs/gcsfs/tests/perf/microbenchmarks/run.py \
      --group=$group \
      $CONFIG_ARG \
      $BUCKET_ARGS \
      --log=true \
      --log-level=INFO

    # Upload results
    echo '--- Uploading Results ---'
    DATE_DIR=\$(date +%d%m%Y)
    RESULTS_DIR='gcsfs/gcsfs/tests/perf/microbenchmarks/__run__'
    if [ -d \"\$RESULTS_DIR\" ]; then
      echo \"Uploading from \$RESULTS_DIR to gs://${RESULTS_BUCKET}/\${DATE_DIR}/${RUN_ID}/\"
      cd \"\$RESULTS_DIR\" && gcloud storage cp --recursive . gs://${RESULTS_BUCKET}/\${DATE_DIR}/${RUN_ID}/ && rm -rf *
    else
      echo \"No results directory found at \$RESULTS_DIR\"
    fi
  "

  gcloud compute ssh "$vm_name" --project="${PROJECT_ID}" --zone="${ZONE}" --ssh-key-file=/workspace/.ssh/google_compute_engine --command="$RUN_CMD"
}

# Main function to orchestrate the sequential execution of benchmarks on a single VM.
main() {
  local vm_name="${VM_NAME}"

  echo "[$vm_name] Starting benchmarks..."

  if ! setup_vm "$vm_name"; then
    echo "WARNING: [$vm_name] Setup failed."
    exit 0
  fi

  # Load configs
  local CONFIG_ARRAY
  IFS=' ' read -r -a CONFIG_ARRAY <<< "${BENCHMARK_CONFIG}"

  local failures=0

  for entry in "${CONFIG_ARRAY[@]}"; do
    # Trim whitespace
    entry=$(echo "$entry" | xargs)
    if [ -z "$entry" ]; then continue; fi

    IFS=':' read -r group config <<< "$entry"

    echo "Launching job for $group:$config on $vm_name"

    if ! run_benchmark "$vm_name" "$group" "$config"; then
        echo "Benchmark $group:$config failed."
        failures=$((failures+1))
    fi

    echo "Sleeping for 30 seconds..."
    sleep 30
  done

  if [ "${failures:-0}" -ne 0 ]; then
    echo "WARNING: $failures benchmark jobs failed. Returning success to proceed with cleanup."
  else
    echo "All benchmarks completed successfully."
  fi
}

main
