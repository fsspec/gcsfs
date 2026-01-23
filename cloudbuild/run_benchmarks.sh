#!/bin/bash
set -e

# Load build variables (buckets, etc.)
source /workspace/build_vars.env

# Waits for SSH to become available on the VM.
# Arguments:
#   vm_name: The name of the VM instance.
#   log_file: Path to the log file for this VM.
wait_for_ssh() {
  local vm_name=$1
  local log_file=$2

  # Wait for SSH (MIG is stable, but SSH might take a moment)
  echo "[$vm_name] Waiting for SSH..." >> "$log_file"
  for i in {1..20}; do
    if gcloud compute ssh "$vm_name" --project="${PROJECT_ID}" --zone="${ZONE}" --ssh-key-file=/workspace/.ssh/google_compute_engine --command="echo ready" >> "$log_file" 2>&1; then
      return 0
    fi
    sleep 10
  done
  return 1
}

# Sets up the VM by installing dependencies and cloning the repository.
# Arguments:
#   vm_name: The name of the VM instance.
#   log_file: Path to the log file for this VM.
setup_vm() {
  local vm_name=$1
  local log_file=$2

  # Copy source code
  echo "[$vm_name] Copying source code..." >> "$log_file"
  gcloud compute scp --recurse . "${vm_name}:~/gcsfs" --project="${PROJECT_ID}" --zone="${ZONE}" --internal-ip --ssh-key-file=/workspace/.ssh/google_compute_engine >> "$log_file" 2>&1

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

  gcloud compute ssh "$vm_name" --project="${PROJECT_ID}" --zone="${ZONE}" --ssh-key-file=/workspace/.ssh/google_compute_engine --command="$SETUP_SCRIPT" >> "$log_file" 2>&1
}

# Runs the benchmark on the VM and uploads the results.
# Arguments:
#   vm_name: The name of the VM instance.
#   group: The benchmark group.
#   config: The benchmark config.
#   log_file: Path to the log file for this VM.
run_benchmark() {
  local vm_name=$1
  local group=$2
  local config=$3
  local log_file=$4

  # Run Benchmark
  echo "[$vm_name] Running benchmark..." >> "$log_file"

  # Construct config arg
  local CONFIG_ARG=""
  if [ -n "$config" ]; then
    CONFIG_ARG="--config=$config"
  fi

  local RUN_CMD="
    source gcsfs/env/bin/activate
    python gcsfs/gcsfs/tests/perf/microbenchmarks/run.py \
      --group=$group \
      $CONFIG_ARG \
      --regional-bucket='${REGIONAL_BUCKET}' \
      --zonal-bucket='${ZONAL_BUCKET}' \
      --hns-bucket='${HNS_BUCKET}' \
      --log=true \
      --log-level=INFO

    # Upload results
    echo '--- Uploading Results ---'
    DATE_DIR=\$(date +%d%m%Y)
    RESULTS_DIR='gcsfs/gcsfs/tests/perf/microbenchmarks/__run__'
    if [ -d \"\$RESULTS_DIR\" ]; then
      echo \"Uploading from \$RESULTS_DIR to gs://${RESULTS_BUCKET}/\${DATE_DIR}/${RUN_ID}/\"
      cd \"\$RESULTS_DIR\" && gcloud storage cp --recursive . gs://${RESULTS_BUCKET}/\${DATE_DIR}/${RUN_ID}/
    else
      echo \"No results directory found at \$RESULTS_DIR\"
    fi
  "

  gcloud compute ssh "$vm_name" --project="${PROJECT_ID}" --zone="${ZONE}" --ssh-key-file=/workspace/.ssh/google_compute_engine --command="$RUN_CMD" >> "$log_file" 2>&1
}

# Orchestrates the execution of a single benchmark job: wait for SSH, setup VM, and run benchmark.
# Arguments:
#   group: The benchmark group.
#   config: The benchmark config.
#   vm_name: The name of the VM instance.
run_job() {
  local group=$1
  local config=$2
  local vm_name=$3
  local log_file="/workspace/${vm_name}.log"

  echo "[$vm_name] Starting job for Group: $group, Config: $config" > "$log_file"

  if ! wait_for_ssh "$vm_name" "$log_file"; then
    echo "[$vm_name] SSH wait failed." >> "$log_file"
    return 1
  fi

  if ! setup_vm "$vm_name" "$log_file"; then
    echo "[$vm_name] Setup failed." >> "$log_file"
    return 1
  fi

  if ! run_benchmark "$vm_name" "$group" "$config" "$log_file"; then
    echo "[$vm_name] Benchmark failed." >> "$log_file"
    return 1
  fi

  echo "[$vm_name] Benchmark finished successfully." >> "$log_file"
  return 0
}

# Main function to orchestrate the parallel execution of benchmarks across VMs.
main() {
  # Load instances
  local INSTANCES=($(cat /workspace/instances.txt))

  # Load configs
  local CONFIG_ARRAY
  IFS=' ' read -r -a CONFIG_ARRAY <<< "${BENCHMARK_FANOUT_CONFIG}"

  local NUM_VMS=${#CONFIG_ARRAY[@]}
  if [ "${#INSTANCES[@]}" -ne "$NUM_VMS" ]; then
      echo "Error: Number of instances (${#INSTANCES[@]}) does not match number of configs ($NUM_VMS)."
      exit 1
  fi

  # Main Loop
  local pids=""

  for i in "${!CONFIG_ARRAY[@]}"; do
    local entry="${CONFIG_ARRAY[$i]}"
    local vm_name="${INSTANCES[$i]}"

    IFS=':' read -r group config <<< "$entry"

    echo "Launching job for $group:$config on $vm_name"

    # Run in background
    run_job "$group" "$config" "$vm_name" &
    pids="$pids $!"
  done

  # Wait for all jobs
  local failures=0
  for pid in $pids; do
    wait $pid || failures=$((failures+1))
  done

  # Print logs
  echo "--- Benchmark Logs ---"
  cat /workspace/*.log || true

  if [ $failures -ne 0 ]; then
    echo "$failures benchmark jobs failed."
    exit 1
  fi
  echo "All benchmarks completed successfully."
}

main
