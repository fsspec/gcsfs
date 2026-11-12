#!/usr/bin/env bash
# rcu-stage: for the Rapid Cache Ultra (RCU) bucket types, put the run's
# dataset in the storage state the type measures before the seed release reads
# it, and check it: *_rcu_z copies it in as RAPID objects with gcsfs (in a Job
# in the cache zone); warm and cold keep the STANDARD copy create-buckets made,
# and a Job checks that the gcsfs under test sees both run buckets as RCU
# buckets. No-op for the other bucket types.
# -E: also run the ERR trap for failures inside lib.sh functions.
set -eE
source "$(dirname "$0")/lib.sh"
trap 'record_failure rcu-stage' ERR
skip_if_failed
if [ -z "${RCU_STATE}" ]; then
  echo "Not an RCU bucket type; skipping rcu-stage."
  exit 0
fi
source "${BUILD_VARS_FILE}"

if [ "${RCU_STATE}" = "z" ]; then
  run_rcu_job stage stage-rapid "${_DATASET_PATH}" "${RUN_DATASET_PATH}"
  verify_storage_class "${RUN_DATASET_PATH}" RAPID
else
  # z proves the gcsfs under test writes RAPID. Warm and cold only read, so
  # check that gcsfs sees both buckets as RCU, or the run would read over HTTP.
  run_rcu_job check check "${RUN_DATASET_PATH}" "gs://${CHECKPOINT_BUCKET}"
  verify_storage_class "${RUN_DATASET_PATH}" STANDARD
fi
