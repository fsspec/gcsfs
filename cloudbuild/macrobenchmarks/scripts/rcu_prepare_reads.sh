#!/usr/bin/env bash
# rcu-prepare-reads: for the Rapid Cache Ultra (RCU) bucket types, put the
# seed checkpoint (restore runs) and the dataset in the state the type measures
# after the seed release, before run-workload starts the clock. No-op for the
# other bucket types.
#   z:    check the seed is RAPID.
#   warm: check the seed is STANDARD, read the first MiB of every dataset and
#         seed object from the cache zone (in a Job), then wait for the cache to
#         ingest them.
#   cold: check the seed is STANDARD. The seed release read the dataset from
#         the cache zone, which may have ingested it, so replace the dataset
#         and the seed with copies nothing has read.
# -E: also run the ERR trap for failures inside lib.sh functions.
set -eE
source "$(dirname "$0")/lib.sh"
trap 'record_failure rcu-prepare-reads' ERR
skip_if_failed
if [ -z "${RCU_STATE}" ]; then
  echo "Not an RCU bucket type; skipping rcu-prepare-reads."
  exit 0
fi
source "${BUILD_VARS_FILE}"
# Set by seed-checkpoint in restore runs only.
SEED="${SEEDED_CKPT_PATH:-}"

case "${RCU_STATE}" in
  z)
    [ -z "$SEED" ] || verify_storage_class "$SEED" RAPID
    ;;
  warm)
    [ -z "$SEED" ] || verify_storage_class "$SEED" STANDARD
    run_rcu_job warm warm "${RUN_DATASET_PATH}" ${SEED:+"$SEED"}
    SETTLE="${_RCU_WARMUP_SETTLE_SECONDS}"
    if [ -z "$SETTLE" ]; then
      # Ingest has no completion signal: 90 s + 1.5 s per GiB read, as in the
      # subsystem benchmarks.
      BYTES=$(gcloud storage du -s "${RUN_DATASET_PATH}" ${SEED:+"$SEED"} | awk '{ s += $1 } END { print s + 0 }')
      SETTLE=$(( 90 + BYTES * 3 / 2 / 1073741824 ))
    fi
    echo "Waiting ${SETTLE}s for the cache to ingest the warmed objects."
    sleep "$SETTLE"
    ;;
  cold)
    if [ -n "$SEED" ]; then
      verify_storage_class "$SEED" STANDARD
      # Delete first: rsync skips objects that are already there.
      gcloud storage rm "${RUN_DATASET_PATH%/}/**"
      copy_dataset_into_run_bucket
      gcloud storage cp --recursive "$SEED" "gs://${CHECKPOINT_BUCKET}/seed-cold/"
      echo "export SEEDED_CKPT_PATH=gs://${CHECKPOINT_BUCKET}/seed-cold/${SEED##*/}" >> "${BUILD_VARS_FILE}"
    fi
    ;;
esac
