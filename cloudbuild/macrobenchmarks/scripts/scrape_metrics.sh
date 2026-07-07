#!/usr/bin/env bash
# scrape-metrics: scrape Cloud Logging into raw CSVs and aggregate them into one
# summary row, retrying for Cloud Logging ingestion lag, then upload the summary
# to the results bucket. calculate exits non-zero until the required metrics are
# complete, gating the upload.
set -e
source "$(dirname "$0")/lib.sh"
trap 'record_failure scrape-metrics' ERR
skip_if_failed
source "${BUILD_VARS_FILE}"
pip3 install --break-system-packages --quiet -r cloudbuild/macrobenchmarks/metrics/requirements.txt
START_TIME=$(cat /workspace/start_time.txt)
END_TIME=$(cat /workspace/end_time.txt)
RAW_DIR=/workspace/raw_metrics
cd cloudbuild/macrobenchmarks
DATE_DIR=$(date +%Y%m%d)
TS_DIR=$(date +%Y%m%d-%H%M%S)
SUMMARY=/workspace/${TS_DIR}.csv
MIN_WRITE_DATAPOINTS=$((${_STEPS} / ${_CHECKPOINT_INTERVAL}))
if [ "$MIN_WRITE_DATAPOINTS" -lt 1 ]; then
  MIN_WRITE_DATAPOINTS=1
fi
MIN_RESTORE_DATAPOINTS=0
RESUME_ARGS=()
# Restore is expected when an external checkpoint is supplied OR the per-run seed
# was generated (_SEED_CHECKPOINT=true). Both make the measured run a resume, so
# select calculate.py's resume-aware validation via --resume-run.
if [ -n "${_CHECKPOINT_LOAD_PATH}" ] || [ "${_SEED_CHECKPOINT}" = "true" ]; then
  MIN_RESTORE_DATAPOINTS=1
  RESUME_ARGS=(--resume-run)
fi
# Run the calculator over the current raw metrics into the summary file $1.
run_calculate() {
  python3 -m metrics.calculate \
      --run-id "$RUN_ID" --workload-name "${_WORKLOAD}" \
      --requirements "${_REQUIREMENTS}" --in-dir "$RAW_DIR" --out-file "$1" \
      --expected-steps "${_STEPS}" \
      --min-write-datapoints "$MIN_WRITE_DATAPOINTS" \
      --min-restore-datapoints "$MIN_RESTORE_DATAPOINTS" \
      "${RESUME_ARGS[@]}" \
      --require-data-loading-metrics \
      --bucket-type "${_BUCKET_TYPE}" --zone "${_ZONE}" --region "$REGION" \
      --machine-type "${_MACHINE_TYPE}" \
      --nodes "${_NODES}" --ranks-per-node "${_RANKS_PER_NODE}" \
      --steps "${_STEPS}" --checkpoint-interval "${_CHECKPOINT_INTERVAL}" \
      --checkpoints-to-keep "${_CKPT_TO_KEEP}" \
      --dataset-path "${_DATASET_PATH}" --model-id "${_MODEL_ID}" \
      --image "${_IMAGE}" \
      --training-strategy "${_TRAINING_STRATEGY}" \
      --simulated-step-compute-seconds "${_SIMULATED_STEP_COMPUTE_SECONDS}" \
      --per-device-batch "${_PER_DEVICE_BATCH}" --grad-accum "${_GRAD_ACCUM}" \
      --dataloader-workers "${_DATALOADER_WORKERS}"
}
# Cloud Logging ingestion lags pod termination by seconds-to-minutes, and the
# last logs emitted (the final checkpoint write and the profiler summary that
# carries the data-loading metric) are the most likely to still be in flight
# when the JobSet reports Completed. Settle once, then re-scrape at a fixed 60s
# interval until the required metrics validate (or attempts are exhausted).
# run_calculate exits non-zero when metrics are incomplete; running it as an
# `if` condition keeps `set -e`/the ERR trap from aborting the step on a
# not-yet-complete attempt.
sleep 60
SCRAPE_OK=false
for attempt in $(seq 1 5); do
  echo "Scrape attempt $attempt of 5..."
  rm -rf "$RAW_DIR"
  # The parser hits the Cloud Logging API; a transient API error should fall
  # through to the backoff like the ingestion-lag case, not abort the step via
  # set -e / the ERR trap. Guard it in a condition so a failure retries.
  if ! python3 -m metrics.parsers.hf \
      --run-id "$RUN_ID" --project "${PROJECT_ID}" \
      --start-time "$START_TIME" --end-time "$END_TIME" \
      --checkpoint-location "gs://$CHECKPOINT_BUCKET/checkpoints" \
      --out-dir "$RAW_DIR"; then
    echo "Scrape failed (transient?); waiting before retry..."
    sleep 60
    continue
  fi
  if run_calculate "$SUMMARY"; then
    SCRAPE_OK=true
    break
  fi
  echo "Required metrics incomplete; waiting for Cloud Logging ingestion..."
  sleep 60
done
if [ "$SCRAPE_OK" != "true" ]; then
  echo "Metrics still incomplete after retries."
  # Record explicitly: bash's ERR trap does NOT fire on `exit`, so without this
  # the allowFailure step leaves the FAILED ledger empty and check-failure would
  # green a run that uploaded no summary.
  record_failure scrape-metrics
  exit 1
fi
# Best-effort system metrics: fetched once after the required metrics validate
# (not per attempt, to avoid re-du'ing the dataset bucket). Settle for GCS
# metric lag, then fold into the summary; a failure here must not lose the
# metrics-complete summary already written above, so it's `|| true`/warn-only.
sleep "${SYSTEM_METRICS_SETTLE_SECONDS:-600}"
python3 -m metrics.monitoring \
  --project "${PROJECT_ID}" --run-id "$RUN_ID" \
  --start-time "$START_TIME" --end-time "$END_TIME" \
  --checkpoint-bucket "$CHECKPOINT_BUCKET" --dataset-bucket "$DATASET_BUCKET" \
  --cluster "$CLUSTER_NAME" \
  --out-dir "$RAW_DIR" || true
run_calculate "$SUMMARY" \
  || echo "Warning: recompute with system metrics failed; uploading summary without them."
gcloud storage cp "$SUMMARY" "gs://$RESULTS_BUCKET/branch=$BRANCH_NAME/$DATE_DIR/$RUN_ID/${TS_DIR}.csv"
