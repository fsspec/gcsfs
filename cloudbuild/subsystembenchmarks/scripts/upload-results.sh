#!/usr/bin/env bash
set -euo pipefail

source "$HOME/ssb_cloudbuild.env"
cd "$HOME/gcsfs"
# _MODE=ci (run-ci.sh) uses per-group venvs instead of env/; the upload only needs gcloud.
if [[ -f env/bin/activate ]]; then
  source env/bin/activate
fi

DATE_DIR=$(date +%Y%m%d)
RESULTS_DIR="gcsfs/tests/perf/subsystembenchmarks/__run__"
if [[ -d "$RESULTS_DIR" ]]; then
  echo "Uploading subsystem benchmark result artifacts to gs://$RESULTS_BUCKET/subsystembenchmarks/branch=$BRANCH_NAME/$DATE_DIR/$RUN_ID/"
  gcloud storage cp --recursive "$RESULTS_DIR"/* \
    "gs://$RESULTS_BUCKET/subsystembenchmarks/branch=$BRANCH_NAME/$DATE_DIR/$RUN_ID/"
fi
