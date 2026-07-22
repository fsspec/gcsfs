#!/usr/bin/env bash
set -euo pipefail

source "$HOME/ssb_cloudbuild.env"
cd "$HOME/gcsfs"
source env/bin/activate

DATE_DIR=$(date +%Y%m%d)
RESULTS_DIR="gcsfs/tests/perf/subsystembenchmarks/__run__"
if [[ -d "$RESULTS_DIR" ]]; then
  gcloud storage cp --recursive "$RESULTS_DIR"/* \
    "gs://$RESULTS_BUCKET/subsystembenchmarks/branch=$BRANCH_NAME/$DATE_DIR/$RUN_ID/"
fi
