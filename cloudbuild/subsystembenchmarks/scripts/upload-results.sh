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
# BigQuery ingestion reads gs://<prefix>-run-results/subsystembenchmarks/*.csv, so _MODE=ci
# artifacts go to a sibling prefix it never reads, even under a nightly _INFRA_PREFIX.
ARTIFACT_ROOT="subsystembenchmarks"
if [[ "${MODE:-nightly}" == "ci" ]]; then
  ARTIFACT_ROOT="subsystembenchmarks-ci"
fi
DEST="gs://$RESULTS_BUCKET/$ARTIFACT_ROOT/branch=$BRANCH_NAME/$DATE_DIR/$RUN_ID/"
if [[ -d "$RESULTS_DIR" ]]; then
  echo "Uploading subsystem benchmark result artifacts to $DEST"
  gcloud storage cp --recursive "$RESULTS_DIR"/* "$DEST"
fi
