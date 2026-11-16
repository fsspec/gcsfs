#!/usr/bin/env bash
# upload-summary: copy the summary CSV that scrape-metrics produced to the
# results bucket. Split out of scrape-metrics so that step can run on a Python
# image while gcloud stays on the gcloud image; scrape-metrics hands over the
# local path and destination via SUMMARY_FILE / SUMMARY_URI in the build vars.
set -e
source "$(dirname "$0")/lib.sh"
trap 'record_failure upload-summary' ERR
skip_if_failed
source "${BUILD_VARS_FILE}"
if [[ -z "${SUMMARY_FILE:-}" || -z "${SUMMARY_URI:-}" ]]; then
  # `exit` does not fire the ERR trap; record explicitly so check-failure
  # doesn't green a run that uploaded nothing.
  echo "ERROR: scrape-metrics did not record SUMMARY_FILE/SUMMARY_URI; nothing to upload."
  record_failure upload-summary
  exit 1
fi
gcloud storage cp "${SUMMARY_FILE}" "${SUMMARY_URI}"
