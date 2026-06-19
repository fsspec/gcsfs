#!/usr/bin/env bash
# check-failure: fail the build if any allowFailure step recorded a failure,
# listing the culprits.
source "$(dirname "$0")/lib.sh"
if [[ -f "${FAILED_FILE}" ]]; then
  echo "Build failed. Steps that reported failures:"
  sort -u "${FAILED_FILE}" | sed 's/^/ - /'
  exit 1
fi
echo "Build successful."
