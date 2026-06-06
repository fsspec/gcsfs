#!/usr/bin/env bash

# This script polls the GitHub check runs API to wait for all Google Cloud Build
# checks on the current commit to complete and pass.

set -eo pipefail

COMMIT_SHA="${GITHUB_SHA}"
POLL_INTERVAL=120
TIMEOUT=2700

if [ -z "$COMMIT_SHA" ]; then
  echo "❌ Error: GITHUB_SHA environment variable is not set."
  exit 1
fi

if [ -z "$GITHUB_REPOSITORY" ]; then
  echo "❌ Error: GITHUB_REPOSITORY environment variable is not set."
  exit 1
fi

echo "Waiting for Cloud Build checks to complete for commit ${COMMIT_SHA} in ${GITHUB_REPOSITORY}..."
START_TIME=$(date +%s)

while true; do
  if API_RESPONSE=$(gh api "repos/${GITHUB_REPOSITORY}/commits/${COMMIT_SHA}/check-runs?per_page=100" 2>&1); then
    CHECK_RUNS_JSON="$API_RESPONSE"
  else
    API_EXIT_CODE=$?
    echo "⚠️ Warning: gh api call failed with exit status $API_EXIT_CODE. Error: $API_RESPONSE"
    CHECK_RUNS_JSON=""
  fi

  if [ -n "$CHECK_RUNS_JSON" ]; then
    if LATEST_CHECK_RUNS=$(echo "$CHECK_RUNS_JSON" | jq -r '[.check_runs[] | select(.app.slug == "google-cloud-build")] | group_by(.name) | map(sort_by(.id) | last) | .[] | "\(.name):\(.status):\(.conclusion)"' 2>/dev/null); then
      :
    else
      echo "⚠️ Warning: Failed to parse check runs JSON."
      LATEST_CHECK_RUNS=""
    fi
  else
    LATEST_CHECK_RUNS=""
  fi

  if [ -n "$LATEST_CHECK_RUNS" ]; then
    ALL_CHECKS_PASSED=true
    ANY_CHECK_FAILED=false
    FAILED_CHECK_NAME=""
    FAILED_CHECK_CONCLUSION=""
    ANY_CHECK_IN_PROGRESS=false

    while IFS= read -r CHECK_RUN_RECORD; do
      [ -z "$CHECK_RUN_RECORD" ] && continue
      CHECK_NAME=$(echo "$CHECK_RUN_RECORD" | cut -d':' -f1)
      CHECK_STATUS=$(echo "$CHECK_RUN_RECORD" | cut -d':' -f2)
      CHECK_CONCLUSION=$(echo "$CHECK_RUN_RECORD" | cut -d':' -f3)

      echo "Found Cloud Build check run: $CHECK_NAME. Status: $CHECK_STATUS, Conclusion: $CHECK_CONCLUSION"

      if [ "$CHECK_STATUS" = "completed" ]; then
        if [ "$CHECK_CONCLUSION" != "success" ]; then
          ALL_CHECKS_PASSED=false
          ANY_CHECK_FAILED=true
          FAILED_CHECK_NAME="$CHECK_NAME"
          FAILED_CHECK_CONCLUSION="$CHECK_CONCLUSION"
        fi
      else
        ALL_CHECKS_PASSED=false
        ANY_CHECK_IN_PROGRESS=true
      fi
    done <<< "$LATEST_CHECK_RUNS"

    if [ "$ALL_CHECKS_PASSED" = true ]; then
      echo "✅ All Cloud Build checks passed!"
      exit 0
    elif [ "$ANY_CHECK_FAILED" = true ]; then
      echo "❌ Cloud Build check '$FAILED_CHECK_NAME' failed with conclusion: $FAILED_CHECK_CONCLUSION"
      exit 1
    elif [ "$ANY_CHECK_IN_PROGRESS" = true ]; then
      echo "Waiting for all Cloud Build checks to complete..."
    fi
  else
    echo "Waiting for Cloud Build check(s) to appear..."
  fi

  CURRENT_TIME=$(date +%s)
  ELAPSED=$((CURRENT_TIME - START_TIME))
  if [ "$ELAPSED" -ge "$TIMEOUT" ]; then
    echo "❌ Timeout waiting for Cloud Build check after ${TIMEOUT} seconds."
    exit 1
  fi

  sleep "$POLL_INTERVAL"
done
