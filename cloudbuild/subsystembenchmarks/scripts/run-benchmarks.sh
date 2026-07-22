#!/usr/bin/env bash
set -euo pipefail

source "$HOME/ssb_cloudbuild.env"
cd "$HOME/gcsfs"
source env/bin/activate

python gcsfs/tests/perf/subsystembenchmarks/run.py \
  "--group=$GROUP" \
  "--sweep-axes=$SWEEP_AXES" \
  "--bucket-prefix=$BUCKET_PREFIX" \
  "--bucket-type=$BUCKET_TYPE" \
  "--project=$PROJECT_ID" \
  "--location=$REGION" \
  "--zone=$ZONE" \
  --require-amplification
