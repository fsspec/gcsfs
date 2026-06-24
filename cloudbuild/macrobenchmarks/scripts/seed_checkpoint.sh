#!/usr/bin/env bash
# seed-checkpoint: when _SEED_CHECKPOINT=true and no external _CHECKPOINT_LOAD_PATH
# is supplied, run the real workload once (1 step) to write a single,
# format-exact checkpoint into the per-run checkpoint bucket, then export its
# path for run-workload to restore from. The seed runs under its own helm
# release (${RUN_ID}-seed) so its logs never enter the benchmark's metrics
# (the scraper filters by the benchmark RUN_ID). Skips entirely when seeding is
# disabled or an external checkpoint is supplied (that path takes precedence).
set -e
source "$(dirname "$0")/lib.sh"
trap 'record_failure seed-checkpoint' ERR
skip_if_failed
source "${BUILD_VARS_FILE}"

if [ "${_SEED_CHECKPOINT}" != "true" ] || [ -n "${_CHECKPOINT_LOAD_PATH}" ]; then
  echo "Seeding disabled or external checkpoint supplied; skipping seed-checkpoint."
  exit 0
fi

SEED_RUN_ID="${RUN_ID}-seed"
SEED_CKPT_DIR="gs://$CHECKPOINT_BUCKET/seed"
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
gcloud container clusters get-credentials "$CLUSTER_NAME" --zone="${_ZONE}" --project="${PROJECT_ID}"
CHART="gcsfs/tests/perf/macrobenchmarks/workloads/${_WORKLOAD}/helm_chart"

# One optimizer step (gradient accumulation is fixed at 1 in launcher.sh) with
# ckptWriterInterval=1 writes exactly one checkpoint at global_step 1. The
# checkpoint is full-size regardless of step count: the frozen ~16 GB model plus
# eagerly-materialized AdamW state are serialized the same as in a long run.
# simulatedStepComputeSeconds=0 makes the single step instant.
echo "Installing seed release $SEED_RUN_ID to write one checkpoint to $SEED_CKPT_DIR ..."
helm install "$SEED_RUN_ID" "$CHART" -f "$CHART/values_base.yaml" \
  --set gcsfs.datasetPath="${_DATASET_PATH}" \
  --set gcsfs.ckptWritePath="$SEED_CKPT_DIR" \
  --set-string gcsfs.ckptLoadPath="" \
  --set workload.modelId="${_MODEL_ID}" \
  --set workload.hfToken="${_HF_TOKEN}" \
  --set workload.steps="1" \
  --set workload.ckptWriterInterval="1" \
  --set workload.nodes="${_NODES}" \
  --set workload.requirements="${_REQUIREMENTS}" \
  --set workload.trainingStrategy="${_TRAINING_STRATEGY}" \
  --set workload.simulatedStepComputeSeconds="0" \
  --set serviceAccount=default

if ! wait_for_jobset "$SEED_RUN_ID" seed-checkpoint; then
  helm uninstall "$SEED_RUN_ID" || true
  exit 1
fi

# The workload writes to ${ckptWritePath}/${RUN_ID}/...; RUN_ID inside the seed
# pod is the seed release name. Select the single checkpoint entry: a *.ckpt
# object for DDP, a *.ckpt/ prefix for FSDP. Strip any trailing slash.
SEEDED_CKPT_PATH=$(gcloud storage ls "$SEED_CKPT_DIR/$SEED_RUN_ID/" | grep -E '\.ckpt/?$' | head -1 | sed 's#/$##')
if [ -z "$SEEDED_CKPT_PATH" ]; then
  echo "ERROR: no seed checkpoint found under $SEED_CKPT_DIR/$SEED_RUN_ID/."
  helm uninstall "$SEED_RUN_ID" || true
  record_failure seed-checkpoint
  exit 1
fi
echo "Seed checkpoint: $SEEDED_CKPT_PATH"
echo "export SEEDED_CKPT_PATH=$SEEDED_CKPT_PATH" >> "${BUILD_VARS_FILE}"

# Free the node pool so the benchmark run can schedule on all $_NODES nodes.
helm uninstall "$SEED_RUN_ID" || true
