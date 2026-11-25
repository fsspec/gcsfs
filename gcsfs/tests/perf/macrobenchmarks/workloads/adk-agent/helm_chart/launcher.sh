#!/usr/bin/env bash
# Installs the ADK and gcsfs builds under test, then runs one scenario of
# adk_bench.py. Metric lines go to stdout, which Cloud Logging collects.
set -euo pipefail

export PYTHONUNBUFFERED=1
export PYTHONPATH=${PYTHONPATH:-}:/workload/configs
export ADK_ENABLE_FSSPEC_ARTIFACT_SERVICE=1

: "${SCENARIO:?SCENARIO is required}"
: "${CKPT_WRITE_PATH:?CKPT_WRITE_PATH (artifact root) is required}"

pip3 install --no-cache-dir -r /workload/configs/requirements.txt
if [[ -n "${REQUIREMENTS:-}" ]]; then
  # Word splitting is intentional: REQUIREMENTS holds whitespace-separated pip
  # specs (the gcsfs build under test and any custom google-adk build).
  # shellcheck disable=SC2086
  pip3 install --no-cache-dir $REQUIREMENTS
  # Reinstall only the explicitly listed packages so one override's dependency
  # pass cannot replace another's.
  # shellcheck disable=SC2086
  pip3 install --no-cache-dir --no-deps --force-reinstall $REQUIREMENTS
fi

python3 - <<'PY'
import importlib.metadata
from pathlib import Path

import gcsfs
import google.adk
from google.adk.integrations.fsspec import FsspecArtifactService  # noqa: F401

distribution = importlib.metadata.distribution("gcsfs")
module = Path(gcsfs.__file__).resolve()
package = Path(distribution.locate_file("gcsfs")).resolve()
if module.parent != package or gcsfs.__version__ != distribution.version:
    raise RuntimeError(
        f"unexpected gcsfs: file={module}, version={gcsfs.__version__}, "
        f"distribution={package}, distribution_version={distribution.version}"
    )
print(
    f"Workload Versions : gcsfs={gcsfs.__version__} "
    f"google-adk={google.adk.__version__}"
)
PY

exec python3 /workload/configs/adk_bench.py \
  --scenario "$SCENARIO" \
  --base-url "${CKPT_WRITE_PATH%/}/adk" \
  --run-tag "${RUN_ID:-local}"
