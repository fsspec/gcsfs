#!/bin/bash
set -e

echo '--- Installing dependencies on VM ---'
sudo apt-get update > /dev/null
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y python3-pip python3-venv fuse fuse3 libfuse2 git > /dev/null

echo '--- Installing Python and dependencies on VM ---'
python3 -m venv env
source env/bin/activate

pip install --upgrade pip > /dev/null
pip install pytest pytest-timeout pytest-subtests pytest-asyncio pytest-xdist build hatchling hatch-vcs fusepy google-cloud-storage > /dev/null
# Upstream frameworks for telemetry e2e tests (gcsfs/tests/test_telemetry_e2e.py).
# CPU-only torch wheels avoid pulling multi-GB CUDA dependencies onto the test VM.
# Non-fatal: those tests importorskip the frameworks, so an index outage must not fail every suite.
pip install torch --index-url https://download.pytorch.org/whl/cpu > /dev/null \
    || echo "WARN: torch not installed; telemetry e2e torch/lightning tests will be skipped"
pip install pandas lightning > /dev/null \
    || echo "WARN: pandas/lightning not installed; telemetry e2e tests will be skipped"
if [ "$INSTALL_GCSFS_FROM_PYPI" = "true" ]; then
    echo '--- Installing gcsfs from PyPI ---'
    pip install gcsfs > /dev/null
else
    echo '--- Installing gcsfs from local source ---'
    pip install -e . > /dev/null
fi

if [ "$INSTALL_FSSPEC_HEAD" = "true" ]; then
    echo '--- Installing fsspec HEAD ---'
    pip install --force-reinstall git+https://github.com/fsspec/filesystem_spec.git > /dev/null
    echo "fsspec version: $(python3 -c 'import fsspec; print(fsspec.__version__)')"
fi
