#!/bin/bash
set -e

echo '--- Installing dependencies on VM ---'
sudo apt-get update > /dev/null
sudo apt-get install -y python3-pip python3-venv fuse fuse3 libfuse2 git > /dev/null

echo '--- Installing Python and dependencies on VM ---'
python3 -m venv env
source env/bin/activate

pip install --upgrade pip > /dev/null
pip install pytest pytest-timeout pytest-subtests pytest-asyncio build hatchling hatch-vcs fusepy google-cloud-storage > /dev/null
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
