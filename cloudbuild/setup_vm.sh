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
pip install -e . > /dev/null
