"""Torch-free checks for the Ray workload launcher."""

import re
import subprocess
from pathlib import Path

LAUNCHER = Path(__file__).parents[1] / "helm_chart" / "launcher.sh"


def test_launcher_is_valid_bash():
    subprocess.run(["bash", "-n", str(LAUNCHER)], check=True)


def test_hf_model_download_uses_hf_cli():
    # transformers>=5 pulls huggingface_hub>=1, where `huggingface-cli` is a stub
    # that exits 1, so an HF repo MODEL_ID would fail pod startup.
    code = [
        line
        for line in LAUNCHER.read_text().splitlines()
        if not line.lstrip().startswith("#")
    ]
    assert not any("huggingface-cli" in line for line in code)
    assert any(re.search(r'\bhf download "\$MODEL_ID"', line) for line in code)
