"""Tiny torchrun worker for launcher tests: records what each rank sees."""

import json
import os
import pathlib
import sys

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator


def main() -> None:
    rank = int(os.environ["RANK"])
    report = {
        "rank": rank,
        "world_size": int(os.environ["WORLD_SIZE"]),
        "device": accelerator.device_kind(),
        "tpu_env": sorted(k for k in os.environ if k.startswith("TORCH_TPU_")),
    }
    out = pathlib.Path(sys.argv[1]) / f"rank-{rank}.json"
    out.write_text(json.dumps(report))


if __name__ == "__main__":
    main()
