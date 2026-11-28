"""Single-node TPU wrapper that prepares TPU launch env before exec'ing torchrun."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence

_TPU_TOPOLOGY_ENV = "TORCH_TPU_TOPOLOGY"
_TPU_SLICEBUILDER_ADDRESSES_ENV = "TORCH_TPU_SLICEBUILDER_ADDRESSES"


def main(argv: Sequence[str] | None = None) -> None:
    raw = list(sys.argv[1:] if argv is None else argv)
    if "--" in raw:
        sep = raw.index("--")
        own_args, rest = raw[:sep], raw[sep + 1 :]
    else:
        own_args, rest = raw, []

    parser = argparse.ArgumentParser(
        description="Configure single-node TPU launch env and exec torch.distributed.run."
    )
    parser.add_argument("--nproc-per-node", type=int, required=True)
    parsed = parser.parse_args(own_args)
    if parsed.nproc_per_node < 1:
        parser.error("--nproc-per-node must be >= 1")
    if not rest:
        parser.error("expected torchrun arguments after '--'")

    os.environ.pop(_TPU_TOPOLOGY_ENV, None)
    os.environ.pop(_TPU_SLICEBUILDER_ADDRESSES_ENV, None)

    import torch.tpu.distributed

    torch.tpu.distributed.set_tpu_launch_env(nproc_per_node=parsed.nproc_per_node)
    os.execv(
        sys.executable,
        [sys.executable, "-m", "torch.distributed.run", *rest],
    )


if __name__ == "__main__":
    main()
