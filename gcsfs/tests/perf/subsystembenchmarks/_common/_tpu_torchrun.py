"""Single-host TPU wrapper: set the TorchTPU launch env, then exec torchrun.

``launcher.run_torchrun`` starts this module with a clean ``TORCH_TPU_*`` env;
``set_tpu_launch_env`` runs in this short-lived process so the benchmark parent
never imports TorchTPU distributed state.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> None:
    raw = list(sys.argv[1:] if argv is None else argv)
    if "--" in raw:
        sep = raw.index("--")
        own_args, rest = raw[:sep], raw[sep + 1 :]
    else:
        own_args, rest = raw, []

    parser = argparse.ArgumentParser(
        description="Configure single-host TPU launch env and exec torch.distributed.run."
    )
    parser.add_argument("--nproc-per-node", type=int, required=True)
    parsed = parser.parse_args(own_args)
    if parsed.nproc_per_node < 1:
        parser.error("--nproc-per-node must be >= 1")
    if not rest:
        parser.error("expected torchrun arguments after '--'")

    import torch.tpu.distributed

    torch.tpu.distributed.set_tpu_launch_env(nproc_per_node=parsed.nproc_per_node)
    os.execv(
        sys.executable,
        [sys.executable, "-m", "torch.distributed.run", *rest],
    )


if __name__ == "__main__":
    main()
