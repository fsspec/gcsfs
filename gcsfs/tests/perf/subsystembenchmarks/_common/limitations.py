"""Registry of proven framework/accelerator limitations with repro citations."""

from __future__ import annotations

import dataclasses
from typing import Callable


@dataclasses.dataclass(frozen=True)
class Limitation:
    group: str
    accelerator: str
    applies: Callable[[object], bool]
    reason: str


KNOWN: tuple[Limitation, ...] = (
    Limitation(
        group="checkpointing/pytorch_lightning",
        accelerator="tpu",
        applies=lambda p: True,
        reason=(
            "PyTorch Lightning does not support TorchTPU; see "
            "gcsfs/tests/perf/subsystembenchmarks/limitations.md#lightning-tpu"
        ),
    ),
)


def limitation_reason(group: str, params, accelerator: str) -> str | None:
    """Return the first matching limitation skip reason for (group, params, accelerator)."""
    for entry in KNOWN:
        if (
            entry.group == group
            and entry.accelerator == accelerator
            and entry.applies(params)
        ):
            return entry.reason
    return None
