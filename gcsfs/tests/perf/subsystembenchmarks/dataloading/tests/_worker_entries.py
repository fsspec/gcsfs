"""Rank entry point and params that ``dataloading/tests`` run through ``_worker``.

They live in their own module so ``_worker`` resolves them by import path, the
same way it resolves the real HF and WebDataset entry points.
"""

import dataclasses

from gcsfs.tests.perf.subsystembenchmarks.dataloading import driver

calls = []


@dataclasses.dataclass
class RecordParams:
    world_size: int = 1
    rounds: int = 1
    batch_size: int = 4

    @property
    def total_rows(self):
        return self.rounds * self.batch_size


def record(rank, world_size, prefix, params, barrier, extra):
    """Record the call, wait at the round barrier, and return fixed rank metrics."""
    calls.append((rank, world_size, prefix, params, barrier, extra))
    driver.await_round(barrier)
    return [(10.0, 11.5, params.total_rows)], 0.25, 0.5
