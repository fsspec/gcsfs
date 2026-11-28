from __future__ import annotations

from gcsfs.tests.perf.subsystembenchmarks._common import nodes
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import (
    CheckpointResult,
    ResourceSnapshot,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch import common


def _step_prefix(prefix: str, step: int) -> str:
    return f"{prefix.rstrip('/')}/step-{step}/"


class PyTorchCheckpointWriteDriver(common.DCPDriver):
    def setup(self, prefix, params):
        self.prepare(params, params.target_layout())

    def run(self, prefix, params):
        node_group = nodes.NodeGroup.from_env()
        world_size = params.target_layout().world_size

        def _verify() -> None:
            common.run_worker(
                "verify_saved",
                _step_prefix(prefix, params.rounds - 1),
                params,
                world_size=world_size,
                rdzv_id=f"{params.name}-verify",
                node_group=node_group,
            )

        data = common.run_worker(
            "save",
            prefix,
            params,
            world_size=world_size,
            rdzv_id=f"{params.name}-save",
            node_group=node_group,
            expect_result=True,
        )
        if data is None:
            return CheckpointResult(durations=[], verify=_verify)

        stats = data["stats"]
        max_written = max(s["bytes_written"] for s in stats)
        return CheckpointResult(
            durations=data["durations"],
            extra_columns={"dcp_write_bytes_per_rank_max": max_written},
            verify=_verify,
            window=tuple(data["window"]),
            resource_metrics=ResourceSnapshot(**data["resource_metrics"]),
        )
