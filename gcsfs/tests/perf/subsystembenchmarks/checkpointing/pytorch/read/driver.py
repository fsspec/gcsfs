from __future__ import annotations

from gcsfs.tests.perf.subsystembenchmarks._common import nodes
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import (
    CheckpointResult,
    ResourceSnapshot,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch import common


class PyTorchCheckpointReadDriver(common.DCPDriver):
    def setup(self, prefix, params):
        source = params.source_layout()
        self.prepare(params, source, params.target_layout())
        common.run_worker(
            "save_source",
            prefix,
            params,
            world_size=source.world_size,
            rdzv_id=f"{params.name}-setup",
            node_group=nodes.NodeGroup.from_env(),
        )

    def run(self, prefix, params):
        node_group = nodes.NodeGroup.from_env()
        data = common.run_worker(
            "load",
            prefix,
            params,
            world_size=params.target_layout().world_size,
            rdzv_id=f"{params.name}-load",
            node_group=node_group,
            expect_result=True,
        )
        if data is None:
            return CheckpointResult(durations=[])

        stats = data["stats"]
        fetched = [s["bytes_fetched"] for s in stats]
        total_fetched = sum(fetched)
        return CheckpointResult(
            durations=data["durations"],
            extra_columns={
                "dcp_read_items_per_rank_max": max(s["read_items"] for s in stats),
                "dcp_read_bytes_fetched_total": total_fetched,
                "dcp_read_bytes_fetched_per_rank_max": max(fetched),
            },
            bytes_transferred=total_fetched,
            window=tuple(data["window"]),
            resource_metrics=ResourceSnapshot(**data["resource_metrics"]),
        )
