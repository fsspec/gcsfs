import math

from torch.distributed.checkpoint.default_planner import (
    DefaultLoadPlanner,
    DefaultSavePlanner,
)
from torch.distributed.checkpoint.planner import LoadItemType, LoadPlan, SavePlan


class RecordingLoadPlanner(DefaultLoadPlanner):
    """Records item count, logical tensor bytes, and storage bytes fetched on this rank."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.read_items: int = 0
        self.bytes_requested: int = 0
        self.bytes_fetched: int = 0

    def _record(self, plan: LoadPlan) -> LoadPlan:
        self.read_items = len(plan.items)
        if self.metadata is None:
            self.bytes_requested = 0
            self.bytes_fetched = 0
            return plan
        md = self.metadata.state_dict_metadata
        self.bytes_requested = sum(
            math.prod(item.lengths) * md[item.dest_index.fqn].properties.dtype.itemsize
            for item in plan.items
            if item.type == LoadItemType.TENSOR
        )
        if self.metadata.storage_data is None:
            self.bytes_fetched = 0
        else:
            storage = self.metadata.storage_data
            storage_indices = {
                item.storage_index
                for item in plan.items
                if item.type in (LoadItemType.TENSOR, LoadItemType.BYTE_IO)
            }
            self.bytes_fetched = sum(storage[idx].length for idx in storage_indices)
        return plan

    def create_local_plan(self) -> LoadPlan:
        return self._record(super().create_local_plan())

    def finish_plan(self, new_plan: LoadPlan) -> LoadPlan:
        return self._record(super().finish_plan(new_plan))


class RecordingSavePlanner(DefaultSavePlanner):
    """Records tensor chunk bytes written on this rank."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bytes_written: int = 0

    def finish_plan(self, new_plan: SavePlan) -> SavePlan:
        plan = super().finish_plan(new_plan)
        self.bytes_written = sum(
            math.prod(item.tensor_data.chunk.sizes)
            * item.tensor_data.properties.dtype.itemsize
            for item in plan.items
            if item.tensor_data is not None
        )
        return plan
