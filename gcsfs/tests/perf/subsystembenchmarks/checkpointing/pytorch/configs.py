import dataclasses
from typing import Optional

from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import MAX_RANKS
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.configurator import (
    OneFactorCheckpointConfigurator,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.device import (
    device_capacity,
    run_device,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parameters import (
    PyTorchCheckpointParameters,
)


@dataclasses.dataclass(frozen=True)
class HostConfig:
    nnodes: int = 1
    nproc_per_node: int = MAX_RANKS


def host_config(device: str) -> HostConfig:
    return HostConfig(nnodes=1, nproc_per_node=device_capacity(device))


def placement_skip_reason(
    p: PyTorchCheckpointParameters, host: HostConfig
) -> Optional[str]:
    n = p.max_world_size()
    if n > host.nnodes * host.nproc_per_node:
        return f"needs {n} ranks, run has {host.nnodes}x{host.nproc_per_node}"
    return None


def case_params(
    configurator_cls,
    configs_file: str,
    host: Optional[HostConfig] = None,
) -> list:
    import pytest

    if host is None:
        host = host_config(run_device())
    params = []
    for case in configurator_cls(configs_file).generate_cases():
        reason = placement_skip_reason(case, host)
        marks = [pytest.mark.skip(reason=reason)] if reason else []
        params.append(pytest.param(case, id=case.name, marks=marks))
    return params


class PyTorchCheckpointConfigurator(OneFactorCheckpointConfigurator):
    FRAMEWORK = "pytorch"
    PARAMS_CLASS = PyTorchCheckpointParameters
    REQUIRES_SOURCE: bool = False

    def validate_case(self, p):
        setup = (
            p.setup_world_size,
            p.setup_tensor_parallel_size,
            p.setup_data_parallel_size,
        )
        if getattr(p, "_has_setup_keys", False) or setup != (
            p.world_size,
            p.tensor_parallel_size,
            p.data_parallel_size,
        ):
            raise ValueError("set the saved layout with source:, not setup_* keys")
        if self.REQUIRES_SOURCE:
            if p.source is None:
                raise ValueError(
                    f"{type(self).__name__} requires source: on every case"
                )
        elif p.source is not None:
            raise ValueError(f"{type(self).__name__} does not accept source: on cases")
        target = p.target_layout()
        source = p.source_layout()
        target.validate()
        source.validate()
        if self.REQUIRES_SOURCE and source == target:
            raise ValueError(
                f"{type(self).__name__} requires source layout != target layout, "
                f"got {target}"
            )
        super().validate_case(p)
