"""Driver for Ray checkpoint save benchmark."""

import os
import shutil
import time

import ray
import torch
import torch.distributed as dist
import torch.distributed.checkpoint as dcp
from torch.distributed.checkpoint.state_dict import StateDictOptions, get_state_dict

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import (
    CheckpointDriver,
    CheckpointResult,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_data.common import (
    _get_staging_dir,
    _pyarrow_fs_copy_files,
    ensure_ray_initialized,
    find_free_port,
    load_benchmark_model,
    materialize_adamw_states,
    parallelize_model,
    resolve_storage,
    setup_distributed_env,
)


@ray.remote
class RayCheckpointSaveWorker:
    """Ray Actor executing distributed checkpoint save operations on CPU."""

    def __init__(self, rank, world_size, port, prefix, params):
        self.rank = rank
        self.world_size = world_size
        self.port = port
        self.prefix = prefix
        self.params = params

    def setup(self):
        setup_distributed_env(self.rank, self.world_size, self.port)
        self.model = load_benchmark_model(self.params)
        self.model = parallelize_model(self.model, self.params)
        self.optimizer = torch.optim.AdamW(self.model.parameters(), lr=1e-3)
        materialize_adamw_states(self.optimizer)
        self.fs, self.arrow_fs, self.base_path = resolve_storage(self.prefix)
        self.destination_ckpt = f"{self.base_path.rstrip('/')}/model.ckpt"

    def save_rounds(self):
        try:
            durations = []
            is_sharded = self.params.strategy in (
                "fsdp_sharded",
                "model_parallel_sharded",
            )
            options = StateDictOptions(
                full_state_dict=not is_sharded,
                cpu_offload=not is_sharded,
            )

            for round_idx in range(self.params.rounds):
                dist.barrier()
                t_start = time.perf_counter()

                local_dir = None
                try:
                    # get_state_dict is a collective operation across all ranks for distributed strategies
                    model_state, opt_state = get_state_dict(
                        self.model, self.optimizer, options=options
                    )
                    app_state = {"model": model_state, "optimizer": opt_state}

                    if is_sharded:
                        local_dir = _get_staging_dir(
                            f"ray-ckpt-r{round_idx}-rank{self.rank}-"
                        )
                        dcp.save(
                            {"app": app_state},
                            storage_writer=dcp.FileSystemWriter(local_dir),
                        )

                        self.arrow_fs.create_dir(self.destination_ckpt)
                        _pyarrow_fs_copy_files(
                            local_dir,
                            self.destination_ckpt,
                            destination_filesystem=self.arrow_fs,
                        )
                    else:
                        if self.rank == 0:
                            local_dir = _get_staging_dir(
                                f"ray-ckpt-r{round_idx}-rank0-"
                            )
                            ckpt_file = os.path.join(local_dir, "checkpoint.pt")
                            torch.save(app_state, ckpt_file)

                            self.arrow_fs.create_dir(self.destination_ckpt)
                            _pyarrow_fs_copy_files(
                                local_dir,
                                self.destination_ckpt,
                                destination_filesystem=self.arrow_fs,
                            )

                    del app_state, model_state, opt_state
                    dist.barrier()
                    if is_sharded and self.rank == 0:
                        self.fs.touch(f"{self.destination_ckpt.rstrip('/')}/_SUCCESS")
                    t_end = time.perf_counter()
                    durations.append((t_start, t_end))
                finally:
                    if local_dir and os.path.exists(local_dir):
                        shutil.rmtree(local_dir, ignore_errors=True)

            return durations
        finally:
            dist.destroy_process_group()


def run_ray_save(prefix, params):
    """Runs single or distributed checkpoint save benchmark across Ray actor workers."""
    ensure_ray_initialized()
    world_size = params.world_size
    port = find_free_port()

    workers = [
        RayCheckpointSaveWorker.remote(rank, world_size, port, prefix, params)
        for rank in range(world_size)
    ]
    ray.get([w.setup.remote() for w in workers])
    results = ray.get([w.save_rounds.remote() for w in workers])

    durations = []
    for r in range(params.rounds):
        begins = [results[rank][r][0] for rank in range(world_size)]
        ends = [results[rank][r][1] for rank in range(world_size)]
        durations.append(max(ends) - min(begins))
    return durations


class RayCheckpointWriteDriver(CheckpointDriver):
    """Driver for Ray checkpoint save benchmarks."""

    def setup(self, prefix: str, params):
        """No-op for checkpoint save; setup is handled per-round in run()."""
        pass

    def run(self, prefix: str, params) -> CheckpointResult:
        """Executes the checkpoint save benchmark across Ray actor workers."""
        try:
            durations = run_ray_save(prefix, params)
            return CheckpointResult(durations=durations)
        finally:
            if ray.is_initialized():
                ray.shutdown()
