"""Driver for Ray checkpoint save benchmark."""

import os
import shutil
import time

import ray
import torch.distributed as dist

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import (
    CheckpointDriver,
    CheckpointResult,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_pytorch.common import (
    ensure_ray_initialized,
    find_free_port,
    resolve_storage,
    setup_distributed_env,
    setup_model_and_optimizer,
    stage_checkpoint_locally,
    upload_checkpoint,
)


@ray.remote(num_cpus=1)
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
        self.model, self.optimizer = setup_model_and_optimizer(self.params)
        self.fs, self.arrow_fs, self.base_path = resolve_storage(self.prefix)
        self.destination_ckpt = f"{self.base_path.rstrip('/')}/model.ckpt"

    def save_rounds(self):
        """Runs the save rounds and returns the per-round GCS upload windows."""
        try:
            durations = []
            for round_idx in range(self.params.rounds):
                local_dir = None
                try:
                    local_dir = stage_checkpoint_locally(
                        self.model,
                        self.optimizer,
                        self.params,
                        self.rank,
                        staging_prefix=f"ray-ckpt-r{round_idx}",
                    )
                    if local_dir is not None:
                        self.arrow_fs.create_dir(self.destination_ckpt)

                    # Timed GCS window: bounded by barriers so every rank reports
                    # the same interval, whether or not it owns data to upload.
                    dist.barrier()
                    t_start = time.perf_counter()
                    upload_checkpoint(
                        local_dir,
                        self.params,
                        self.rank,
                        self.arrow_fs,
                        self.fs,
                        self.destination_ckpt,
                    )
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
