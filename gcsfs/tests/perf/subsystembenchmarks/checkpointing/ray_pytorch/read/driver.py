"""Driver for Ray checkpoint load benchmark."""

import contextlib
import os
import shutil
import time

import ray
import ray.train
import torch
import torch.distributed as dist
import torch.distributed.checkpoint as dcp
from torch.distributed.checkpoint.state_dict import (
    StateDictOptions,
    get_state_dict,
    set_model_state_dict,
    set_optimizer_state_dict,
    set_state_dict,
)

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator
from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
    setup_distributed_env,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import (
    CheckpointDriver,
    CheckpointResult,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.model_staging import (
    stage_gcs_model,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_pytorch.common import (
    reduce_ray_durations,
    resolve_storage,
    run_ray_workers,
    run_subprocess_worker,
    setup_model_and_optimizer,
    stage_checkpoint_locally,
    upload_checkpoint,
)


@ray.remote(num_cpus=1)
class RayCheckpointSetupWorker:
    """Ray Actor that creates the initial checkpoint on storage for load benchmarks."""

    def __init__(self, rank, world_size, port, prefix, params):
        self.rank = rank
        self.world_size = world_size
        self.port = port
        self.prefix = prefix
        self.params = params

    def setup_and_save(self):
        local_dir = None
        try:
            device = setup_distributed_env(self.rank, self.world_size, self.port)
            model, optimizer = setup_model_and_optimizer(self.params, device)
            fs, arrow_fs, base_path = resolve_storage(self.prefix)
            destination_ckpt = f"{base_path.rstrip('/')}/model.ckpt"

            local_dir = stage_checkpoint_locally(
                model,
                optimizer,
                self.params,
                self.rank,
                staging_prefix="ray-setup-ckpt",
            )
            if local_dir is not None:
                arrow_fs.create_dir(destination_ckpt)
            upload_checkpoint(
                local_dir,
                self.params,
                self.rank,
                arrow_fs,
                fs,
                destination_ckpt,
            )
        finally:
            if local_dir and os.path.exists(local_dir):
                shutil.rmtree(local_dir, ignore_errors=True)
            dist.destroy_process_group()


@ray.remote(num_cpus=1)
class RayCheckpointLoadWorker:
    """Ray Actor executing distributed checkpoint load operations."""

    def __init__(self, rank, world_size, port, prefix, params):
        self.rank = rank
        self.world_size = world_size
        self.port = port
        self.prefix = prefix
        self.params = params

    def setup(self):
        self.device = setup_distributed_env(self.rank, self.world_size, self.port)
        self.model, self.optimizer = setup_model_and_optimizer(self.params, self.device)
        self.fs, self.arrow_fs, self.base_path = resolve_storage(self.prefix)
        self.destination_ckpt = f"{self.base_path.rstrip('/')}/model.ckpt"

    def load_rounds(self):
        try:
            durations = []
            is_sharded = self.params.strategy in (
                "fsdp_sharded",
                "model_parallel_sharded",
            )
            for round_idx in range(self.params.rounds):
                # Broadcast the Checkpoint instance from rank 0 so all workers on the host
                # share the same Checkpoint and UUID. Ray's as_directory() uses this UUID to
                # coordinate file-locking and deduplicate the download across workers on the
                # same host, and clean up the directory after all workers exit.
                ckpt_list = [
                    (
                        ray.train.Checkpoint(
                            path=self.destination_ckpt, filesystem=self.arrow_fs
                        )
                        if self.rank == 0
                        else None
                    )
                ]
                dist.broadcast_object_list(ckpt_list, src=0)
                checkpoint = ckpt_list[0]

                dist.barrier()
                t_start = time.perf_counter()

                directory_context = (
                    checkpoint.as_directory()
                    if is_sharded or self.rank == 0
                    else contextlib.nullcontext(None)
                )
                with directory_context as local_dir:
                    if is_sharded:
                        options = StateDictOptions(
                            full_state_dict=False,
                            cpu_offload=False,
                        )
                        model_state, opt_state = get_state_dict(
                            self.model, self.optimizer, options=options
                        )
                        app_state = {"model": model_state, "optimizer": opt_state}
                        dcp.load(
                            {"app": app_state},
                            storage_reader=dcp.FileSystemReader(local_dir),
                        )
                        set_state_dict(
                            self.model,
                            self.optimizer,
                            model_state_dict=app_state["model"],
                            optim_state_dict=app_state["optimizer"],
                            options=options,
                        )
                        del app_state, model_state, opt_state
                    else:
                        options = StateDictOptions(
                            full_state_dict=True,
                            cpu_offload=False,
                            broadcast_from_rank0=self.world_size > 1,
                        )
                        if self.rank == 0:
                            ckpt_file = os.path.join(local_dir, "checkpoint.pt")
                            state = torch.load(
                                ckpt_file, map_location="cpu", weights_only=True
                            )
                            model_state = state["model"]
                            opt_state = state["optimizer"]
                        else:
                            model_state = {}
                            opt_state = {}

                        set_model_state_dict(
                            self.model,
                            model_state,
                            options=options,
                        )
                        set_optimizer_state_dict(
                            self.model,
                            self.optimizer,
                            opt_state,
                            options=options,
                        )
                        if self.rank == 0:
                            del state, model_state, opt_state

                    accelerator.synchronize(self.device)
                    dist.barrier()
                    t_end = time.perf_counter()
                    durations.append((t_start, t_end))

            return durations
        finally:
            dist.destroy_process_group()


def run_ray_load(prefix, params):
    """Runs single or distributed checkpoint load benchmark across Ray actor workers."""
    world_size = params.world_size
    results = run_ray_workers(
        RayCheckpointLoadWorker,
        world_size,
        prefix,
        params,
        setup_method="setup",
        run_method="load_rounds",
    )
    return reduce_ray_durations(results, world_size, params.rounds)


class RayCheckpointReadDriver(CheckpointDriver):
    """Driver for Ray checkpoint load benchmarks."""

    def setup(self, prefix: str, params):
        """Generates the source checkpoint on storage using setup topology."""
        stage_gcs_model(getattr(params, "model_id", ""))
        run_subprocess_worker("setup_load", prefix, params)

    def run(self, prefix: str, params) -> CheckpointResult:
        """Executes the checkpoint load benchmark across Ray actor workers."""
        stage_gcs_model(getattr(params, "model_id", ""))
        data = run_subprocess_worker("load", prefix, params, expect_result=True)
        return CheckpointResult(durations=data["durations"])

    def read_count(self, params) -> int:
        """Ray transfers a single copy of the checkpoint from storage per node."""
        return 1
