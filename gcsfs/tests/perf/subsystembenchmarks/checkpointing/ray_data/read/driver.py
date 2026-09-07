"""Driver for Ray checkpoint load benchmark."""

import contextlib
import dataclasses
import os
import time
import uuid

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

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import (
    CheckpointDriver,
    CheckpointResult,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_data.common import (
    ensure_ray_initialized,
    find_free_port,
    resolve_storage,
    save_checkpoint_step,
    setup_distributed_env,
    setup_model_and_optimizer,
)


@ray.remote
class RayCheckpointSetupWorker:
    """Ray Actor that creates the initial checkpoint on storage for load benchmarks."""

    def __init__(self, rank, world_size, port, prefix, params):
        self.rank = rank
        self.world_size = world_size
        self.port = port
        self.prefix = prefix
        self.params = params

    def setup_and_save(self):
        try:
            setup_distributed_env(self.rank, self.world_size, self.port)
            model, optimizer = setup_model_and_optimizer(self.params)
            fs, arrow_fs, base_path = resolve_storage(self.prefix)
            destination_ckpt = f"{base_path.rstrip('/')}/model.ckpt"
            save_checkpoint_step(
                model,
                optimizer,
                self.params,
                self.rank,
                arrow_fs,
                fs,
                destination_ckpt,
                staging_prefix="ray-setup-ckpt",
            )
        finally:
            dist.destroy_process_group()


@ray.remote
class RayCheckpointLoadWorker:
    """Ray Actor executing distributed checkpoint load operations on CPU."""

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

    def load_rounds(self):
        try:
            durations = []
            is_sharded = self.params.strategy in (
                "fsdp_sharded",
                "model_parallel_sharded",
            )
            for round_idx in range(self.params.rounds):
                dist.barrier()
                t_start = time.perf_counter()

                checkpoint = ray.train.Checkpoint(
                    path=self.destination_ckpt, filesystem=self.arrow_fs
                )
                # Ensure all workers on the host share the same UUID for this round
                # so Ray's built-in file locking deduplicates the download across workers
                # and cleans up the shared temporary directory after all workers exit.
                checkpoint._uuid = uuid.uuid5(
                    uuid.NAMESPACE_URL, f"{self.destination_ckpt}-round-{round_idx}"
                )

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
                            broadcast_from_rank0=True,
                        )
                        if self.rank == 0:
                            ckpt_file = os.path.join(local_dir, "checkpoint.pt")
                            state = torch.load(
                                ckpt_file, map_location="cpu", weights_only=False
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

                    dist.barrier()
                    t_end = time.perf_counter()
                    durations.append((t_start, t_end))

            return durations
        finally:
            dist.destroy_process_group()


def run_ray_load(prefix, params):
    """Runs single or distributed checkpoint load benchmark across Ray actor workers."""
    ensure_ray_initialized()
    world_size = params.world_size
    port = find_free_port()

    workers = [
        RayCheckpointLoadWorker.remote(rank, world_size, port, prefix, params)
        for rank in range(world_size)
    ]
    ray.get([w.setup.remote() for w in workers])
    results = ray.get([w.load_rounds.remote() for w in workers])

    durations = []
    for r in range(params.rounds):
        begins = [results[rank][r][0] for rank in range(world_size)]
        ends = [results[rank][r][1] for rank in range(world_size)]
        durations.append(max(ends) - min(begins))
    return durations


class RayCheckpointReadDriver(CheckpointDriver):
    """Driver for Ray checkpoint load benchmarks."""

    def setup(self, prefix: str, params):
        """Generates the source checkpoint on storage using setup topology."""
        ensure_ray_initialized()
        setup_world_size = params.setup_world_size
        setup_tp = params.setup_tensor_parallel_size
        setup_dp = params.setup_data_parallel_size

        setup_params = dataclasses.replace(
            params,
            world_size=setup_world_size,
            tensor_parallel_size=setup_tp,
            data_parallel_size=setup_dp,
        )
        port = find_free_port()
        try:
            workers = [
                RayCheckpointSetupWorker.remote(
                    rank, setup_world_size, port, prefix, setup_params
                )
                for rank in range(setup_world_size)
            ]
            ray.get([w.setup_and_save.remote() for w in workers])
        finally:
            if ray.is_initialized():
                ray.shutdown()

    def run(self, prefix: str, params) -> CheckpointResult:
        """Executes the checkpoint load benchmark across Ray actor workers."""
        try:
            durations = run_ray_load(prefix, params)
            return CheckpointResult(durations=durations)
        finally:
            if ray.is_initialized():
                ray.shutdown()
