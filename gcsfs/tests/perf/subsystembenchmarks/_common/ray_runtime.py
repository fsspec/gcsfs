"""Shared local Ray cluster lifecycle and TPU slice env management."""

from __future__ import annotations

import logging
import os

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator

_RAY_TPU_EXTRA_ENV_VARS = ("RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS",)
_SAVED_TPU_LAUNCH_ENV: dict[str, str | None] | None = None
_ACTIVE_TPU_WORLD_SIZE: int | None = None


def ensure_ray_initialized(
    world_size: int | None = None,
    *,
    check_capacity: bool = False,
    restart_on_tpu_slice_change: bool = False,
) -> None:
    """Initialize a local Ray cluster if not already running.

    On TPU, snapshots and configures TorchTPU slice launch env vars before
    ``ray.init()`` so spawned Ray workers inherit the slice layout.
    """
    global _ACTIVE_TPU_WORLD_SIZE, _SAVED_TPU_LAUNCH_ENV
    import ray

    if check_capacity and world_size is not None and accelerator.device_kind() != "cpu":
        count = accelerator.local_device_count()
        if world_size > count:
            raise ValueError(
                f"world_size {world_size} exceeds {count} "
                f"{accelerator.accelerator_type()} devices"
            )

    needs_tpu_launch = world_size is not None and accelerator.device_kind() == "tpu"
    if (
        restart_on_tpu_slice_change
        and needs_tpu_launch
        and ray.is_initialized()
        and _ACTIVE_TPU_WORLD_SIZE != world_size
    ):
        shutdown_ray()

    if not ray.is_initialized():
        previous_env: dict[str, str | None] | None = None
        if needs_tpu_launch:
            previous_env = accelerator.capture_launch_env(_RAY_TPU_EXTRA_ENV_VARS)
        try:
            if needs_tpu_launch:
                os.environ.setdefault("RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS", "1")
                accelerator.prepare_launch(world_size)
            ray.init(
                # Always start a local cluster; never attach to $RAY_ADDRESS.
                address="local",
                include_dashboard=False,
                ignore_reinit_error=True,
                logging_level=logging.WARNING,
            )
        except BaseException:
            if previous_env is not None:
                accelerator.restore_launch_env(previous_env)
            raise
        if previous_env is not None:
            if _SAVED_TPU_LAUNCH_ENV is None:
                _SAVED_TPU_LAUNCH_ENV = previous_env
            _ACTIVE_TPU_WORLD_SIZE = world_size


def kill_actors(actors) -> None:
    """Explicitly terminate Ray actors before cluster shutdown."""
    if not actors:
        return
    import ray

    for actor in actors:
        try:
            ray.kill(actor, no_restart=True)
        except Exception:
            pass


def shutdown_ray() -> None:
    """Shut down Ray if initialized and restore any saved TPU launch env vars."""
    global _ACTIVE_TPU_WORLD_SIZE, _SAVED_TPU_LAUNCH_ENV
    import ray

    try:
        if ray.is_initialized():
            ray.shutdown()
    finally:
        _ACTIVE_TPU_WORLD_SIZE = None
        if _SAVED_TPU_LAUNCH_ENV is not None:
            accelerator.restore_launch_env(_SAVED_TPU_LAUNCH_ENV)
            _SAVED_TPU_LAUNCH_ENV = None
