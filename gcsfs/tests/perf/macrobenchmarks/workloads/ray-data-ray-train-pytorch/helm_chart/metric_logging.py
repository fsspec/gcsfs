"""Atomic metric logging directly to the container's stdout."""

from __future__ import annotations

import logging
import os

CONTAINER_STDOUT = "/proc/1/fd/1"


class AtomicContainerHandler(logging.Handler):
    """Write each formatted record to the container pipe in one syscall."""

    def __init__(self, path=CONTAINER_STDOUT):
        super().__init__()
        self._fd = os.open(os.fspath(path), os.O_WRONLY | os.O_APPEND)
        self._pipe_buf = os.fpathconf(self._fd, "PC_PIPE_BUF")
        self.setFormatter(logging.Formatter("%(message)s"))

    def emit(self, record):
        encoded = (self.format(record) + "\n").encode("utf-8")
        if len(encoded) > self._pipe_buf:
            raise ValueError(
                f"atomic metric record is {len(encoded)} bytes; "
                f"PIPE_BUF is {self._pipe_buf}"
            )
        written = os.write(self._fd, encoded)
        if written != len(encoded):
            raise RuntimeError(
                f"atomic metric record was only partially written: "
                f"{written} of {len(encoded)} bytes"
            )

    def close(self):
        fd, self._fd = self._fd, -1
        if fd >= 0:
            os.close(fd)
        super().close()


def emit_metric(logger, *, event, **fields):
    """Emit a Ray observation using the Lightning benchmark's text formats."""
    if event == "step":
        logger.info(
            "Global Rank: %s | Step: %s | Loss: %.4f | "
            "Step Time: %.9fs | Throughput: %.6f samples/s",
            fields["global_rank"],
            fields["step"],
            fields["loss"],
            fields["duration_s"],
            fields["samples_per_second"],
        )
    elif event == "dataset_build":
        logger.info(
            "Dataset Build : Rank : %s : Duration : %.9f seconds : Path: %s",
            fields["global_rank"],
            fields["duration_s"],
            fields["dataset_path"],
        )
    elif event == "checkpoint_committed":
        logger.info(
            "Checkpoint Save : Rank: %s : Step: %s : Start time: %.6f "
            "seconds: Path: %s",
            fields["global_rank"],
            fields["checkpoint_step"],
            fields["start_time_s"],
            fields["checkpoint_location"],
        )
        logger.info(
            "Finished saving checkpoint to %s in %.9f seconds for "
            "global_step %s from rank %s",
            fields["checkpoint_location"],
            fields["duration_s"],
            fields["checkpoint_step"],
            fields["global_rank"],
        )
    elif event == "checkpoint_size":
        logger.info(
            "Checkpoint Size : Rank : 0 : Step : %s : Bytes : %s : Path: %s",
            fields["checkpoint_step"],
            fields["size_bytes"],
            fields["checkpoint_location"],
        )
    elif event == "checkpoint_restore_started":
        logger.info(
            "Checkpoint Restore Start : Rank : %s : Start time: %.6f "
            "seconds : Path: %s",
            fields["global_rank"],
            fields["time_s"],
            fields["checkpoint_location"],
        )
    elif event == "checkpoint_restore_completed":
        logger.info(
            "Finished restoring checkpoint : Rank : %s : Duration: %.9f "
            "seconds : End Time: %.6f seconds : Path: %s",
            fields["global_rank"],
            fields["duration_s"],
            fields["time_s"],
            fields["checkpoint_location"],
        )
    elif event == "checkpoint_deleted":
        if not fields["success"]:
            raise RuntimeError(
                f"Ray failed to delete checkpoint {fields['checkpoint_location']}"
            )
        logger.info(
            "Finished deleting checkpoint %s in %.9f seconds for "
            "global_step %s from rank 0",
            fields["checkpoint_location"],
            fields["duration_s"],
            fields["checkpoint_step"],
        )
    elif event == "ray_data_iteration":
        logger.info(
            "Ray Data Iteration : Rank : %s : Split : %s : Blocked : %.9f "
            "seconds : Total : %.9f seconds : First Batch : %.9f seconds : "
            "Blocked Calls : %s",
            fields["global_rank"],
            fields["split_index"],
            fields["total_blocked_s"],
            fields["total_s"],
            fields["time_to_first_batch_s"],
            fields["blocked_calls"],
        )
    else:
        raise ValueError(f"unknown metric event: {event!r}")
