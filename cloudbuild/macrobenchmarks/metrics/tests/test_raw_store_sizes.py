from dataclasses import dataclass, field
from typing import List

from metrics import raw_store, schema


@dataclass
class _FakeParsed:
    step_metrics: List = field(default_factory=list)
    write_metrics: dict = field(default_factory=dict)
    restore_metrics: dict = field(default_factory=dict)
    delete_metrics: dict = field(default_factory=dict)
    data_loading_metrics: List = field(default_factory=list)
    checkpoint_sizes: List = field(default_factory=list)


def test_checkpoint_sizes_roundtrip(tmp_path):
    parsed = _FakeParsed(
        checkpoint_sizes=[
            schema.CheckpointSizeMetrics(
                checkpoint_step=25,
                checkpoint_location="gs://b/ckpt/r/llama-00-25.ckpt",
                size_bytes=1000,
                global_rank=0,
            )
        ],
    )
    raw_store.write_raw_metrics(parsed, str(tmp_path))
    tables = raw_store.read_raw_metrics(str(tmp_path))
    assert tables.size_rows == [
        {
            "checkpoint_step": 25,
            "checkpoint_location": "gs://b/ckpt/r/llama-00-25.ckpt",
            "size_bytes": 1000,
            "global_rank": 0,
        }
    ]


def test_absent_sizes_read_as_empty(tmp_path):
    tables = raw_store.read_raw_metrics(str(tmp_path))
    assert tables.size_rows == []
