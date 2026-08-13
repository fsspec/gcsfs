"""Parameters for HuggingFace datasets read benchmark cases."""

from dataclasses import dataclass

from gcsfs.tests.perf.subsystembenchmarks.dataloading.configurator import ReadParameters


@dataclass
class HFReadParameters(ReadParameters):
    """Read parameters for streaming HuggingFace datasets benchmark cases."""

    LOADER_TAG = "hf"
    FMT_TAGS = {
        "pretok_parquet": "ptpq",
        "text_parquet": "txpq",
        "pretok_jsonl": "ptjsonl",
    }

    # Text-corpus shape parameters.
    seq_len: int = 2048
    row_group_size: int = 1024  # parquet only

    # Shuffle-only configuration for streaming datasets.
    shuffle_buffer_size: int = 1000
    max_buffer_input_shards: int = 0

    def ingest(self, prefix):
        from gcsfs.tests.perf.subsystembenchmarks.dataloading import datagen

        return datagen.ingest_dataset(
            prefix,
            fmt=self.fmt,
            seq_len=self.seq_len,
            file_count=self.file_count,
            rows_per_file=self.rows_per_file,
            row_group_size=self.row_group_size,
        )

    def _id_corpus_tokens(self):
        return [
            f"rg{self.row_group_size}",
            f"fc{self.file_count}x{self.rows_per_file}",
        ]

    def _id_extra_tokens(self):
        if self.access == "shuffled" and self.max_buffer_input_shards:
            return [f"mbis{self.max_buffer_input_shards}"]
        return []

    def extra_columns(self):
        return {
            "persistent_data_loader_workers_enabled": self.num_workers > 0,
            "shuffle_max_buffer_input_shards": self.max_buffer_input_shards,
            # 0 represents no shuffling for sequential access.
            "shuffle_buffer_size": (
                self.shuffle_buffer_size if self.access == "shuffled" else 0
            ),
            "sample_sequence_length_tokens": self.seq_len,
            "parquet_row_group_size_rows": self.row_group_size,
        }
