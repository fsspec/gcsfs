"""Parameters for Ray Data read benchmark cases."""

from dataclasses import dataclass

from gcsfs.tests.perf.subsystembenchmarks.dataloading.configurator import ReadParameters


@dataclass
class RayDataReadParameters(ReadParameters):
    """Read parameters for streaming Ray Data benchmark cases."""

    LOADER_TAG = "ray"
    FMT_TAGS = {
        "pretok_parquet": "ptpq",
        "text_parquet": "txpq",
    }

    # Text-corpus shape parameters.
    seq_len: int = 2048
    row_group_size: int = 1800

    # Ray Data read and shuffle parameters.
    file_shuffle: bool = True
    shuffle_buffer_size: int = 0

    @property
    def read_access_pattern(self):
        """Access pattern: 'sequential', 'shuffled', or 'shuffled_samples_only'."""
        if self.access == "sequential":
            return "sequential"
        return "shuffled" if self.file_shuffle else "shuffled_samples_only"

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
        tokens = []
        if self.access == "shuffled":
            # File shuffle is default on; only tag if explicitly disabled.
            if not self.file_shuffle:
                tokens.append("nofshuf")
            if self.shuffle_buffer_size > 0:
                tokens.append(f"buf{self.shuffle_buffer_size}")
        return tokens

    def extra_columns(self):
        return {
            "shuffle_buffer_size": (
                self.shuffle_buffer_size if self.access == "shuffled" else 0
            ),
            "parquet_row_group_size_rows": self.row_group_size,
            "sample_sequence_length_tokens": self.seq_len,
        }
