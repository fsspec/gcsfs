"""Parameters for Ray Data read benchmark cases."""

from dataclasses import dataclass
from typing import Optional

from gcsfs.tests.perf.subsystembenchmarks.dataloading.configurator import ReadParameters

_MiB = 1 << 20


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

    # pretok_parquet feature columns: feature_column_count extra binary columns of
    # feature_value_bytes per value on average (0 keeps the plain tokens + label
    # corpus). The read selects tokens, label and projected_feature_count of them
    # (0 = all), picked in blocks of projection_run_length adjacent columns (1 =
    # scattered). Blocks can merge or be cut. Batches then stay pyarrow tables: binary columns have no tensor form.
    feature_column_count: int = 0
    feature_value_bytes: int = 10
    projected_feature_count: int = 0
    projection_run_length: int = 1

    # gcsfs open() options for every file Ray opens; None keeps gcsfs's defaults.
    cache_type: Optional[str] = None
    block_size: Optional[int] = None

    @property
    def read_access_pattern(self):
        """Access pattern: 'sequential', 'shuffled', or 'shuffled_samples_only'."""
        if self.access == "sequential":
            return "sequential"
        return "shuffled" if self.file_shuffle else "shuffled_samples_only"

    @property
    def has_features(self):
        return self.fmt == "pretok_parquet" and self.feature_column_count > 0

    def read_columns(self):
        """Columns the read selects."""
        if self.fmt == "text_parquet":
            return ["text", "label"]
        columns = ["tokens", "label"]
        if self.has_features:
            from gcsfs.tests.perf.subsystembenchmarks.dataloading import datagen

            names = datagen.feature_column_names(self.feature_column_count)
            indices = datagen.feature_projection(
                self.feature_column_count,
                self.projected_feature_count,
                self.projection_run_length,
            )
            columns += [names[i] for i in indices]
        return columns

    def ingest(self, prefix):
        from gcsfs.tests.perf.subsystembenchmarks.dataloading import datagen

        feature_options = {
            "feature_column_count": self.feature_column_count,
            "feature_value_bytes": self.feature_value_bytes,
        }
        return datagen.ingest_dataset(
            prefix,
            fmt=self.fmt,
            seq_len=self.seq_len,
            file_count=self.file_count,
            rows_per_file=self.rows_per_file,
            row_group_size=self.row_group_size,
            fmt_options=feature_options if self.has_features else None,
        )

    def _id_corpus_tokens(self):
        tokens = [
            f"rg{self.row_group_size}",
            f"fc{self.file_count}x{self.rows_per_file}",
        ]
        if self.has_features:
            tokens.append(f"feat{self.feature_column_count}v{self.feature_value_bytes}")
        return tokens

    def _id_extra_tokens(self):
        tokens = []
        if self.access == "shuffled":
            # File shuffle is default on; only tag if explicitly disabled.
            if not self.file_shuffle:
                tokens.append("nofshuf")
            if self.shuffle_buffer_size > 0:
                tokens.append(f"buf{self.shuffle_buffer_size}")
        if self.has_features and self.projected_feature_count:
            tokens.append(
                f"proj{self.projected_feature_count}r{self.projection_run_length}"
            )
        if self.cache_type is not None:
            tokens.append(f"ct{self.cache_type}")
        if self.block_size is not None:
            size = self.block_size
            tokens.append(f"bs{size // _MiB}m" if size % _MiB == 0 else f"bs{size}")
        return tokens

    def extra_columns(self):
        features = self.has_features
        return {
            "shuffle_buffer_size": (
                self.shuffle_buffer_size if self.access == "shuffled" else 0
            ),
            "parquet_row_group_size_rows": self.row_group_size,
            "sample_sequence_length_tokens": self.seq_len,
            "parquet_feature_column_count": self.feature_column_count or None,
            "read_projected_feature_column_count": (
                (self.projected_feature_count or self.feature_column_count)
                if features
                else None
            ),
            "read_projection_run_length_columns": (
                self.projection_run_length
                if features and self.projected_feature_count
                else None
            ),
            "gcsfs_read_cache_type": self.cache_type,
            "gcsfs_read_block_size_bytes": self.block_size,
        }
