"""Parameters for webdataset image read benchmark cases."""

from dataclasses import dataclass

from gcsfs.tests.perf.subsystembenchmarks.dataloading.configurator import ReadParameters
from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset import (
    gcsfs_opener,
    imagegen,
)

_GCS_MODE_TAGS = {"readahead_32mb": "ra32m", "whole_object": "whole"}


def _buffer_tag(read_buffer_bytes):
    """Formats read buffer size into an ID tag (e.g. buf32m or buf512k)."""
    if read_buffer_bytes % 2**20 == 0:
        return f"buf{read_buffer_bytes // 2**20}m"
    return f"buf{read_buffer_bytes // 2**10}k"


@dataclass
class WebDatasetReadParameters(ReadParameters):
    """Parameters for a WebDataset image read benchmark case."""

    LOADER_TAG = "wds"
    FMT_TAGS = {"image_tar": "imgtar", "image_tar_gz": "imgtgz"}

    pixel_budget: int = imagegen.DEFAULT_PIXEL_BUDGET
    image_encoding: str = "jpeg"
    jpeg_quality: int = 75
    sample_shape: str = "pairs"

    shard_shuffle: bool = True
    shuffle_buffer_size: int = 1000
    resampled: bool = False
    # Image decoding is disabled by default to isolate storage performance.
    decode: bool = False
    cache_dir_enabled: bool = False
    gcs_read_mode: str = "default"
    gcs_read_concurrency: int = gcsfs_opener.DEFAULT_READ_CONCURRENCY
    read_buffer_bytes: int = gcsfs_opener.DEFAULT_READ_BUFFER_BYTES

    @property
    def split_count(self):
        """Total shard consumer splits: world_size * max(num_workers, 1)."""
        return self.world_size * max(self.num_workers, 1)

    @property
    def shards_per_split(self):
        """Average shards allocated per split."""
        return max(1, self.file_count // self.split_count)

    @property
    def sample_shuffle_enabled(self):
        return self.access == "shuffled"

    @property
    def shard_shuffle_enabled(self):
        return self.access == "shuffled" and self.shard_shuffle

    @property
    def dataset_format(self):
        """Dataset container and encoding format (e.g. image_tar_jpeg)."""
        return f"{self.fmt}_{self.image_encoding}"

    @property
    def read_access_pattern(self):
        """Access pattern: 'sequential', 'shuffled', or 'shuffled_samples_only'."""
        if not self.sample_shuffle_enabled:
            return "sequential"
        return "shuffled" if self.shard_shuffle_enabled else "shuffled_samples_only"

    def ingest(self, prefix):
        return imagegen.ingest_tar_shards(
            prefix,
            fmt=self.fmt,
            file_count=self.file_count,
            rows_per_file=self.rows_per_file,
            pixel_budget=self.pixel_budget,
            image_encoding=self.image_encoding,
            jpeg_quality=self.jpeg_quality,
            sample_shape=self.sample_shape,
        )

    def _encoding_tag(self):
        if self.image_encoding == "jpeg":
            return f"jpg{self.jpeg_quality}"
        return self.image_encoding

    def _id_corpus_tokens(self):
        return [
            f"sh{self.file_count}x{self.rows_per_file}",
            f"px{self.pixel_budget // 1000}k",
            self._encoding_tag(),
        ]

    def _id_extra_tokens(self):
        """Benchmark ID tokens for non-default parameters."""
        tokens = []
        if self.sample_shape != "pairs":
            tokens.append("ilv")
        if self.file_count % self.split_count:
            tokens.append("unev")
        if self.gcs_read_mode != "default":
            tokens.append(_GCS_MODE_TAGS[self.gcs_read_mode])
        if self.gcs_read_concurrency != gcsfs_opener.DEFAULT_READ_CONCURRENCY:
            tokens.append(f"conc{self.gcs_read_concurrency}")
        if self.read_buffer_bytes:
            tokens.append(_buffer_tag(self.read_buffer_bytes))
        # Only tag disabled shard shuffle if sample shuffle is active.
        if self.sample_shuffle_enabled and not self.shard_shuffle:
            tokens.append("noshsh")
        if self.shuffle_buffer_size != 1000:
            tokens.append(f"sb{self.shuffle_buffer_size}")
        if self.resampled:
            tokens.append("resamp")
        if self.decode:
            tokens.append("dec")
        if self.cache_dir_enabled:
            tokens.append("cache")
        return tokens

    def extra_columns(self):
        return {
            "persistent_data_loader_workers_enabled": self.num_workers > 0,
            "image_max_pixels": self.pixel_budget,
            "image_jpeg_quality": (
                self.jpeg_quality if self.image_encoding == "jpeg" else 0
            ),
            "sample_layout": (
                "image_text_pair"
                if self.sample_shape == "pairs"
                else "interleaved_document"
            ),
            "gcs_read_mode": self.gcs_read_mode,
            "gcs_read_concurrency": self.gcs_read_concurrency,
            "gcs_read_buffer_bytes": self.read_buffer_bytes,
            "shuffle_buffer_size": (
                self.shuffle_buffer_size if self.sample_shuffle_enabled else 0
            ),
            "shard_resampling_enabled": self.resampled,
            "image_decode_enabled": self.decode,
            "local_shard_cache_enabled": self.cache_dir_enabled,
        }
