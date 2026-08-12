"""One-factor configurator for the webdataset image read benchmark."""

from gcsfs.tests.perf.subsystembenchmarks.dataloading.configurator import (
    ACCESS_PATTERNS,
    OneFactorReadConfigurator,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset import (
    gcsfs_opener,
    imagegen,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset.parameters import (
    WebDatasetReadParameters,
)

_ENUMS = (
    ("fmt", imagegen.FORMATS),
    ("image_encoding", imagegen.ENCODINGS),
    ("sample_shape", imagegen.SAMPLE_SHAPES),
    ("access", ACCESS_PATTERNS),
    ("gcs_read_mode", gcsfs_opener.READ_MODES),
)

# smart_resize floors dimensions at FACTOR; a smaller budget cannot be honored.
_MINIMUMS = (
    ("file_count", 1),
    ("rows_per_file", 1),
    ("pixel_budget", imagegen.FACTOR**2),
    ("jpeg_quality", 1),
    ("batch_size", 1),
    ("world_size", 1),
    ("num_workers", 0),
    ("prefetch_factor", 1),
    ("shuffle_buffer_size", 1),
    ("gcs_read_concurrency", 1),
    ("read_buffer_bytes", 0),
)

_MAXIMUMS = (("jpeg_quality", 95),)


def _describe(p):
    """Describe a case even if its benchmark name cannot be constructed."""
    try:
        return repr(p.benchmark_name())
    except Exception:
        return f"case on sweep_axis {p.sweep_axis!r}"


class WebDatasetReadConfigurator(OneFactorReadConfigurator):
    FRAMEWORK = "webdataset"
    PARAMS_CLASS = WebDatasetReadParameters

    def validate_case(self, p):
        super().validate_case(p)
        for field, allowed in _ENUMS:
            value = getattr(p, field)
            if value not in allowed:
                raise ValueError(
                    f"{field} must be one of {allowed}, got {value!r} "
                    f"for {_describe(p)}"
                )
        for field, minimum in _MINIMUMS:
            value = getattr(p, field)
            if value < minimum:
                raise ValueError(
                    f"{field} must be >= {minimum}, got {value!r} for {_describe(p)}"
                )
        for field, maximum in _MAXIMUMS:
            value = getattr(p, field)
            if value > maximum:
                raise ValueError(
                    f"{field} must be <= {maximum}, got {value!r} for {_describe(p)}"
                )
        if p.file_count < p.split_count:
            # Every rank/worker split must receive at least one shard.
            raise ValueError(
                f"file_count ({p.file_count}) must be >= world_size x "
                f"max(num_workers, 1) ({p.split_count}) or some splits get no "
                f"shard at all; {_describe(p)}"
            )
        if p.read_buffer_bytes and p.gcs_read_mode == "whole_object":
            raise ValueError(
                "whole_object reads are already materialized, so read_buffer_bytes "
                f"would measure nothing; {_describe(p)}"
            )
