"""Shared read-case parameters base and one-factor-at-a-time configurator.

`ReadParameters` subclasses define per-loader parameters and unique benchmark IDs.
`OneFactorReadConfigurator` expands baseline and single-factor YAML variants.
"""

import dataclasses
import os

from gcsfs.tests.perf.subsystembenchmarks._common.config_loader import (
    OneFactorConfigurator,
)

_FMT = {"pretok_parquet": "ptpq", "text_parquet": "txpq", "pretok_jsonl": "ptjsonl"}
_BUCKET = {"regional": "reg", "zonal": "zon", "hns": "hns"}

# Run-level environment keys; YAML variants must not override them.
_RUN_LEVEL_KEYS = ("bucket_type",)


@dataclasses.dataclass
class ReadParameters:
    """One self-contained streaming read case (fresh per-case bucket ingested at runtime)."""

    LOADER_TAG = "base"

    name: str
    bucket_name: str
    bucket_type: str
    rounds: int
    scenario: str
    framework: str

    fmt: str
    seq_len: int
    file_count: int
    rows_per_file: int
    row_group_size: int  # parquet only

    access: str
    num_workers: int
    batch_size: int
    prefetch_factor: int = 2
    split_by_node: bool = False
    world_size: int = 1
    # YAML axis that produced this case ("baseline" or factor name).
    sweep_axis: str = "baseline"

    def _id_extra_tokens(self):
        """Loader-specific id tokens inserted between split and bucket tokens."""
        return []

    def extra_columns(self):
        """Loader-specific CSV columns (merged onto shared columns by runner)."""
        return {}

    def benchmark_name(self):
        """Stable, param-encoding pytest-benchmark id using swept values."""
        acc = "shuf" if self.access == "shuffled" else "seq"
        parts = [
            f"read-{self.LOADER_TAG}",
            _FMT[self.fmt],
            acc,
            f"nw{self.num_workers}",
            f"rg{self.row_group_size}",
            f"fc{self.file_count}x{self.rows_per_file}",
        ]
        if self.split_by_node:
            div = "div" if self.file_count % self.world_size == 0 else "indiv"
            parts.append(f"splitws{self.world_size}{div}")
        parts += self._id_extra_tokens()
        parts.append(_BUCKET[self.bucket_type])
        if self.prefetch_factor != 2:
            parts.append(f"pf{self.prefetch_factor}")
        return "-".join(parts)


class OneFactorReadConfigurator(OneFactorConfigurator):
    """Read-family pins for generic one-factor config mechanics."""

    RUN_LEVEL_KEYS = _RUN_LEVEL_KEYS

    def shared_keys(self, scenario, common_config):
        return dict(
            rounds=common_config.get("rounds", 3),
            scenario=scenario["scenario"],
            seq_len=common_config.get("seq_len", 2048),
            batch_size=common_config.get("batch_size", 64),
            bucket_type=os.environ.get("GCSFS_SUBSYSTEM_BUCKET_TYPE", "regional"),
        )

    def validate_case(self, p):
        if p.rounds < 1:
            # Prevent drivers from failing on empty round loops (rounds must be >= 1).
            raise ValueError(
                f"rounds must be >= 1, got {p.rounds!r} for case {p.benchmark_name()!r}"
            )
