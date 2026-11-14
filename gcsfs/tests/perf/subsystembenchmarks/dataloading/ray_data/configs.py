"""One-factor configurator for the self-contained Ray Data read benchmark."""

from gcsfs.tests.perf.subsystembenchmarks.dataloading.configurator import (
    OneFactorReadConfigurator,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.parameters import (
    RayDataReadParameters,
)


class RayDataReadConfigurator(OneFactorReadConfigurator):
    FRAMEWORK = "ray_data"
    PARAMS_CLASS = RayDataReadParameters

    def shared_keys(self, scenario, common_config):
        keys = super().shared_keys(scenario, common_config)
        # seq_len sizes the text corpus for this benchmark group.
        keys["seq_len"] = common_config.get("seq_len", 2048)
        return keys

    def validate_case(self, p):
        super().validate_case(p)
        checks = [
            (
                p.feature_column_count and p.fmt != "pretok_parquet",
                "feature columns are only generated for pretok_parquet",
            ),
            (
                p.feature_column_count < 0
                or p.feature_value_bytes < 1
                or p.projected_feature_count < 0,
                "need feature_column_count >= 0, feature_value_bytes >= 1 and "
                "projected_feature_count >= 0",
            ),
            (
                p.projected_feature_count
                and p.projected_feature_count >= p.feature_column_count,
                "projected_feature_count must be < feature_column_count (use 0 to read "
                "every feature column)",
            ),
            (
                p.projection_run_length != 1 and not p.projects_subset,
                "projection_run_length needs 0 < projected_feature_count < "
                "feature_column_count",
            ),
            (
                not 1 <= p.projection_run_length <= max(p.projected_feature_count, 1),
                "projection_run_length must be between 1 and projected_feature_count",
            ),
        ]
        for failed, message in checks:
            if failed:
                raise ValueError(f"{message}, got case {p.benchmark_name()!r}")
