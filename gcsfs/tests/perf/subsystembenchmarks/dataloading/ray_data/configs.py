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
        if p.feature_column_count and p.fmt != "pretok_parquet":
            raise ValueError(
                f"feature columns are only generated for pretok_parquet, got "
                f"{p.fmt!r} for case {p.benchmark_name()!r}"
            )
        if not (
            p.feature_column_count >= 0
            and p.feature_value_bytes >= 1
            and 0 <= p.projected_feature_count <= p.feature_column_count
            and p.projection_run_length >= 1
        ):
            raise ValueError(
                "need feature_column_count >= 0, feature_value_bytes >= 1, "
                "0 <= projected_feature_count <= feature_column_count and "
                f"projection_run_length >= 1; got case {p.benchmark_name()!r}"
            )
