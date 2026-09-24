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
