"""One-factor configurator for the self-contained HuggingFace read benchmark."""

from gcsfs.tests.perf.subsystembenchmarks.dataloading.configurator import (
    OneFactorReadConfigurator,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.huggingface_datasets.parameters import (
    HFReadParameters,
)


class HuggingFaceReadConfigurator(OneFactorReadConfigurator):
    FRAMEWORK = "huggingface_datasets"
    PARAMS_CLASS = HFReadParameters
