"""Parameters for HuggingFace datasets read benchmark cases."""

from dataclasses import dataclass

from gcsfs.tests.perf.subsystembenchmarks.dataloading.configurator import ReadParameters


@dataclass
class HFReadParameters(ReadParameters):
    """Read parameters for streaming HuggingFace datasets benchmark cases."""

    LOADER_TAG = "hf"

    # Shuffle-only configuration for streaming datasets.
    shuffle_buffer_size: int = 1000
    max_buffer_input_shards: int = 0

    def _id_extra_tokens(self):
        if self.access == "shuffled" and self.max_buffer_input_shards:
            return [f"mbis{self.max_buffer_input_shards}"]
        return []

    def extra_columns(self):
        return {
            "persistent_workers": self.num_workers > 0,
            "max_buffer_input_shards": self.max_buffer_input_shards,
        }
