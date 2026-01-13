import unittest.mock as mock

import pytest

from gcsfs.tests.perf.microbenchmarks import configs
from gcsfs.tests.perf.microbenchmarks.delete.configs import get_delete_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.listing.configs import (
    ListingConfigurator,
    get_listing_benchmark_cases,
)
from gcsfs.tests.perf.microbenchmarks.read.configs import (
    ReadConfigurator,
    get_read_benchmark_cases,
)
from gcsfs.tests.perf.microbenchmarks.rename.configs import get_rename_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.write.configs import (
    WriteConfigurator,
    get_write_benchmark_cases,
)
from gcsfs.tests.settings import BENCHMARK_SKIP_TESTS

pytestmark = pytest.mark.skipif(
    BENCHMARK_SKIP_TESTS,
    reason="""Skipping benchmark tests.
Set GCSFS_BENCHMARK_SKIP_TESTS=false to run them,
or use the orchestrator script at gcsfs/tests/perf/microbenchmarks/run.py""",
)

MB = 1024 * 1024


@pytest.fixture
def mock_config_dependencies():
    """Mocks external dependencies for configurator tests."""
    with (
        mock.patch(
            "gcsfs.tests.perf.microbenchmarks.configs.BUCKET_NAME_MAP",
            {"regional": "test-bucket"},
        ),
        mock.patch("gcsfs.tests.perf.microbenchmarks.configs.BENCHMARK_FILTER", ""),
    ):
        yield


def test_load_config_filtering(mock_config_dependencies):
    """Test that _load_config correctly filters scenarios based on BENCHMARK_FILTER."""
    config_content = {
        "common": {},
        "scenarios": [{"name": "run_me"}, {"name": "skip_me"}],
    }

    # Test with filter enabled
    with (
        mock.patch(
            "gcsfs.tests.perf.microbenchmarks.configs.BENCHMARK_FILTER", "run_me"
        ),
        mock.patch("builtins.open", mock.mock_open(read_data="")),
        mock.patch("yaml.safe_load", return_value=config_content),
    ):

        configurator = configs.BaseBenchmarkConfigurator("dummy")
        _, scenarios = configurator._load_config()
        assert len(scenarios) == 1
        assert scenarios[0]["name"] == "run_me"

    # Test without filter (should return all)
    with (
        mock.patch("gcsfs.tests.perf.microbenchmarks.configs.BENCHMARK_FILTER", ""),
        mock.patch("builtins.open", mock.mock_open(read_data="")),
        mock.patch("yaml.safe_load", return_value=config_content),
    ):

        configurator = configs.BaseBenchmarkConfigurator("dummy")
        _, scenarios = configurator._load_config()
        assert len(scenarios) == 2


def test_read_configurator(mock_config_dependencies):
    """Test that ReadConfigurator correctly builds benchmark parameters."""
    common = {
        "bucket_types": ["regional"],
        "file_sizes_mb": [1],
        "block_sizes_mb": [16],
        "rounds": 1,
    }
    scenario = {"name": "read_test", "processes": [1], "threads": [1], "pattern": "seq"}

    configurator = ReadConfigurator("dummy")
    cases = configurator.build_cases(scenario, common)

    assert len(cases) == 1
    case = cases[0]
    assert case.name == "read_test_1procs_1threads_1MB_file_16MB_block_regional"
    assert case.file_size_bytes == 1 * MB
    assert case.block_size_bytes == 16 * MB
    assert case.chunk_size_bytes == 16 * MB
    assert case.pattern == "seq"
    assert case.bucket_name == "test-bucket"


def test_write_configurator(mock_config_dependencies):
    """Test that WriteConfigurator correctly builds benchmark parameters."""
    common = {
        "bucket_types": ["regional"],
        "file_sizes_mb": [10],
        "chunk_sizes_mb": [5],
        "rounds": 1,
    }
    scenario = {"name": "write_test", "processes": [2], "threads": [1]}

    configurator = WriteConfigurator("dummy")
    cases = configurator.build_cases(scenario, common)

    assert len(cases) == 1
    case = cases[0]
    assert case.name == "write_test_2procs_1threads_10MB_file_5MB_chunk_regional"
    assert case.file_size_bytes == 10 * MB
    assert case.chunk_size_bytes == 5 * MB
    assert case.processes == 2
    assert case.files == 2  # threads * processes


def test_listing_configurator(mock_config_dependencies):
    """Test that ListingConfigurator correctly builds benchmark parameters."""
    common = {"bucket_types": ["regional"], "files": [100], "rounds": 1}
    scenario = {
        "name": "list_test",
        "processes": [1],
        "threads": [1],
        "depth": 2,
        "folders": [5],
        "pattern": "prefix",
    }

    configurator = ListingConfigurator("dummy")
    cases = configurator.build_cases(scenario, common)

    assert len(cases) == 1
    case = cases[0]
    assert (
        case.name
        == "list_test_1procs_1threads_100files_2depth_5folders_prefix_regional"
    )
    assert case.files == 100
    assert case.depth == 2
    assert case.folders == 5
    assert case.pattern == "prefix"


def test_generate_cases_calls_load(mock_config_dependencies):
    """Test that generate_cases integrates _load_config and build_cases."""
    config_content = {
        "common": {
            "bucket_types": ["regional"],
            "file_sizes_mb": [1],
            "chunk_sizes_mb": [1],
        },
        "scenarios": [{"name": "test", "processes": [1], "threads": [1]}],
    }

    with (
        mock.patch("builtins.open", mock.mock_open(read_data="")),
        mock.patch("yaml.safe_load", return_value=config_content),
    ):

        configurator = WriteConfigurator("dummy")
        cases = configurator.generate_cases()
        assert len(cases) == 1
        assert cases[0].name.startswith("test")


def test_validate_actual_yaml_configs():
    """
    Loads the actual configs.yaml files for each benchmark type and verifies
    that they produce valid benchmark cases. This ensures the YAML files are
    valid and the logic works with the real configuration.
    """
    # Ensure BENCHMARK_FILTER is empty so we load all cases
    with mock.patch("gcsfs.tests.perf.microbenchmarks.configs.BENCHMARK_FILTER", ""):
        # Read
        cases = get_read_benchmark_cases()
        assert len(cases) > 0, "Read config produced no cases"

        # Write
        cases = get_write_benchmark_cases()
        assert len(cases) > 0, "Write config produced no cases"

        # Listing
        cases = get_listing_benchmark_cases()
        assert len(cases) > 0, "Listing config produced no cases"

        # Delete
        cases = get_delete_benchmark_cases()
        assert len(cases) > 0, "Delete config produced no cases"

        # Rename
        cases = get_rename_benchmark_cases()
        assert len(cases) > 0, "Rename config produced no cases"
