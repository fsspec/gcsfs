import unittest.mock as mock

import pytest

from gcsfs.tests.perf.microbenchmarks import configs
from gcsfs.tests.perf.microbenchmarks.cat_ranges.configs import (
    CatRangesConfigurator,
    get_cat_ranges_benchmark_cases,
)
from gcsfs.tests.perf.microbenchmarks.comparison.configs import (
    ComparisonConfigurator,
    get_comparison_benchmark_cases,
)
from gcsfs.tests.perf.microbenchmarks.delete.configs import get_delete_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.glob.configs import get_glob_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.info.configs import (
    InfoConfigurator,
    get_info_benchmark_cases,
)
from gcsfs.tests.perf.microbenchmarks.listing.configs import (
    ListingConfigurator,
    get_listing_benchmark_cases,
)
from gcsfs.tests.perf.microbenchmarks.open.configs import get_open_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.put.configs import (
    PutConfigurator,
    get_put_benchmark_cases,
)
from gcsfs.tests.perf.microbenchmarks.read.configs import (
    ReadConfigurator,
    get_read_benchmark_cases,
)
from gcsfs.tests.perf.microbenchmarks.rename.configs import get_rename_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.runner import filter_test_cases
from gcsfs.tests.perf.microbenchmarks.write.configs import (
    WriteConfigurator,
    get_write_benchmark_cases,
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
        "chunk_sizes_mb": [16],
        "rounds": 1,
    }
    scenario = {
        "name": "read_test",
        "processes": [1],
        "threads": [1],
        "pattern": "seq",
        "block_sizes_mb": [16],
    }

    configurator = ReadConfigurator("dummy")
    cases = configurator.build_cases(scenario, common)

    assert len(cases) == 1
    case = cases[0]
    assert (
        case.name
        == "read_test_mrd_pool_cache_16_mrd_pool_None_1procs_1threads_1MB_file_16MB_chunk_16MB_block_regional"
    )
    assert case.file_size_bytes == 1 * MB
    assert case.block_size_bytes == 16 * MB
    assert case.chunk_size_bytes == 16 * MB
    assert case.pattern == "seq"
    assert case.bucket_name == "test-bucket"


def test_read_fixed_duration_multi_thread_config(mock_config_dependencies):
    """Test that read fixed-duration multi-thread cases are generated and classified."""
    with mock.patch("gcsfs.tests.perf.microbenchmarks.configs.BENCHMARK_FILTER", ""):
        cases = get_read_benchmark_cases()

    _, multi_thread_cases, _ = filter_test_cases(cases)
    multi_thread_cases = [
        case
        for case in multi_thread_cases
        if case.name.startswith("read_seq_fixed_duration_multi_thread")
        or case.name.startswith("read_rand_fixed_duration_multi_thread")
    ]

    names = {case.name for case in multi_thread_cases}
    assert any(
        name.startswith("read_seq_fixed_duration_multi_thread") for name in names
    )
    assert any(
        name.startswith("read_rand_fixed_duration_multi_thread") for name in names
    )
    assert {case.threads for case in multi_thread_cases} == {32}
    assert {case.files for case in multi_thread_cases} == {1}


def test_write_configurator(mock_config_dependencies):
    """Test that WriteConfigurator correctly builds benchmark parameters."""
    common = {
        "bucket_types": ["regional"],
        "chunk_sizes_mb": [10],
        "block_sizes_mb": [32],
        "rounds": 1,
        "runtime": 30,
    }
    scenario = {"name": "write_test", "processes": [2], "threads": [1]}

    configurator = WriteConfigurator("dummy")
    cases = configurator.build_cases(scenario, common)

    assert len(cases) == 1
    case = cases[0]
    assert (
        case.name
        == "write_test_2procs_1threads_10MB_chunk_32MB_block_regional_30s_duration"
    )
    assert case.file_size_bytes == 0
    assert case.chunk_size_bytes == 10 * MB
    assert case.block_size_bytes == 32 * MB
    assert case.processes == 2
    assert case.files == 2  # threads * processes


def test_put_configurator(mock_config_dependencies):
    """Test that PutConfigurator correctly builds benchmark parameters."""
    common = {
        "bucket_types": ["regional"],
        "file_sizes_mb": [200],
        "chunk_sizes_mb": [50],
        "rounds": 1,
    }
    scenario = {"name": "put_test", "processes": [2], "threads": [1]}

    configurator = PutConfigurator("dummy")
    cases = configurator.build_cases(scenario, common)

    assert len(cases) == 1
    case = cases[0]
    assert case.name == "put_test_2procs_1threads_200MB_file_50MB_chunk_regional"
    assert case.file_size_bytes == 200 * MB
    assert case.chunk_size_bytes == 50 * MB
    assert case.processes == 2
    assert case.files == 2  # threads * processes
    assert case.bucket_name == "test-bucket"


def test_listing_configurator(mock_config_dependencies):
    """Test that ListingConfigurator correctly builds benchmark parameters."""
    common = {"bucket_types": ["regional"], "rounds": 1}
    scenario = {
        "name": "list_test",
        "processes": [1],
        "threads": [1],
        "depth": 2,
        "folders": [5],
        "files": [100],
        "pattern": "ls",
    }

    configurator = ListingConfigurator("dummy")
    cases = configurator.build_cases(scenario, common)

    assert len(cases) == 1
    case = cases[0]
    assert case.name == "list_test_1procs_1threads_100files_2depth_5folders_ls_regional"
    assert case.files == 100
    assert case.depth == 2
    assert case.folders == 5
    assert case.pattern == "ls"


def test_listing_configurator_walk_pattern(mock_config_dependencies):
    """Test that ListingConfigurator preserves walk pattern in parameters and id."""
    common = {"bucket_types": ["regional"], "rounds": 1}
    scenario = {
        "name": "walk_test",
        "processes": [1],
        "threads": [1],
        "depth": 8,
        "folders": [16],
        "files": [256],
        "pattern": "walk",
    }

    configurator = ListingConfigurator("dummy")
    cases = configurator.build_cases(scenario, common)

    assert len(cases) == 1
    case = cases[0]
    assert (
        case.name == "walk_test_1procs_1threads_256files_8depth_16folders_walk_regional"
    )
    assert case.pattern == "walk"


def test_info_configurator(mock_config_dependencies):
    """Test that InfoConfigurator correctly builds benchmark parameters."""
    common = {"bucket_types": ["regional"]}
    scenario = {
        "name": "info_test",
        "processes": [1],
        "threads": [1],
        "depth": 0,
        "pattern": "info",
        "target_type": "file",
        "files": [100],
        "folders": [1],
    }

    configurator = InfoConfigurator("dummy")
    cases = configurator.build_cases(scenario, common)

    assert len(cases) == 1
    case = cases[0]
    assert (
        case.name
        == "info_test_1procs_1threads_100files_0depth_1folders_file_info_regional"
    )
    assert case.files == 100
    assert case.depth == 0
    assert case.folders == 1
    assert case.pattern == "info"
    assert case.target_type == "file"


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

        # Info
        cases = get_info_benchmark_cases()
        assert len(cases) > 0, "Info config produced no cases"

        # Open
        cases = get_open_benchmark_cases()
        assert len(cases) > 0, "Open config produced no cases"

        # Put
        cases = get_put_benchmark_cases()
        assert len(cases) > 0, "Put config produced no cases"

        # Glob
        cases = get_glob_benchmark_cases()
        assert len(cases) > 0, "Glob config produced no cases"

        # Comparison
        cases = get_comparison_benchmark_cases()
        assert len(cases) > 0, "Comparison config produced no cases"

        # Cat Ranges
        cases = get_cat_ranges_benchmark_cases()
        assert len(cases) > 0, "Cat ranges config produced no cases"


def test_comparison_configurator(mock_config_dependencies):
    """Test that ComparisonConfigurator correctly builds benchmark parameters."""
    common = {
        "bucket_types": ["regional"],
        "file_sizes_mb": [1024],
        "chunk_sizes_mb": [50],
        "threads": [4],
        "rounds": 3,
    }
    scenario = {"name": "download_large_file"}

    configurator = ComparisonConfigurator("dummy")
    cases = configurator.build_cases(scenario, common)

    assert len(cases) == 1
    case = cases[0]
    assert case.scenario == "download_large_file"
    assert case.file_size_bytes == 1024 * MB
    assert case.chunk_size_bytes == 50 * MB
    assert case.threads == 4
    assert case.processes == 1
    assert case.rounds == 3
    assert case.bucket_type == "regional"


def test_cat_ranges_configurator(mock_config_dependencies):
    """Test that CatRangesConfigurator correctly builds benchmark parameters."""
    common = {
        "bucket_types": ["regional"],
        "file_sizes_mb": [256],
        "num_ranges_list": [200],
        "rounds": 1,
    }
    scenario = {
        "name": "cat_ranges_test",
        "pattern": "rand",
        "files": 1,
        "chunk_sizes_mb": [[0.0625, 1, 4, 16]],
        "batch_sizes": [64],
        "max_gaps": [1024],
    }

    configurator = CatRangesConfigurator("dummy")
    cases = configurator.build_cases(scenario, common)

    assert len(cases) == 1
    case = cases[0]
    assert (
        case.name
        == "cat_ranges_test_256MB_file_1files_mixed_0.0625_1_4_16MB_chunk_200ranges_64batch_1024maxgap_rand_regional"
    )
    assert case.files == 1
    assert case.threads == 1
    assert case.processes == 1
    assert case.num_ranges == 200
    assert case.batch_size == 64
    assert case.max_gap == 1024
    assert case.pattern == "rand"
    assert case.file_size_bytes == 256 * MB
    assert case.chunk_sizes_bytes == [
        int(0.0625 * MB),
        int(1 * MB),
        int(4 * MB),
        int(16 * MB),
    ]


def test_cat_ranges_configurator_empty_fallbacks(mock_config_dependencies):
    """Test CatRangesConfigurator falls back cleanly when optional lists are empty or None."""
    common = {"rounds": 1}
    scenario = {
        "name": "empty_fallbacks",
        "chunk_sizes_mb": None,
        "batch_sizes": None,
        "max_gaps": None,
    }
    configurator = CatRangesConfigurator("dummy")
    cases = configurator.build_cases(scenario, common)
    assert len(cases) == 1
    case = cases[0]
    assert case.chunk_sizes_bytes == [int(1 * MB)]
    assert case.batch_size is None
    assert case.max_gap is None


def test_cat_ranges_generate_ranges_sequential_exceeds_capacity():
    """Test sequential range generation raises ValueError when requested ranges exceed file size."""
    from gcsfs.tests.perf.microbenchmarks.cat_ranges.test_cat_ranges import (
        _generate_ranges,
    )

    # 100 MB file with 10 x 30 MB chunks exceeds 100 MB capacity
    with pytest.raises(
        ValueError,
        match="Requested sequential ranges exceed file size; cannot generate non-overlapping ranges.",
    ):
        _generate_ranges(
            file_paths=["test.bin"],
            file_size_bytes=100 * MB,
            chunk_sizes_bytes=30 * MB,
            num_ranges=10,
            pattern="seq",
        )


def test_cat_ranges_generate_ranges_sequential_contiguous():
    """Test sequential range generation produces contiguous non-overlapping ranges."""
    from gcsfs.tests.perf.microbenchmarks.cat_ranges.test_cat_ranges import (
        _generate_ranges,
    )

    # 100 MB file with 3 x 30 MB chunks fits within 100 MB
    paths, starts, ends = _generate_ranges(
        file_paths=["test.bin"],
        file_size_bytes=100 * MB,
        chunk_sizes_bytes=30 * MB,
        num_ranges=3,
        pattern="seq",
    )
    assert len(paths) == len(starts) == len(ends) == 3
    assert starts == [0, 30 * MB, 60 * MB]
    assert ends == [30 * MB, 60 * MB, 90 * MB]


def test_cat_ranges_generate_ranges_multi_file_sequential():
    """Test sequential range generation maintains independent sequential offsets per file."""
    from gcsfs.tests.perf.microbenchmarks.cat_ranges.test_cat_ranges import (
        _generate_ranges,
    )

    paths, starts, ends = _generate_ranges(
        file_paths=["file_a.bin", "file_b.bin"],
        file_size_bytes=100 * MB,
        chunk_sizes_bytes=10 * MB,
        num_ranges=6,
        pattern="seq",
    )
    assert paths == [
        "file_a.bin",
        "file_b.bin",
        "file_a.bin",
        "file_b.bin",
        "file_a.bin",
        "file_b.bin",
    ]
    assert starts == [0, 0, 10 * MB, 10 * MB, 20 * MB, 20 * MB]
    assert ends == [10 * MB, 10 * MB, 20 * MB, 20 * MB, 30 * MB, 30 * MB]


def test_cat_ranges_generate_ranges_invalid_pattern():
    """Test range generation raises ValueError on unsupported pattern."""
    from gcsfs.tests.perf.microbenchmarks.cat_ranges.test_cat_ranges import (
        _generate_ranges,
    )

    with pytest.raises(ValueError, match="Unsupported pattern"):
        _generate_ranges(
            file_paths=["test.bin"],
            file_size_bytes=100 * MB,
            chunk_sizes_bytes=10 * MB,
            num_ranges=5,
            pattern="mixed",
        )

    with pytest.raises(ValueError, match="Unsupported pattern"):
        _generate_ranges(
            file_paths=["test.bin"],
            file_size_bytes=100 * MB,
            chunk_sizes_bytes=10 * MB,
            num_ranges=5,
            pattern="unknown",
        )


def test_cat_ranges_op_error_handling():
    """Test _cat_ranges_op requests on_error='raise' and raises on failure."""
    from gcsfs.tests.perf.microbenchmarks.cat_ranges.test_cat_ranges import (
        _cat_ranges_op,
    )

    mock_gcs = mock.MagicMock()
    mock_gcs.cat_ranges.return_value = [b"data1", b"data2"]

    res = _cat_ranges_op(
        mock_gcs,
        paths=["a", "b"],
        starts=[0, 10],
        ends=[10, 20],
        max_gap=1024,
        batch_size=32,
    )
    assert res == [b"data1", b"data2"]
    mock_gcs.cat_ranges.assert_called_once_with(
        ["a", "b"],
        [0, 10],
        [10, 20],
        on_error="raise",
        max_gap=1024,
        batch_size=32,
    )

    # Test error handling when an Exception is returned in results
    mock_gcs.cat_ranges.return_value = [b"data1", OSError("Failed read")]
    with pytest.raises(OSError, match="Failed read"):
        _cat_ranges_op(mock_gcs, paths=["a", "b"], starts=[0, 10], ends=[10, 20])


def test_cat_ranges_generate_ranges_oversized_chunk():
    """Test range generation raises ValueError if chunk size exceeds file size."""
    from gcsfs.tests.perf.microbenchmarks.cat_ranges.test_cat_ranges import (
        _generate_ranges,
    )

    with pytest.raises(ValueError, match="exceeds file size"):
        _generate_ranges(
            file_paths=["test.bin"],
            file_size_bytes=10 * MB,
            chunk_sizes_bytes=20 * MB,
            num_ranges=1,
            pattern="seq",
        )


def test_cat_ranges_generate_ranges_rand_chunk_sizes_bytes_list():
    """Test random range generation with varying chunk sizes calculates actual total bytes."""
    from gcsfs.tests.perf.microbenchmarks.cat_ranges.test_cat_ranges import (
        _generate_ranges,
    )

    chunk_sizes = [1 * MB, 4 * MB, 16 * MB]
    paths, starts, ends = _generate_ranges(
        file_paths=["test.bin"],
        file_size_bytes=100 * MB,
        chunk_sizes_bytes=chunk_sizes,
        num_ranges=10,
        pattern="rand",
    )
    assert len(starts) == len(ends) == 10
    for s, e in zip(starts, ends):
        assert (e - s) in chunk_sizes
        assert 0 <= s < e <= 100 * MB

    total_bytes = sum(e - s for s, e in zip(starts, ends))
    assert total_bytes > 0


def test_publish_benchmark_extra_info_total_bytes():
    """Test publish_benchmark_extra_info publishes total_bytes property."""
    from gcsfs.tests.perf.microbenchmarks.conftest import publish_benchmark_extra_info

    class MockBenchmark:
        def __init__(self):
            self.extra_info = {}
            self.group = None

    class MockParams:
        files = 2
        file_size_bytes = 10 * MB
        chunk_size_bytes = 1 * MB
        threads = 1
        processes = 1
        rounds = 1
        bucket_name = "test-bucket"
        bucket_type = "regional"
        total_bytes = 25 * MB

    bench = MockBenchmark()
    publish_benchmark_extra_info(bench, MockParams(), "cat_ranges")
    assert bench.extra_info["total_bytes"] == 25 * MB

    # Test fallback to file_size * files when total_bytes is None
    del MockParams.total_bytes
    bench_fallback = MockBenchmark()
    publish_benchmark_extra_info(bench_fallback, MockParams(), "read")
    assert bench_fallback.extra_info["total_bytes"] == 20 * MB
