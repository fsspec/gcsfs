import unittest.mock as mock

import pytest

from gcsfs.tests.perf.microbenchmarks import configs
from gcsfs.tests.perf.microbenchmarks.cat.configs import (
    CatConfigurator,
    get_cat_benchmark_cases,
)
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
from gcsfs.tests.perf.microbenchmarks.pipe.configs import get_pipe_benchmark_cases
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
        mock.patch(
            "gcsfs.tests.perf.microbenchmarks.configs.BENCHMARK_BUCKET_TYPES", ""
        ),
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

        # Cat
        cases = get_cat_benchmark_cases()
        assert len(cases) > 0, "Cat config produced no cases"

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


def test_prepare_files_gcloud_sets_bidi_finalize_env():
    """Test that _prepare_files_gcloud sets CLOUDSDK_STORAGE_BIDI_STREAMING_FINALIZE_WRITES=True."""
    from gcsfs.tests.perf.microbenchmarks.conftest import _prepare_files_gcloud

    with (
        mock.patch("shutil.which", return_value="/usr/bin/gcloud"),
        mock.patch(
            "gcsfs.tests.perf.microbenchmarks.conftest._write_local_file"
        ) as mock_write,
        mock.patch("subprocess.run") as mock_run,
    ):
        mock_run.return_value = mock.MagicMock(returncode=0)
        assert _prepare_files_gcloud(["bucket/obj.bin"], 1024) is True
        mock_write.assert_called_once()
        _, kwargs = mock_run.call_args
        assert (
            kwargs["env"].get("CLOUDSDK_STORAGE_BIDI_STREAMING_FINALIZE_WRITES")
            == "True"
        )


def test_pytest_benchmark_generate_json_filters_empty_stats():
    """Test that pytest_benchmark_generate_json removes timed-out benchmarks with empty or partial stats.data."""
    from types import SimpleNamespace

    from gcsfs.tests.perf.microbenchmarks.conftest import pytest_benchmark_generate_json

    class FakeBench:
        def __init__(self, data, extra_info=None):
            self.stats = SimpleNamespace(data=data)
            self.extra_info = extra_info or {}

        def get(self, key, default=None):
            if key == "extra_info":
                return self.extra_info
            return default

    # Arrange
    completed = FakeBench([1.25])
    timed_out = FakeBench([])
    partial_timeout = FakeBench([1.25], extra_info={"rounds": 3})
    multi_proc = FakeBench(
        [],
        extra_info={
            "runs": [2.5],
            "min_run": 2.5,
            "max_run": 2.5,
            "mean_run": 2.5,
            "median_run": 2.5,
            "stddev_run": 0.0,
            "rounds": 1,
        },
    )
    benchmarks = [completed, timed_out, partial_timeout, multi_proc]

    # Act
    pytest_benchmark_generate_json(None, benchmarks, {}, {})

    # Assert
    assert benchmarks == [completed, multi_proc]
    assert multi_proc.stats.data == [2.5]


def test_cat_configurator(mock_config_dependencies):
    """Test that CatConfigurator correctly builds benchmark parameters."""
    common = {
        "bucket_types": ["regional"],
        "file_sizes_bytes": [8],
        "threads": [1],
        "files": 20,
        "rounds": 5,
    }
    scenario = {"name": "cat_file_whole", "pattern": "whole"}

    configurator = CatConfigurator("dummy")
    cases = configurator.build_cases(scenario, common)

    assert len(cases) == 1
    case = cases[0]
    assert case.pattern == "whole"
    # Left unset so the case exercises the concurrency production code gets.
    assert case.concurrency is None
    assert case.file_size_bytes == 8
    assert case.files == 20
    assert case.threads == 1
    assert case.processes == 1
    assert case.rounds == 5
    assert case.bucket_type == "regional"


def test_cat_configurator_covers_small_objects():
    """The shipped config must keep reading objects small enough to catch #1048.

    Request amplification only shows up below MIN_CHUNK_SIZE_FOR_CONCURRENCY,
    where the concurrent path never splits the range. If someone raises every
    size in configs.yaml the group stops testing what it was written for, so
    pin that here rather than leaving it to review.
    """
    from gcsfs.core import GCSFileSystem

    with (
        mock.patch(
            "gcsfs.tests.perf.microbenchmarks.configs.BUCKET_NAME_MAP",
            {"regional": "test-bucket"},
        ),
        mock.patch("gcsfs.tests.perf.microbenchmarks.configs.BENCHMARK_FILTER", ""),
    ):
        cases = get_cat_benchmark_cases()

    small = [
        c
        for c in cases
        if c.file_size_bytes < GCSFileSystem.MIN_CHUNK_SIZE_FOR_CONCURRENCY
    ]
    assert small, "cat config has no objects below MIN_CHUNK_SIZE_FOR_CONCURRENCY"

    # The whole-object read with no explicit range is the regressing call.
    assert any(c.pattern == "whole" and c.concurrency is None for c in small)
    # And the ranged control it is compared against.
    assert any(c.pattern == "ranged" for c in small)


def test_cat_configurator_rejects_unknown_pattern(mock_config_dependencies):
    """A typo in configs.yaml fails while building cases, not mid-benchmark."""
    configurator = CatConfigurator("dummy")

    with pytest.raises(ValueError, match="Unsupported cat pattern"):
        configurator.build_cases(
            {"name": "cat_typo", "pattern": "wohle"},
            {"bucket_types": ["regional"], "file_sizes_bytes": [8]},
        )


def test_cat_supported_patterns_all_have_an_operation():
    """Validation is only useful if every accepted pattern can actually run.

    Otherwise a pattern added to SUPPORTED_PATTERNS but not to the operation
    table passes configuration and then dies after the fixture has uploaded
    its objects -- exactly the failure the validation exists to prevent.
    """
    from gcsfs.tests.perf.microbenchmarks.cat.parameters import SUPPORTED_PATTERNS
    from gcsfs.tests.perf.microbenchmarks.cat.test_cat import CAT_OPERATIONS

    assert set(CAT_OPERATIONS) == set(SUPPORTED_PATTERNS)


def test_cat_ranges_configurator(mock_config_dependencies):
    """Test that CatRangesConfigurator correctly builds benchmark parameters and handles fallbacks."""
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

    # Test fallback defaults when optional lists are empty or None
    empty_scenario = {
        "name": "empty_fallbacks",
        "chunk_sizes_mb": None,
        "batch_sizes": None,
        "max_gaps": None,
    }
    fallback_cases = configurator.build_cases(empty_scenario, common)
    assert len(fallback_cases) == 1
    assert fallback_cases[0].chunk_sizes_bytes == [int(1 * MB)]
    assert fallback_cases[0].batch_size is None
    assert fallback_cases[0].max_gap is None


@pytest.mark.parametrize(
    "file_paths,file_size,kwargs,expected_match",
    [
        (
            ["test.bin"],
            10 * MB,
            {"chunk_sizes_bytes": 20 * MB, "num_ranges": 1, "pattern": "seq"},
            "exceeds file size",
        ),
        (
            ["test.bin"],
            100 * MB,
            {"chunk_sizes_bytes": 30 * MB, "num_ranges": 10, "pattern": "seq"},
            "Requested sequential ranges exceed file size",
        ),
        (
            ["file_a.bin", "file_b.bin"],
            100 * MB,
            {"chunk_sizes_bytes": 40 * MB, "num_ranges": 5, "pattern": "seq"},
            "Requested sequential ranges exceed file size",
        ),
        (
            ["test.bin"],
            100 * MB,
            {"chunk_sizes_bytes": 10 * MB, "num_ranges": 5, "pattern": "mixed"},
            "Unsupported pattern",
        ),
        (
            ["test.bin"],
            100 * MB,
            {"chunk_sizes_bytes": 10 * MB, "num_ranges": 5, "pattern": "unknown"},
            "Unsupported pattern",
        ),
    ],
)
def test_cat_ranges_generate_ranges_validation_errors(
    file_paths, file_size, kwargs, expected_match
):
    """Test that _generate_ranges raises ValueError for invalid configurations."""
    from gcsfs.tests.perf.microbenchmarks.cat_ranges.test_cat_ranges import (
        _generate_ranges,
    )

    with pytest.raises(ValueError, match=expected_match):
        _generate_ranges(
            file_paths=file_paths,
            file_size_bytes=file_size,
            **kwargs,
        )


@pytest.mark.parametrize(
    "file_paths,num_ranges,chunk_size,expected_starts,expected_ends",
    [
        (
            ["test.bin"],
            3,
            30 * MB,
            [0, 30 * MB, 60 * MB],
            [30 * MB, 60 * MB, 90 * MB],
        ),
        (
            ["file_a.bin", "file_b.bin"],
            6,
            10 * MB,
            [0, 0, 10 * MB, 10 * MB, 20 * MB, 20 * MB],
            [10 * MB, 10 * MB, 20 * MB, 20 * MB, 30 * MB, 30 * MB],
        ),
    ],
)
def test_cat_ranges_generate_ranges_sequential(
    file_paths, num_ranges, chunk_size, expected_starts, expected_ends
):
    """Test sequential range generation produces contiguous non-overlapping ranges per file."""
    from gcsfs.tests.perf.microbenchmarks.cat_ranges.test_cat_ranges import (
        _generate_ranges,
    )

    paths, starts, ends = _generate_ranges(
        file_paths=file_paths,
        file_size_bytes=100 * MB,
        chunk_sizes_bytes=chunk_size,
        num_ranges=num_ranges,
        pattern="seq",
    )
    assert len(paths) == num_ranges
    assert starts == expected_starts
    assert ends == expected_ends


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


def test_listing_configurator_skips_invalid_folders_and_files(mock_config_dependencies):
    """Test that ListingConfigurator filters out invalid files/folders combinations."""
    common = {"bucket_types": ["regional"], "rounds": 1}
    scenario = {
        "name": "delete_fsdp_checkpoint_rank_folders",
        "processes": [1],
        "threads": [1],
        "depth": 1,
        "folders": [0, 8, 32, 128, 512],
        "files": [100, 1000],
    }

    configurator = ListingConfigurator("dummy")
    cases = configurator.build_cases(scenario, common)

    # folders=0 is skipped when files > 0; for files=100 only folders=8, 32
    # are valid; for files=1000 folders=8, 32, 128, 512 are valid.
    assert len(cases) == 6
    assert all(0 < c.folders <= c.files for c in cases)


@pytest.mark.parametrize(
    "get_cases",
    [get_delete_benchmark_cases, get_open_benchmark_cases],
)
def test_shipped_listing_based_cases_have_valid_folders_and_files(
    mock_config_dependencies, get_cases
):
    """Ensure shipped listing-based configs have valid folders and files counts."""
    cases = get_cases()
    assert cases
    assert all(0 < c.folders <= c.files for c in cases)


@pytest.mark.parametrize("folders,files", [(128, 100), (0, 10)])
def test_benchmark_listing_fixture_helper_rejects_invalid_folders_and_files(
    folders, files
):
    """Test that _benchmark_listing_fixture_helper raises ValueError when not (0 < folders <= files)."""
    from types import SimpleNamespace

    from gcsfs.tests.perf.microbenchmarks.conftest import (
        _benchmark_listing_fixture_helper,
    )

    params = SimpleNamespace(
        name="bad_delete_case",
        bucket_name="test-bucket",
        folders=folders,
        depth=1,
        files=files,
    )
    gen = _benchmark_listing_fixture_helper(
        lambda: mock.MagicMock(), params, "benchmark-delete", teardown=False
    )
    with pytest.raises(ValueError, match="require 0 < folders .* <= files"):
        next(gen)


def test_bucket_name_map_maps_all_six_rcu_bucket_types():
    """Verify flat and HNS RCU bucket_type variants map to their respective test buckets."""
    from gcsfs.tests import settings
    from gcsfs.tests.conftest import BUCKET_NAME_MAP

    for b_type in ("standard_rcu_z", "standard_rcu_warm", "standard_rcu_cold"):
        assert BUCKET_NAME_MAP[b_type] == settings.TEST_STANDARD_RCU_BUCKET

    for b_type in ("hns_rcu_z", "hns_rcu_warm", "hns_rcu_cold"):
        assert BUCKET_NAME_MAP[b_type] == settings.TEST_HNS_RCU_BUCKET


@pytest.mark.parametrize(
    "bucket_types_env,expected_resolved",
    [
        (
            "",
            {
                "regional": "reg-bkt",
                "standard_rcu_z": "std-rcu-bkt",
                "standard_rcu_warm": "std-rcu-bkt",
                "standard_rcu_cold": "std-rcu-bkt",
                "hns_rcu_z": "hns-rcu-bkt",
            },
        ),
        (
            "standard_rcu_z, hns_rcu_z",
            {
                "regional": None,
                "standard_rcu_z": "std-rcu-bkt",
                "standard_rcu_warm": None,
                "standard_rcu_cold": None,
                "hns_rcu_z": "hns-rcu-bkt",
            },
        ),
        (
            "standard_rcu_warm standard_rcu_cold",
            {
                "regional": None,
                "standard_rcu_z": None,
                "standard_rcu_warm": "std-rcu-bkt",
                "standard_rcu_cold": "std-rcu-bkt",
                "hns_rcu_z": None,
            },
        ),
    ],
)
def test_get_bucket_name_filters_by_benchmark_bucket_types(
    bucket_types_env, expected_resolved
):
    """Verify BENCHMARK_BUCKET_TYPES selects specific variants on a shared RCU bucket."""
    fake_map = {
        "regional": "reg-bkt",
        "standard_rcu_z": "std-rcu-bkt",
        "standard_rcu_warm": "std-rcu-bkt",
        "standard_rcu_cold": "std-rcu-bkt",
        "hns_rcu_z": "hns-rcu-bkt",
    }
    with (
        mock.patch(
            "gcsfs.tests.perf.microbenchmarks.configs.BUCKET_NAME_MAP", fake_map
        ),
        mock.patch(
            "gcsfs.tests.perf.microbenchmarks.configs.BENCHMARK_BUCKET_TYPES",
            bucket_types_env,
        ),
    ):
        configurator = configs.BaseBenchmarkConfigurator("dummy")
        for b_type, expected_bucket in expected_resolved.items():
            assert configurator.get_bucket_name(b_type) == expected_bucket


def test_rcu_bucket_types_in_shipped_yaml_configs():
    """Verify all 11 active microbenchmark groups include the expected RCU bucket_types and open stays untouched."""
    all_six_rcu = {
        "standard_rcu_z",
        "standard_rcu_warm",
        "standard_rcu_cold",
        "hns_rcu_z",
        "hns_rcu_warm",
        "hns_rcu_cold",
    }
    write_rcu = {"standard_rcu_z", "hns_rcu_z"}
    metadata_rcu = {
        "standard_rcu_z",
        "standard_rcu_cold",
        "hns_rcu_z",
        "hns_rcu_cold",
    }

    with (
        mock.patch("gcsfs.tests.perf.microbenchmarks.configs.BENCHMARK_FILTER", ""),
        mock.patch(
            "gcsfs.tests.perf.microbenchmarks.configs.BENCHMARK_BUCKET_TYPES", ""
        ),
    ):
        # Read groups: all 6 RCU bucket types (including read_reopen_fixed_duration)
        read_cases = get_read_benchmark_cases()
        assert all_six_rcu.issubset({c.bucket_type for c in read_cases})
        reopen_types = {
            c.bucket_type
            for c in read_cases
            if c.name.startswith("read_reopen_fixed_duration")
        }
        assert all_six_rcu.issubset(reopen_types)

        for get_fn in (get_cat_benchmark_cases, get_cat_ranges_benchmark_cases):
            types = {c.bucket_type for c in get_fn()}
            assert all_six_rcu.issubset(types)

        # Write groups: only *_rcu_z among RCU types
        for get_fn in (
            get_write_benchmark_cases,
            get_put_benchmark_cases,
            get_pipe_benchmark_cases,
        ):
            rcu_types = {c.bucket_type for c in get_fn()} & all_six_rcu
            assert rcu_types == write_rcu

        # Comparison group
        comp_cases = get_comparison_benchmark_cases()
        dl_large = [c for c in comp_cases if c.scenario == "download_large_file"]
        for b_type in (
            "standard_rcu_warm",
            "standard_rcu_cold",
            "hns_rcu_warm",
            "hns_rcu_cold",
        ):
            sizes_mb = {
                c.file_size_bytes // MB for c in dl_large if c.bucket_type == b_type
            }
            assert sizes_mb == {1024, 10240, 51200, 102400}
        for b_type in ("standard_rcu_z", "hns_rcu_z"):
            sizes_mb = {
                c.file_size_bytes // MB for c in dl_large if c.bucket_type == b_type
            }
            assert sizes_mb == {1024, 10240, 51200, 102400, 204800}

        dl_small_types = {
            c.bucket_type for c in comp_cases if c.scenario == "download_small_files"
        }
        assert dl_small_types == {"regional", "zonal"} | all_six_rcu

        ul_large = [c for c in comp_cases if c.scenario == "upload_large_file"]
        assert ({c.bucket_type for c in ul_large} & all_six_rcu) == write_rcu
        for b_type in ("standard_rcu_z", "hns_rcu_z"):
            sizes_mb = {
                c.file_size_bytes // MB for c in ul_large if c.bucket_type == b_type
            }
            assert sizes_mb == {1024, 10240, 51200, 102400, 204800}

        ul_small_types = {
            c.bucket_type for c in comp_cases if c.scenario == "upload_small_files"
        }
        assert ul_small_types == {"regional", "zonal"} | write_rcu

        # 0-byte Metadata groups: *_rcu_z and *_rcu_cold, never *_rcu_warm
        for get_fn in (
            get_listing_benchmark_cases,
            get_glob_benchmark_cases,
            get_info_benchmark_cases,
            get_delete_benchmark_cases,
            get_rename_benchmark_cases,
        ):
            rcu_types = {c.bucket_type for c in get_fn()} & all_six_rcu
            assert rcu_types == metadata_rcu

        # Open group stays untouched
        open_types = {c.bucket_type for c in get_open_benchmark_cases()}
        assert open_types == {"regional", "zonal", "hns"}
