import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset import configs

CONFIG = configs.__file__


def _cases():
    return configs.WebDatasetReadConfigurator(CONFIG).generate_cases()


def _baseline():
    return next(c for c in _cases() if c.sweep_axis == "baseline")


def test_case_ids_are_unique_and_encode_the_corpus_shape():
    """Ensures all generated benchmark case IDs are distinct."""
    cases = _cases()
    assert len({c.name for c in cases}) == len(cases)
    assert all(c.name.startswith("read-wds-") for c in cases)

    baseline = _baseline()
    for token in ("imgtar", "shuf", f"nw{baseline.num_workers}", "sh64x784", "jpg75"):
        assert token in baseline.name, f"{token} missing from {baseline.name}"


def test_image_count_held_constant_except_divisibility_axis():
    for case in _cases():
        images = case.file_count * case.rows_per_file
        expected = 50_160 if case.sweep_axis == "shard_divisibility" else 50_176
        assert images == expected, f"{case.name} has {images} images"


def test_no_case_starves_a_split():
    for case in _cases():
        assert case.file_count >= case.split_count, f"{case.name} starves splits"


def test_read_concurrency_is_only_swept_where_it_can_take_effect():
    """Read concurrency requires buffer >= 2 * 5 MiB chunk threshold to take effect."""
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset import gcsfs_opener

    for case in _cases():
        if case.gcs_read_concurrency != gcsfs_opener.DEFAULT_READ_CONCURRENCY:
            assert (
                case.read_buffer_bytes >= 2 * 5 * 2**20
            ), f"{case.name} sweeps concurrency at a buffer too small to split"


def test_shard_shuffle_is_swept_where_a_split_holds_more_than_one_shard():
    """Shard shuffle sweep requires multiple shards per split."""
    shuffle_case = next(c for c in _cases() if c.sweep_axis == "shard_shuffle")
    assert shuffle_case.shards_per_split >= 32


def _corpus_shape(case):
    return (case.file_count, case.rows_per_file)


def test_confounded_axes_keep_a_partner_row_to_compare_against():
    """Ensures non-baseline sweep axes match a baseline partner configuration."""
    cases = _cases()

    shard_shuffle = next(c for c in cases if c.sweep_axis == "shard_shuffle")
    partners = [c for c in cases if c.sweep_axis == "shard_size"]
    assert _corpus_shape(shard_shuffle) in [_corpus_shape(c) for c in partners], (
        "shard_shuffle must share a corpus shape with a shard_size case; "
        "otherwise it is a two-factor change against every other row"
    )

    for case in cases:
        if case.sweep_axis != "gcs_read_concurrency":
            continue
        buffers = {c.read_buffer_bytes for c in cases if c.sweep_axis == "read_buffer"}
        assert case.read_buffer_bytes in buffers, (
            "gcs_read_concurrency must be swept at a buffer size the read_buffer "
            "axis also measures, or it has no partner row"
        )


def test_axis_names_are_complete():
    assert {c.sweep_axis for c in _cases()} == {
        "baseline",
        "pixel_budget",
        "encoding",
        "compression",
        "sample_shape",
        "shard_size",
        "shard_divisibility",
        "workers",
        "prefetch",
        "access",
        "shard_shuffle",
        "shuffle_buffer",
        "cache",
        "resampled",
        "gcs_read_mode",
        "gcs_read_concurrency",
        "read_buffer",
        "decode",
    }


def test_extra_columns_carry_every_swept_parameter():
    """Ensures all swept parameters are published in extra_columns."""
    baseline = _baseline()
    columns = baseline.extra_columns()
    assert columns["image_max_pixels"] == baseline.pixel_budget
    assert columns["image_jpeg_quality"] == baseline.jpeg_quality
    assert columns["sample_layout"] == "image_text_pair"
    assert columns["gcs_read_mode"] == baseline.gcs_read_mode
    assert columns["gcs_read_concurrency"] == baseline.gcs_read_concurrency
    assert columns["gcs_read_buffer_bytes"] == baseline.read_buffer_bytes
    assert columns["image_decode_enabled"] is False
    assert columns["shard_resampling_enabled"] is False
    # Swept values reported via shared columns.
    assert baseline.dataset_format == "image_tar_jpeg"
    assert baseline.read_access_pattern == "shuffled"


def test_interleaved_case_reports_its_layout():
    interleaved = next(c for c in _cases() if c.sweep_axis == "sample_shape")
    assert interleaved.extra_columns()["sample_layout"] == "interleaved_document"
    assert "ilv" in interleaved.name


def test_non_jpeg_case_reports_zero_jpeg_quality():
    png = next(c for c in _cases() if c.image_encoding == "png")
    assert png.extra_columns()["image_jpeg_quality"] == 0
    # Non-JPEG encodings are identified in dataset_format.
    assert png.dataset_format == "image_tar_png"


def test_gzip_case_is_identified_by_its_format():
    gz = next(c for c in _cases() if c.fmt == "image_tar_gz")
    assert "imgtgz" in gz.name
    assert gz.dataset_format == "image_tar_gz_jpeg"


def test_sequential_access_disables_both_shuffles():
    seq = next(c for c in _cases() if c.sweep_axis == "access")
    assert seq.read_access_pattern == "sequential"
    assert seq.extra_columns()["shuffle_buffer_size"] == 0


def test_shard_shuffle_off_is_distinguishable_from_a_full_shuffle():
    """Verifies read_access_pattern when shard shuffle is disabled."""
    no_shard_shuffle = next(c for c in _cases() if c.sweep_axis == "shard_shuffle")
    assert no_shard_shuffle.read_access_pattern == "shuffled_samples_only"
    assert no_shard_shuffle.extra_columns()["shuffle_buffer_size"] == 1000


@pytest.mark.parametrize(
    "field, value, match",
    [
        ("rows_per_file", 0, "rows_per_file must be >="),
        ("batch_size", 0, "batch_size must be >="),
        ("world_size", 0, "world_size must be >="),
        ("num_workers", -1, "num_workers must be >="),
        ("prefetch_factor", 0, "prefetch_factor must be >="),
        ("shuffle_buffer_size", 0, "shuffle_buffer_size must be >="),
        ("gcs_read_concurrency", 0, "gcs_read_concurrency must be >="),
        ("read_buffer_bytes", -1, "read_buffer_bytes must be >="),
        ("jpeg_quality", 0, "jpeg_quality must be >="),
        ("jpeg_quality", 96, "jpeg_quality must be <="),
        ("pixel_budget", 783, "pixel_budget must be >="),
        ("file_count", 31, "must be >= world_size"),
        # Invalid enum values.
        ("fmt", "image_zip", "fmt must be one of"),
        ("gcs_read_mode", "bogus", "gcs_read_mode must be one of"),
        ("image_encoding", "webp", "image_encoding must be one of"),
        ("sample_shape", "triples", "sample_shape must be one of"),
        ("access", "random", "access must be one of"),
    ],
)
def test_validate_case_rejects_invalid_values(field, value, match):
    configurator = configs.WebDatasetReadConfigurator(CONFIG)
    case = next(c for c in configurator.generate_cases() if c.sweep_axis == "baseline")
    setattr(case, field, value)
    with pytest.raises(ValueError, match=match):
        configurator.validate_case(case)


def test_validate_case_rejects_a_read_buffer_on_whole_object_reads():
    """whole_object mode is already in memory and cannot use a read buffer."""
    configurator = configs.WebDatasetReadConfigurator(CONFIG)
    case = next(c for c in configurator.generate_cases() if c.sweep_axis == "baseline")
    case.gcs_read_mode = "whole_object"
    case.read_buffer_bytes = 8 * 2**20
    with pytest.raises(ValueError, match="whole_object"):
        configurator.validate_case(case)


def test_buffer_tags_separate_sub_mib_sizes():
    """Buffer tags distinguish sub-MiB sizes (e.g. buf512k)."""
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset import parameters

    assert parameters._buffer_tag(32 * 2**20) == "buf32m"
    assert parameters._buffer_tag(1 << 20) == "buf1m"
    assert parameters._buffer_tag(512 << 10) != parameters._buffer_tag(256 << 10)


def test_sequential_case_carries_no_shard_shuffle_token():
    """Sequential access disables shuffling and omits the noshsh ID token."""
    seq = next(c for c in _cases() if c.sweep_axis == "access")
    seq.shard_shuffle = False
    assert "noshsh" not in seq.benchmark_name()


def test_exactly_one_case_decodes_inside_the_timed_loop():
    """Image decoding is enabled only for the dedicated 'decode' sweep axis."""
    decoding = [c for c in _cases() if c.decode]
    assert [c.sweep_axis for c in decoding] == ["decode"]
    assert "dec" in decoding[0].name
