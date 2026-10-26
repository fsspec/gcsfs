import pytest

from gcsfs.core import GCSFileSystem


@pytest.mark.parametrize("protocol", ["", "gs://", "gcs://"])
@pytest.mark.parametrize(
    "key,expected_key,generation",
    [
        ("file.txt", "file.txt", None),
        ("http://example.com/a.txt#123", "http://example.com/a.txt", "123"),
        ("dir/file#variant.txt#123", "dir/file#variant.txt", "123"),
        ("dir/file?variant.txt#123", "dir/file?variant.txt", "123"),
        ("dir/file?generation=456#123", "dir/file", "123"),
        ("dir/file?generation=123", "dir/file", "123"),
        ("http://example.com/a.txt?generation=123", "http://example.com/a.txt", "123"),
        ("dir/file#variant.txt?generation=123", "dir/file#variant.txt", "123"),
        ("dir/file#123?generation=456", "dir/file#123", "456"),
        ("dir/file#v1#v2?generation=123", "dir/file#v1#v2", "123"),
        (
            "http://example.com/a#variant?generation=123",
            "http://example.com/a#variant",
            "123",
        ),
        ("dir/file#variant?generation=no", "dir/file#variant?generation=no", None),
        ("dir/file#variant?other=1&generation=123", "dir/file#variant", "123"),
        ("dir/file#variant?generation=123&other=1", "dir/file#variant", "123"),
        ("dir/file#variant?generation=123#456", "dir/file#variant", "456"),
        (
            "dir/file#variant?generation=123#invalid",
            "dir/file#variant?generation=123#invalid",
            None,
        ),
        ("dir/file#variant?generation=123#", "dir/file#variant", "123"),
        ("dir/file#variant?generation=", "dir/file#variant?generation=", None),
        ("dir/file#variant?generation=%31%32%33", "dir/file#variant", "123"),
        ("dir/file#variant.txt", "dir/file#variant.txt", None),
        ("dir/file?variant.txt", "dir/file?variant.txt", None),
        ("dir/file?generation=no", "dir/file?generation=no", None),
        ("dir/file#123#456", "dir/file#123", "456"),
        ("dir/file#123", "dir/file", "123"),
        ("dir/file?generation=123#", "dir/file", "123"),
        ("dir/file?generation=123#invalid", "dir/file?generation=123#invalid", None),
    ],
)
def test_split_path_preserves_object_key(protocol, key, expected_key, generation):
    assert GCSFileSystem._split_path(f"{protocol}bucket/{key}", version_aware=True) == (
        "bucket",
        expected_key,
        generation,
    )
    assert GCSFileSystem._split_path(
        f"{protocol}bucket/{key}", version_aware=False
    ) == ("bucket", key, None)


@pytest.mark.parametrize(
    "key", ["dir/file#variant.txt", "http://example.com/a#variant.txt"]
)
def test_read_old_generation_with_literal_hash_in_key(gcs_versioned, key):
    from gcsfs.tests.settings import TEST_VERSIONED_BUCKET

    path = f"{TEST_VERSIONED_BUCKET}/{key}"
    gcs_versioned.pipe_file(path, b"first version")
    generation = gcs_versioned.info(path)["generation"]
    gcs_versioned.pipe_file(path, b"second version")

    versioned_path = f"{path}?generation={generation}"
    assert gcs_versioned.cat_file(versioned_path) == b"first version"
    assert gcs_versioned.info(versioned_path)["generation"] == generation
    assert gcs_versioned.cat_file(path) == b"second version"
