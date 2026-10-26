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
