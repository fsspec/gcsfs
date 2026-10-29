import pytest

from gcsfs.core import GCSFileSystem


@pytest.mark.parametrize("protocol", ["", "gs://", "gcs://"])
@pytest.mark.parametrize(
    "key,expected_key,generation",
    [
        ("file.txt", "file.txt", None),
        ("dir/a?b?generation=1", "dir/a?b?generation=1", None),
        ("dir/a?b#c?generation=1", "dir/a?b#c?generation=1", None),
        ("dir/a?b#123", "dir/a?b", "123"),
        ("dir/file?foo=bar#123", "dir/file?foo=bar", "123"),
        ("dir/file?generation=abc#123", "dir/file?generation=abc", "123"),
        ("dir/file#?generation=123", "dir/file#", "123"),
        ("dir/file#", "dir/file#", None),
        ("dir/file#123#", "dir/file#123#", None),
        ("dir/file?generation=123&generation=456", "dir/file", "123"),
        ("dir/file?generation=+123", "dir/file", " 123"),
        ("dir/file#+123", "dir/file", "+123"),
        ("dir/file#0123", "dir/file", "0123"),
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
