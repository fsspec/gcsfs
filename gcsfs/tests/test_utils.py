import array

import pytest

from gcsfs.utils import as_byte_buffer, is_empty_range


@pytest.mark.parametrize(
    "start, end, size, expected, description",
    [
        # Unknown size
        (None, None, None, False, "Full file read with unknown size"),
        (0, 10, None, False, "Normal slice without size"),
        (None, 10, None, False, "Valid start=None slice without size"),
        (None, 0, None, True, "Slice ending at 0 without size"),
        (10, 5, None, True, "Inverted range without size"),
        (0, -1, None, False, "Negative end with unknown size (deferred)"),
        (-10, None, None, False, "Negative start with unknown size (deferred)"),
        (-10, -5, None, False, "Negative range with unknown size (deferred)"),
        # Zero size
        (None, None, 0, True, "Full file read of 0-byte file"),
        (0, 10, 0, True, "Read slice of 0-byte file"),
        (None, 0, 0, True, "Zero end read of 0-byte file"),
        (-5, None, 0, True, "Negative start read of 0-byte file"),
        # Known size
        (None, None, 100, False, "Full file read with known size"),
        (0, 10, 100, False, "Normal slice with size"),
        (None, 10, 100, False, "Valid start=None slice with size"),
        (5, 5, 100, True, "Zero-length range"),
        (10, 5, 100, True, "Inverted range with size"),
        (100, 150, 100, True, "Start at EOF"),
        (150, None, 100, True, "Start beyond EOF"),
        (50, None, 100, False, "Valid tail slice to EOF"),
        (None, 0, 100, True, "Slice ending at 0 with size"),
        (0, -1, 100, False, "Read up to last byte with size"),
        (99, -1, 100, True, "Zero-length negative end slice"),
        (100, -1, 100, True, "Inverted negative end slice"),
        (-10, None, 100, False, "Negative start suffix with size"),
        (-10, -5, 100, False, "Negative range with size"),
        (-5, -10, 100, True, "Inverted negative range with size"),
        (None, -100, 100, True, "Start is None, end == -size with size"),
        (None, -150, 100, True, "Start is None, end < -size with size"),
    ],
)
def test_is_empty_range(start, end, size, expected, description):
    assert (
        is_empty_range(start, end, size) == expected
    ), f"Failed for case: {description} (start={start}, end={end}, size={size})"


def test_as_byte_buffer_returns_bytes_unchanged():
    """bytes is not wrapped, so io.BytesIO can share it instead of copying."""
    data = b"abc"
    assert as_byte_buffer(data) is data


def _numpy():
    return pytest.importorskip("numpy")


@pytest.mark.parametrize(
    "make_data",
    [
        lambda: bytearray(b"abc"),
        lambda: memoryview(b"abc"),
        lambda: array.array("d", [1.5, 2.5]),
        lambda: memoryview(array.array("i", [1, 2, 3])),
        lambda: _numpy().arange(5, dtype="int32"),
        # bool() of this array is False although it holds 4 bytes.
        lambda: _numpy().zeros(1, dtype="int32"),
        lambda: _numpy().arange(6, dtype="float32").reshape(2, 3),
        lambda: _numpy().int32(7),
        lambda: _numpy().zeros(0, dtype="int32"),
        # memoryview.cast() rejects a zero in the shape.
        lambda: _numpy().zeros((0, 3), dtype="float32"),
    ],
    ids=[
        "bytearray",
        "memoryview",
        "typed_array",
        "typed_memoryview",
        "numpy",
        "numpy_single_zero",
        "numpy_2d",
        "numpy_scalar",
        "numpy_empty",
        "numpy_empty_2d",
    ],
)
def test_as_byte_buffer_has_bytes_semantics(make_data):
    data = make_data()
    payload = bytes(memoryview(data))

    buf = as_byte_buffer(data)

    assert bool(buf) is bool(payload)
    assert len(buf) == len(payload)
    assert bytes(buf) == payload
    assert bytes(buf[1:3]) == payload[1:3]


@pytest.mark.parametrize(
    "make_data",
    [
        lambda: "text",
        lambda: 3,
        lambda: memoryview(bytes(8))[::2],
        lambda: _numpy().arange(10)[::2],
        lambda: _numpy().asfortranarray(_numpy().ones((2, 3))),
    ],
    ids=["str", "int", "strided_memoryview", "strided_numpy", "fortran_numpy"],
)
def test_as_byte_buffer_rejects_non_contiguous_or_non_buffer(make_data):
    """Rejected like io.BytesIO.write() rejects them."""
    with pytest.raises(TypeError):
        as_byte_buffer(make_data())
