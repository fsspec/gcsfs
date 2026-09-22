import pytest

from gcsfs.utils import is_empty_range


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
