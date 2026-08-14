import hashlib
import tarfile

import numpy as np
import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset import imagegen


def test_image_shape_is_patch_aligned_and_within_budget():
    rng = np.random.default_rng(0)
    budget = imagegen.DEFAULT_PIXEL_BUDGET
    for _ in range(50):
        h, w = imagegen.image_shape(rng, budget)
        assert h % imagegen.FACTOR == 0 and w % imagegen.FACTOR == 0
        assert h * w <= budget
        assert h >= imagegen.FACTOR and w >= imagegen.FACTOR


def test_image_array_is_deterministic_under_seed():
    a = imagegen.image_array(np.random.default_rng(7), 56, 84)
    b = imagegen.image_array(np.random.default_rng(7), 56, 84)
    assert a.shape == (56, 84, 3)
    assert a.dtype == np.uint8
    assert np.array_equal(a, b)


@pytest.mark.parametrize(
    "encoding,expected_ext", [("jpeg", "jpg"), ("png", "png"), ("npy", "npy")]
)
def test_encode_image_extension(encoding, expected_ext):
    """Verifies file extensions for supported image encodings."""
    arr = imagegen.image_array(np.random.default_rng(1), 56, 56)
    ext, payload = imagegen.encode_image(arr, encoding, 75)
    assert ext == expected_ext
    assert isinstance(payload, bytes) and payload


def test_unknown_encoding_is_rejected():
    arr = imagegen.image_array(np.random.default_rng(1), 56, 56)
    with pytest.raises(ValueError, match="unknown encoding"):
        imagegen.encode_image(arr, "webp", 75)


def _mean_jpeg_bytes_per_pixel(seed, pixel_budget, n):
    rng = np.random.default_rng(seed)
    pixels = payload_bytes = 0
    for _ in range(n):
        h, w = imagegen.image_shape(rng, pixel_budget)
        arr = imagegen.image_array(rng, h, w)
        payload_bytes += len(imagegen.encode_image(arr, "jpeg", 75)[1])
        pixels += h * w
    return payload_bytes / pixels


@pytest.mark.parametrize("budget", [50_176, imagegen.DEFAULT_PIXEL_BUDGET, 4_014_080])
def test_mean_jpeg_bytes_per_pixel_are_realistic_at_every_configured_budget(budget):
    """Verifies JPEG bytes-per-pixel ratio (~0.20 B/px at q75) across pixel budgets."""
    mean_bpp = _mean_jpeg_bytes_per_pixel(11, budget, 6)
    assert 0.18 <= mean_bpp <= 0.26, f"mean bytes/pixel {mean_bpp:.4f} outside band"


def test_make_image_returns_a_distinct_payload_every_call():
    """Ensures successive image generations produce unique payloads."""
    rng = np.random.default_rng(3)
    images = [imagegen.make_image(rng, 50_176, "jpeg", 75) for _ in range(8)]
    assert {ext for ext, _ in images} == {"jpg"}
    assert len({payload for _, payload in images}) == 8


def test_images_per_shard_sums_exactly_and_spreads_remainder():
    assert imagegen.images_per_shard(10, 3) == [4, 3, 3]
    assert sum(imagegen.images_per_shard(50176, 64)) == 50176


def test_plan_documents_interleaved_sums_exactly():
    rng = np.random.default_rng(0)
    plan = imagegen.plan_documents(rng, 200, "interleaved")
    assert sum(plan) == 200
    assert all(1 <= n <= 8 for n in plan)
    assert max(plan) > 1, "interleaved documents must hold more than one image"
    assert imagegen.plan_documents(rng, 5, "pairs") == [1] * 5


_SMALL = dict(
    file_count=3,
    rows_per_file=4,
    pixel_budget=imagegen.FACTOR * imagegen.FACTOR * 4,
    image_encoding="jpeg",
    jpeg_quality=75,
)


def _ingest(tmp_path, **overrides):
    pytest.importorskip("webdataset")
    kwargs = dict(_SMALL, fmt="image_tar", sample_shape="pairs", seed=0)
    kwargs.update(overrides)
    return imagegen.ingest_tar_shards(str(tmp_path) + "/data/", **kwargs)


def _members(tmp_path):
    names = []
    for shard in sorted((tmp_path / "data").iterdir()):
        with tarfile.open(shard) as tar:
            names.extend(tar.getnames())
    return names


def _payload_digests(tmp_path):
    """Computes SHA-256 digests for tar member payloads."""
    digests = {}
    for shard in sorted((tmp_path / "data").iterdir()):
        with tarfile.open(shard) as tar:
            for member in tar.getmembers():
                digests[member.name] = hashlib.sha256(
                    tar.extractfile(member).read()
                ).hexdigest()
    return digests


def test_manifest_counts_for_pairs(tmp_path):
    manifest = _ingest(tmp_path)
    assert manifest["image_count"] == 12
    assert manifest["sample_count"] == 12  # One document per image for pairs.
    assert manifest["file_count"] == 3
    assert manifest["corpus_bytes"] > 0
    assert manifest["fmt"] == "image_tar"


def test_pairs_members_are_jpg_txt_json(tmp_path):
    _ingest(tmp_path)
    names = _members(tmp_path)
    assert sum(n.endswith(".jpg") for n in names) == 12
    assert sum(n.endswith(".txt") for n in names) == 12
    assert sum(n.endswith(".json") for n in names) == 12


def test_interleaved_holds_more_images_than_documents(tmp_path):
    manifest = _ingest(tmp_path, sample_shape="interleaved")
    assert manifest["image_count"] == 12
    assert manifest["sample_count"] < 12
    names = _members(tmp_path)
    assert sum(n.endswith(".jpg") for n in names) == 12
    assert sum(n.endswith(".txt") for n in names) == manifest["sample_count"]


def test_gzip_format_writes_gzip_compressed_tars(tmp_path):
    _ingest(tmp_path, fmt="image_tar_gz")
    shards = sorted((tmp_path / "data").iterdir())
    assert all(p.name.endswith(".tar.gz") for p in shards)
    # Verify gzip magic bytes.
    for shard in shards:
        with open(shard, "rb") as raw:
            assert raw.read(2) == b"\x1f\x8b", f"{shard} is not gzip-compressed"
        with tarfile.open(shard, mode="r:gz") as tar:
            names = tar.getnames()
        assert any(n.endswith(".jpg") for n in names), f"{shard} holds no images"


def test_shard_bytes_stay_even_at_production_shard_sizes(tmp_path):
    """Verifies shard byte sizes remain evenly balanced across the corpus."""
    manifest = _ingest(
        tmp_path,
        file_count=4,
        rows_per_file=128,
        pixel_budget=imagegen.FACTOR * imagegen.FACTOR * 400,
    )
    sizes = [p.stat().st_size for p in sorted((tmp_path / "data").iterdir())]
    assert len(set(sizes)) > 1, "budget too small: every image quantized alike"
    spread = (max(sizes) - min(sizes)) / manifest["mean_shard_bytes"]
    assert spread < 0.15, f"shard sizes spread {spread:.3f} of the mean: {sizes}"


def test_every_stored_image_is_byte_unique(tmp_path):
    """Ensures all generated image payloads are unique to prevent deduplication."""
    _ingest(tmp_path, file_count=3, rows_per_file=16)
    digests = []
    for shard in sorted((tmp_path / "data").iterdir()):
        with tarfile.open(shard) as tar:
            digests += [
                hashlib.sha256(tar.extractfile(m).read()).hexdigest()
                for m in tar.getmembers()
                if m.name.endswith(".jpg")
            ]
    assert len(digests) == 48
    assert len(set(digests)) == 48, "duplicate image payloads in the corpus"


def test_corpus_bytes_depend_only_on_the_seed_not_on_the_pool(tmp_path):
    """Corpus generation is deterministic for a given seed across worker processes."""
    first, second = tmp_path / "a", tmp_path / "b"
    _ingest(first, sample_shape="interleaved", file_count=5, rows_per_file=7)
    _ingest(second, sample_shape="interleaved", file_count=5, rows_per_file=7)
    assert _payload_digests(first) == _payload_digests(second)


def test_shards_are_written_across_worker_processes(tmp_path):
    """Verifies shard generation utilizes multiple worker processes."""
    manifest = _ingest(tmp_path, file_count=4, rows_per_file=2)
    assert manifest["writer_process_count"] > 1


def test_unknown_fmt_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="unknown fmt"):
        _ingest(tmp_path, fmt="image_zip")


def test_unknown_sample_shape_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="unknown sample_shape"):
        _ingest(tmp_path, sample_shape="triples")
