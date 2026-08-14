"""Generates deterministic VLM image documents in parallel tar shards."""

import io
import json
import math
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor

from gcsfs.tests.perf.subsystembenchmarks.dataloading.datagen import ingest_workers

# Qwen-VL patch factor: patch_size (14) * merge_size (2).
FACTOR = 28
# Qwen2-VL default max_pixels (28 * 28 * 1280).
DEFAULT_PIXEL_BUDGET = 28 * 28 * 1280

ENCODINGS = ("jpeg", "png", "npy")

# Aspect ratio range (portrait to landscape).
_MIN_ASPECT, _MAX_ASPECT = 0.5, 2.0

# Noise cell scale relative to shorter side to keep bytes-per-pixel consistent.
_NOISE_CELL_FRACTION = 1 / 8


def smart_resize(height, width, *, factor=FACTOR, max_pixels=DEFAULT_PIXEL_BUDGET):
    """Rounds dimensions down to multiples of factor within max_pixels."""
    h_bar = max(round(height / factor) * factor, factor)
    w_bar = max(round(width / factor) * factor, factor)
    if h_bar * w_bar > max_pixels:
        beta = math.sqrt((height * width) / max_pixels)
        h_bar = max(math.floor(height / beta / factor) * factor, factor)
        w_bar = max(math.floor(width / beta / factor) * factor, factor)
    return h_bar, w_bar


def image_shape(rng, pixel_budget):
    """Samples a random patch-aligned shape within pixel_budget."""
    aspect = rng.uniform(_MIN_ASPECT, _MAX_ASPECT)
    area = pixel_budget * rng.uniform(0.25, 1.0)
    height = math.sqrt(area / aspect)
    return smart_resize(
        max(1, int(height)), max(1, int(height * aspect)), max_pixels=pixel_budget
    )


def image_array(rng, height, width):
    """Generates an RGB uint8 image array of shape (height, width, 3)."""
    import numpy as np
    from PIL import Image

    cells = max(2, round(min(height, width) * _NOISE_CELL_FRACTION))
    grid = rng.integers(0, 256, size=(cells, cells, 3), dtype=np.uint8)
    small = Image.fromarray(grid, mode="RGB")
    return np.asarray(small.resize((width, height), Image.BICUBIC), dtype=np.uint8)


def encode_image(array, encoding, jpeg_quality):
    """Encodes an image array, returning (file_extension, payload_bytes)."""
    buf = io.BytesIO()
    if encoding == "npy":
        import numpy as np

        np.save(buf, array, allow_pickle=False)
        return "npy", buf.getvalue()

    from PIL import Image

    image = Image.fromarray(array, mode="RGB")
    if encoding == "jpeg":
        image.save(buf, format="JPEG", quality=jpeg_quality)
        return "jpg", buf.getvalue()
    if encoding == "png":
        image.save(buf, format="PNG", compress_level=6)
        return "png", buf.getvalue()
    raise ValueError(f"unknown encoding {encoding!r}; expected one of {ENCODINGS}")


def make_image(rng, pixel_budget, image_encoding, jpeg_quality):
    return encode_image(
        image_array(rng, *image_shape(rng, pixel_budget)),
        image_encoding,
        jpeg_quality,
    )


SAMPLE_SHAPES = ("pairs", "interleaved")
FORMATS = ("image_tar", "image_tar_gz")

# Shard extensions shared between writer and reader.
SHARD_EXT = {"image_tar": ".tar", "image_tar_gz": ".tar.gz"}

# Geometric distribution parameter and max cap for images per interleaved document.
_INTERLEAVED_P = 0.4
_MAX_IMAGES_PER_DOC = 8


def images_per_shard(image_count, shard_count):
    """Distributes images evenly across shards with remainder to lowest indices."""
    base, rem = divmod(image_count, shard_count)
    return [base + (1 if i < rem else 0) for i in range(shard_count)]


def plan_documents(rng, image_count, sample_shape):
    """Generates per-document image counts summing to image_count."""
    if sample_shape == "pairs":
        return [1] * image_count
    if sample_shape != "interleaved":
        raise ValueError(
            f"unknown sample_shape {sample_shape!r}; expected one of {SAMPLE_SHAPES}"
        )
    plan, remaining = [], image_count
    while remaining > 0:
        take = min(int(rng.geometric(_INTERLEAVED_P)), _MAX_IMAGES_PER_DOC, remaining)
        plan.append(take)
        remaining -= take
    return plan


def caption(rng):
    """Generates random hex alt-text filler (24-198 characters)."""
    return rng.bytes(int(rng.integers(12, 100))).hex()


def _member_name(extension, index, document_images):
    """Returns tar member name (numbered for multi-image documents)."""
    if document_images == 1:
        return extension
    return f"image_{index}.{extension}"


def _resolve_root(prefix):
    """Resolves fsspec filesystem and normalized root path for a prefix."""
    import fsspec

    fs, root = fsspec.core.url_to_fs(prefix)
    return fs, root.rstrip("/")


def _write_shard(
    prefix,
    idx,
    document_plan,
    fmt,
    pixel_budget,
    image_encoding,
    jpeg_quality,
    seed,
):
    """Writes a single tar shard deterministically using (seed, idx)."""
    import numpy as np
    import webdataset as wds

    fs, root = _resolve_root(prefix)
    rng = np.random.default_rng([seed, idx])
    path = f"{root}/shard_{idx:05d}{SHARD_EXT[fmt]}"
    images = 0
    with fs.open(path, "wb", finalize_on_close=True) as handle:
        with wds.TarWriter(handle, compress=(fmt == "image_tar_gz")) as sink:
            for doc_idx, doc_images in enumerate(document_plan):
                sample = {
                    "__key__": f"{idx:05d}{doc_idx:07d}",
                    "txt": caption(rng).encode(),
                    "json": json.dumps(
                        {"shard": idx, "doc": doc_idx, "images": doc_images}
                    ).encode(),
                }
                for j in range(doc_images):
                    extension, payload = make_image(
                        rng, pixel_budget, image_encoding, jpeg_quality
                    )
                    sample[_member_name(extension, j, doc_images)] = payload
                    images += 1
                sink.write(sample)
    return {
        "bytes": int(fs.info(path)["size"]),
        "documents": len(document_plan),
        "images": images,
        # Worker process PID for parallelism verification.
        "pid": os.getpid(),
    }


def _write_shard_task(args):
    """Unpacks arguments and writes one shard."""
    return _write_shard(*args)


def ingest_tar_shards(
    prefix,
    *,
    fmt,
    file_count,
    rows_per_file,
    pixel_budget,
    image_encoding,
    jpeg_quality,
    sample_shape,
    seed=0,
):
    """Writes tar shards in parallel and returns the corpus manifest."""
    import numpy as np

    if fmt not in FORMATS:
        raise ValueError(f"unknown fmt {fmt!r}; expected one of {FORMATS}")
    if sample_shape not in SAMPLE_SHAPES:
        raise ValueError(
            f"unknown sample_shape {sample_shape!r}; expected one of {SAMPLE_SHAPES}"
        )
    if image_encoding not in ENCODINGS:
        raise ValueError(
            f"unknown encoding {image_encoding!r}; expected one of {ENCODINGS}"
        )

    fs, root = _resolve_root(prefix)
    fs.makedirs(root, exist_ok=True)

    image_count = file_count * rows_per_file
    plans = [
        plan_documents(np.random.default_rng([seed, 1, idx]), count, sample_shape)
        for idx, count in enumerate(images_per_shard(image_count, file_count))
    ]
    # Pass per-shard plans directly to avoid pickling the full plan list.
    tasks = [
        (prefix, idx, plans[idx], fmt, pixel_budget, image_encoding, jpeg_quality, seed)
        for idx in range(file_count)
    ]

    # Use spawned process pool to bypass GIL during image synthesis and encoding.
    with ProcessPoolExecutor(
        max_workers=ingest_workers(file_count),
        mp_context=multiprocessing.get_context("spawn"),
    ) as pool:
        results = list(pool.map(_write_shard_task, tasks))

    corpus_bytes = sum(r["bytes"] for r in results)
    written_images = sum(r["images"] for r in results)
    return {
        "fmt": fmt,
        "file_count": file_count,
        "rows_per_file": rows_per_file,
        "sample_count": sum(r["documents"] for r in results),
        "corpus_bytes": corpus_bytes,
        "image_count": written_images,
        "mean_shard_bytes": corpus_bytes / file_count,
        "mean_image_bytes": corpus_bytes / written_images,
        # Distinct worker PIDs used during shard generation.
        "writer_process_count": len({r["pid"] for r in results}),
    }
