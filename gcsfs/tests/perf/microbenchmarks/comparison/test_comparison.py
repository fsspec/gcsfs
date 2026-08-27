import logging
import mmap
import os
import shutil
import subprocess
import tempfile
import time
import uuid

import pytest

from gcsfs.tests.perf.microbenchmarks.comparison.configs import (
    get_comparison_benchmark_cases,
)
from gcsfs.tests.perf.microbenchmarks.conftest import (
    MB,
    _prepare_files,
    _write_local_file,
    publish_benchmark_extra_info,
    publish_resource_metrics,
)

BENCHMARK_GROUP = "comparison"


# --- GCSFS Operations ---


def _gcsfs_get(gcs, remote_path, local_path, chunk_size):
    """Download a remote GCS file to local disk via gcs.get()."""
    if os.path.exists(local_path):
        os.remove(local_path)
    gcs.get(remote_path, local_path, chunk_size=chunk_size)


def _gcsfs_cat_file(gcs, remote_path):
    """Read a remote GCS file directly into memory via gcs.cat_file()."""
    return gcs.cat_file(remote_path)


def _gcsfs_open_read(gcs, remote_path, chunk_size):
    """Stream a remote GCS file in chunks via gcs.open('rb').read()."""
    with gcs.open(remote_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break


def _gcsfs_get_batch(gcs, remote_dir, local_dir, threads):
    """Download multiple files or a directory to local disk via gcs.get(recursive=True)."""
    os.makedirs(local_dir, exist_ok=True)
    gcs.get(remote_dir, local_dir, recursive=True, batch_size=threads)


def _gcsfs_cat_batch(gcs, remote_dir, threads):
    """Download multiple files directly into memory via gcs.cat()."""
    file_paths = gcs.find(remote_dir)
    return gcs.cat(file_paths, batch_size=threads)


def _gcsfs_put(gcs, local_path, remote_path, chunk_size):
    """Upload a local file to GCS via gcs.put()."""
    gcs.put(local_path, remote_path, chunksize=chunk_size)


def _gcsfs_pipe(gcs, local_path, remote_path, chunk_size):
    """Upload data to GCS using in-memory or memory-mapped buffer via gcs.pipe()."""
    with open(local_path, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            gcs.pipe(remote_path, mm, chunksize=chunk_size)


def _gcsfs_open_write(gcs, local_path, remote_path, chunk_size):
    """Upload data by streaming chunks to gcs.open('wb')."""
    with (
        open(local_path, "rb") as src,
        gcs.open(remote_path, "wb", finalize_on_close=True) as dst,
    ):
        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            dst.write(chunk)


def _gcsfs_put_batch(gcs, local_dir, remote_dir, threads):
    """Upload a local directory tree to GCS via gcs.put(recursive=True)."""
    gcs.put(local_dir, remote_dir, recursive=True, batch_size=threads)


def _gcsfs_pipe_batch(gcs, local_dir, remote_dir):
    """Upload multiple files using gcs.pipe() with path-to-bytes dictionary."""
    mapping = {}
    open_files = []
    mmaps = []
    try:
        for root, _, files in os.walk(local_dir):
            for f in files:
                lp = os.path.join(root, f)
                rel = os.path.relpath(lp, local_dir)
                rp = f"{remote_dir.rstrip('/')}/{rel}"
                file_size = os.path.getsize(lp)
                if file_size > 0:
                    src = open(lp, "rb")
                    open_files.append(src)
                    mm = mmap.mmap(src.fileno(), 0, access=mmap.ACCESS_READ)
                    mmaps.append(mm)
                    mapping[rp] = mm
                else:
                    mapping[rp] = b""
        gcs.pipe(mapping)
    finally:
        for mm in mmaps:
            mm.close()
        for src in open_files:
            src.close()


# --- GCloud CLI Operations ---


def _gcloud_cp(src, dst, threads=None):
    """Copy a file using gcloud storage cp."""
    if not dst.startswith("gs://") and os.path.exists(dst):
        os.remove(dst)
    cmd = ["gcloud", "storage", "cp", src, dst]
    env = os.environ.copy()
    if threads:
        env["CLOUDSDK_STORAGE_THREAD_COUNT"] = str(threads)
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if res.returncode != 0:
        raise RuntimeError(f"gcloud storage cp failed:\n{res.stderr}")


def _gcloud_cat(remote_path):
    """Stream a cloud object to stdout using gcloud storage cat."""
    cmd = ["gcloud", "storage", "cat", f"gs://{remote_path}"]
    res = subprocess.run(
        cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True
    )
    if res.returncode != 0:
        raise RuntimeError(f"gcloud storage cat failed:\n{res.stderr}")


def _gcloud_cp_batch(src_pattern, dst_dir, threads=None):
    """Copy multiple files using gcloud storage cp --recursive."""
    if not dst_dir.startswith("gs://"):
        os.makedirs(dst_dir, exist_ok=True)
    cmd = ["gcloud", "storage", "cp", "--recursive", src_pattern, dst_dir]
    env = os.environ.copy()
    if threads:
        env["CLOUDSDK_STORAGE_THREAD_COUNT"] = str(threads)
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if res.returncode != 0:
        raise RuntimeError(f"gcloud storage cp --recursive failed:\n{res.stderr}")


# --- Benchmark Helper ---


def run_comparison_benchmark(
    benchmark, monitor_cls, params, method_name, engine, func, args
):
    """Helper to run a benchmark test, publish parameters, and record telemetry."""
    params.method = method_name
    params.engine = engine
    publish_benchmark_extra_info(benchmark, params, BENCHMARK_GROUP)

    with monitor_cls() as m:
        benchmark.pedantic(func, rounds=params.rounds, args=args)

    publish_resource_metrics(benchmark, m)


# --- Fixtures ---


@pytest.fixture
def gcsfs_benchmark_comparison_download_large(extended_gcs_factory, request):
    """Sets up a remote large file on GCS and creates local destination scratch space."""
    params = request.param
    gcs = extended_gcs_factory()
    prefix = f"{params.bucket_name}/benchmark-comparison-{uuid.uuid4().hex[:8]}"
    remote_path = f"{prefix}/large_file.bin"
    temp_dir = tempfile.mkdtemp(prefix="gcsfs-comparison-bench-dl-")
    local_path = os.path.join(temp_dir, "downloaded.bin")

    logging.info(
        f"Setting up large file benchmark '{params.name}': creating remote object {remote_path} "
        f"of size {params.file_size_bytes / MB:.2f} MB."
    )
    try:
        start_time = time.perf_counter()
        _prepare_files(gcs, [remote_path], params.file_size_bytes)
        duration_ms = (time.perf_counter() - start_time) * 1000
        logging.info(f"Remote test object created in {duration_ms:.2f} ms.")

        yield gcs, remote_path, local_path, params
    finally:
        logging.info(f"Cleaning up {remote_path} and {temp_dir}...")
        try:
            gcs.rm(prefix, recursive=True)
        except Exception as e:
            logging.error(f"Failed to clean up remote benchmark files: {e!r}")
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def gcsfs_benchmark_comparison_download_small(extended_gcs_factory, request):
    """Sets up multiple remote small files on GCS and creates a local destination folder."""
    params = request.param
    gcs = extended_gcs_factory()
    prefix = f"{params.bucket_name}/benchmark-comparison-small-{uuid.uuid4().hex[:8]}"
    file_paths = [f"{prefix}/file_{i:06d}.bin" for i in range(params.files)]
    temp_dir = tempfile.mkdtemp(prefix="gcsfs-comparison-bench-dl-small-")
    local_dir = os.path.join(temp_dir, "downloads")

    logging.info(
        f"Setting up small files download benchmark '{params.name}': creating {params.files} remote files "
        f"of size {params.file_size_bytes / MB:.2f} MB each."
    )
    try:
        start_time = time.perf_counter()
        _prepare_files(gcs, file_paths, params.file_size_bytes)
        duration_ms = (time.perf_counter() - start_time) * 1000
        logging.info(f"Remote test files created in {duration_ms:.2f} ms.")

        yield gcs, prefix, local_dir, params
    finally:
        logging.info(f"Cleaning up {prefix} and {temp_dir}...")
        try:
            gcs.rm(prefix, recursive=True)
        except Exception as e:
            logging.error(f"Failed to clean up remote benchmark files: {e!r}")
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def gcsfs_benchmark_comparison_upload_large(extended_gcs_factory, request):
    """Creates a local large source file and targets a remote GCS path."""
    params = request.param
    gcs = extended_gcs_factory()
    prefix = f"{params.bucket_name}/benchmark-comparison-ul-{uuid.uuid4().hex[:8]}"
    remote_path = f"{prefix}/uploaded.bin"
    temp_dir = tempfile.mkdtemp(prefix="gcsfs-comparison-bench-ul-")
    local_path = os.path.join(temp_dir, "source.bin")

    logging.info(
        f"Setting up large file upload benchmark '{params.name}': creating local source file {local_path} "
        f"of size {params.file_size_bytes / MB:.2f} MB."
    )
    try:
        start_time = time.perf_counter()
        _write_local_file(local_path, params.file_size_bytes)
        duration_ms = (time.perf_counter() - start_time) * 1000
        logging.info(f"Local source file created in {duration_ms:.2f} ms.")

        yield gcs, local_path, remote_path, params
    finally:
        logging.info(f"Cleaning up {remote_path} and {temp_dir}...")
        try:
            gcs.rm(prefix, recursive=True)
        except Exception as e:
            logging.error(f"Failed to clean up remote benchmark files: {e!r}")
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def gcsfs_benchmark_comparison_upload_small(extended_gcs_factory, request):
    """Creates a local directory with multiple small files and targets a remote GCS directory."""
    params = request.param
    gcs = extended_gcs_factory()
    prefix = (
        f"{params.bucket_name}/benchmark-comparison-ul-small-{uuid.uuid4().hex[:8]}"
    )
    temp_dir = tempfile.mkdtemp(prefix="gcsfs-comparison-bench-ul-small-")
    local_dir = os.path.join(temp_dir, "sources")
    os.makedirs(local_dir, exist_ok=True)

    logging.info(
        f"Setting up small files upload benchmark '{params.name}': creating {params.files} local files at {local_dir}."
    )
    try:
        start_time = time.perf_counter()
        for i in range(params.files):
            file_path = os.path.join(local_dir, f"file_{i:06d}.bin")
            _write_local_file(file_path, params.file_size_bytes)
        duration_ms = (time.perf_counter() - start_time) * 1000
        logging.info(f"Local test files created in {duration_ms:.2f} ms.")

        yield gcs, local_dir, prefix, params
    finally:
        logging.info(f"Cleaning up {prefix} and {temp_dir}...")
        try:
            gcs.rm(prefix, recursive=True)
        except Exception as e:
            logging.error(f"Failed to clean up remote benchmark files: {e!r}")
        shutil.rmtree(temp_dir, ignore_errors=True)


# --- Test Cases ---

all_benchmark_cases = get_comparison_benchmark_cases()
download_large_cases = [
    c for c in all_benchmark_cases if c.scenario == "download_large_file"
]
download_small_cases = [
    c for c in all_benchmark_cases if c.scenario == "download_small_files"
]
upload_large_cases = [
    c for c in all_benchmark_cases if c.scenario == "upload_large_file"
]
upload_small_cases = [
    c for c in all_benchmark_cases if c.scenario == "upload_small_files"
]


gcloud_required = pytest.mark.skipif(
    shutil.which("gcloud") is None,
    reason="gcloud CLI is not installed or available on PATH",
)


# 1. Download Single Large File Tests


@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_download_large",
    download_large_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_download_large_file_gcsfs_get(
    benchmark, gcsfs_benchmark_comparison_download_large, monitor
):
    gcs, remote_path, local_path, params = gcsfs_benchmark_comparison_download_large
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "get",
        "gcsfs",
        _gcsfs_get,
        (gcs, remote_path, local_path, params.chunk_size_bytes),
    )


@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_download_large",
    download_large_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_download_large_file_gcsfs_cat_file(
    benchmark, gcsfs_benchmark_comparison_download_large, monitor
):
    gcs, remote_path, _, params = gcsfs_benchmark_comparison_download_large
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "cat_file",
        "gcsfs",
        _gcsfs_cat_file,
        (gcs, remote_path),
    )


@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_download_large",
    download_large_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_download_large_file_gcsfs_open_read(
    benchmark, gcsfs_benchmark_comparison_download_large, monitor
):
    gcs, remote_path, _, params = gcsfs_benchmark_comparison_download_large
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "open_read",
        "gcsfs",
        _gcsfs_open_read,
        (gcs, remote_path, params.chunk_size_bytes),
    )


@gcloud_required
@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_download_large",
    download_large_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_download_large_file_gcloud_cp(
    benchmark, gcsfs_benchmark_comparison_download_large, monitor
):
    _, remote_path, local_path, params = gcsfs_benchmark_comparison_download_large
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "cp",
        "gcloud",
        _gcloud_cp,
        (f"gs://{remote_path}", local_path, params.threads),
    )


@gcloud_required
@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_download_large",
    download_large_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_download_large_file_gcloud_cat(
    benchmark, gcsfs_benchmark_comparison_download_large, monitor
):
    _, remote_path, _, params = gcsfs_benchmark_comparison_download_large
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "cat",
        "gcloud",
        _gcloud_cat,
        (remote_path,),
    )


# 2. Download Multiple Small Files Tests


@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_download_small",
    download_small_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_download_small_files_gcsfs_get(
    benchmark, gcsfs_benchmark_comparison_download_small, monitor
):
    gcs, remote_dir, local_dir, params = gcsfs_benchmark_comparison_download_small
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "get",
        "gcsfs",
        _gcsfs_get_batch,
        (gcs, remote_dir, local_dir, params.threads),
    )


@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_download_small",
    download_small_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_download_small_files_gcsfs_cat(
    benchmark, gcsfs_benchmark_comparison_download_small, monitor
):
    gcs, remote_dir, _, params = gcsfs_benchmark_comparison_download_small
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "cat",
        "gcsfs",
        _gcsfs_cat_batch,
        (gcs, remote_dir, params.threads),
    )


@gcloud_required
@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_download_small",
    download_small_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_download_small_files_gcloud_cp(
    benchmark, gcsfs_benchmark_comparison_download_small, monitor
):
    _, remote_dir, local_dir, params = gcsfs_benchmark_comparison_download_small
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "cp_recursive",
        "gcloud",
        _gcloud_cp_batch,
        (f"gs://{remote_dir.rstrip('/')}/*", local_dir, params.threads),
    )


# 3. Upload Single Large File Tests


@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_upload_large",
    upload_large_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_upload_large_file_gcsfs_put(
    benchmark, gcsfs_benchmark_comparison_upload_large, monitor
):
    gcs, local_path, remote_path, params = gcsfs_benchmark_comparison_upload_large
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "put",
        "gcsfs",
        _gcsfs_put,
        (gcs, local_path, remote_path, params.chunk_size_bytes),
    )


@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_upload_large",
    upload_large_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_upload_large_file_gcsfs_pipe(
    benchmark, gcsfs_benchmark_comparison_upload_large, monitor
):
    gcs, local_path, remote_path, params = gcsfs_benchmark_comparison_upload_large
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "pipe",
        "gcsfs",
        _gcsfs_pipe,
        (gcs, local_path, remote_path, params.chunk_size_bytes),
    )


@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_upload_large",
    upload_large_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_upload_large_file_gcsfs_open_write(
    benchmark, gcsfs_benchmark_comparison_upload_large, monitor
):
    gcs, local_path, remote_path, params = gcsfs_benchmark_comparison_upload_large
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "open_write",
        "gcsfs",
        _gcsfs_open_write,
        (gcs, local_path, remote_path, params.chunk_size_bytes),
    )


@gcloud_required
@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_upload_large",
    upload_large_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_upload_large_file_gcloud_cp(
    benchmark, gcsfs_benchmark_comparison_upload_large, monitor
):
    _, local_path, remote_path, params = gcsfs_benchmark_comparison_upload_large
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "cp",
        "gcloud",
        _gcloud_cp,
        (local_path, f"gs://{remote_path}", params.threads),
    )


# 4. Upload Multiple Small Files Tests


@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_upload_small",
    upload_small_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_upload_small_files_gcsfs_put(
    benchmark, gcsfs_benchmark_comparison_upload_small, monitor
):
    gcs, local_dir, remote_dir, params = gcsfs_benchmark_comparison_upload_small
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "put",
        "gcsfs",
        _gcsfs_put_batch,
        (gcs, local_dir, remote_dir, params.threads),
    )


@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_upload_small",
    upload_small_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_upload_small_files_gcsfs_pipe(
    benchmark, gcsfs_benchmark_comparison_upload_small, monitor
):
    gcs, local_dir, remote_dir, params = gcsfs_benchmark_comparison_upload_small
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "pipe",
        "gcsfs",
        _gcsfs_pipe_batch,
        (gcs, local_dir, remote_dir),
    )


@gcloud_required
@pytest.mark.parametrize(
    "gcsfs_benchmark_comparison_upload_small",
    upload_small_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_upload_small_files_gcloud_cp(
    benchmark, gcsfs_benchmark_comparison_upload_small, monitor
):
    _, local_dir, remote_dir, params = gcsfs_benchmark_comparison_upload_small
    run_comparison_benchmark(
        benchmark,
        monitor,
        params,
        "cp_recursive",
        "gcloud",
        _gcloud_cp_batch,
        (
            os.path.join(local_dir, "*"),
            f"gs://{remote_dir.rstrip('/')}/",
            params.threads,
        ),
    )
