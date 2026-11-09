import datetime
import unittest.mock as mock
from types import SimpleNamespace

import google.oauth2.credentials
import pytest

from gcsfs.extended_gcsfs import ExtendedGcsFileSystem
from gcsfs.tests.perf.microbenchmarks import conftest as bench_conftest
from gcsfs.tests.perf.microbenchmarks.cat import test_cat as cat_bench
from gcsfs.tests.perf.microbenchmarks.cat_ranges import (
    test_cat_ranges as cat_ranges_bench,
)
from gcsfs.tests.perf.microbenchmarks.read import test_read as read_bench
from gcsfs.tests.settings import TEST_BUCKET


def _fake_benchmark():
    benchmark = mock.Mock()
    benchmark.extra_info = {}

    def _pedantic(target, setup=None, rounds=1, args=(), **kwargs):
        for _ in range(rounds):
            call_args = args
            call_kwargs = {}
            if setup is not None:
                maybe = setup()
                if maybe is not None:
                    call_args, call_kwargs = maybe
            target(*call_args, **call_kwargs)

    benchmark.pedantic.side_effect = _pedantic
    return benchmark


def _fake_monitor_cls():
    monitor = mock.Mock()
    monitor.__enter__ = mock.Mock(return_value=monitor)
    monitor.__exit__ = mock.Mock(return_value=None)
    monitor.max_cpu = 1.0
    monitor.max_mem = 10.0
    monitor.throughput_s = 5.0
    monitor.vcpus = 2
    return mock.Mock(return_value=monitor)


@pytest.mark.parametrize("bucket_type", ["standard_rcu_z", "hns_rcu_z"])
def test_prepare_files_rcu_z_skips_gcloud_and_uses_extended_gcs(bucket_type):
    """*_rcu_z must never use gcloud storage cp (which writes STANDARD instead of RAPID)."""
    large_size = 10240 * bench_conftest.MB
    mock_gcs = mock.MagicMock(spec=ExtendedGcsFileSystem)
    mock_pool = mock.MagicMock()
    mock_ctx = mock.MagicMock()
    mock_ctx.Pool.return_value.__enter__.return_value = mock_pool

    with (
        mock.patch.object(
            bench_conftest, "_prepare_files_gcloud", return_value=True
        ) as mock_gcloud,
        mock.patch.object(
            bench_conftest.multiprocessing, "get_context", return_value=mock_ctx
        ),
    ):
        bench_conftest._prepare_files(
            mock_gcs,
            ["bkt/large_0"],
            file_size=large_size,
            bucket_type=bucket_type,
        )

    mock_gcloud.assert_not_called()
    mock_pool.starmap.assert_called_once()
    uploaded_fs = mock_pool.starmap.call_args[0][1][0][0]
    assert uploaded_fs is mock_gcs


@pytest.mark.parametrize(
    "bucket_type",
    ["standard_rcu_warm", "standard_rcu_cold", "hns_rcu_warm", "hns_rcu_cold"],
)
def test_prepare_files_rcu_warm_and_cold_use_base_http_gcs_on_emulator(
    extended_gcs_factory, bucket_type
):
    """*_rcu_warm and *_rcu_cold must upload STANDARD objects via base HTTP GCSFileSystem."""
    gcs = extended_gcs_factory()
    paths = [f"{TEST_BUCKET}/rcu-http-{bucket_type}/file_{i}" for i in range(2)]

    try:
        with mock.patch.object(
            ExtendedGcsFileSystem,
            "_pipe_file",
            side_effect=AssertionError("ExtendedGcsFileSystem._pipe_file must not run"),
        ):
            bench_conftest._prepare_files(
                gcs, paths, file_size=64, bucket_type=bucket_type
            )

        for path in paths:
            assert len(gcs.cat_file(path)) == 64
    finally:
        if gcs.exists(f"{TEST_BUCKET}/rcu-http-{bucket_type}"):
            gcs.rm(f"{TEST_BUCKET}/rcu-http-{bucket_type}", recursive=True)


@pytest.mark.parametrize("bucket_type", ["standard_rcu_cold", "hns_rcu_cold"])
def test_benchmark_listing_fixture_rcu_cold_uses_http_upload(
    extended_gcs_factory, bucket_type
):
    """Metadata fixtures on *_rcu_cold must upload 0-byte objects over HTTP STANDARD."""
    params = SimpleNamespace(
        name=f"listing_{bucket_type}",
        bucket_name=TEST_BUCKET,
        bucket_type=bucket_type,
        files=2,
        folders=1,
        depth=0,
        file_size_bytes=0,
        rounds=1,
    )
    with mock.patch.object(
        ExtendedGcsFileSystem,
        "_pipe_file",
        side_effect=AssertionError("ExtendedGcsFileSystem._pipe_file must not run"),
    ):
        gen = bench_conftest._benchmark_listing_fixture_helper(
            extended_gcs_factory,
            params,
            "benchmark-listing-rcu-cold",
            teardown=True,
            require_file_paths=True,
        )
        gcs, _, file_paths, _, _ = next(gen)
        try:
            for path in file_paths:
                assert gcs.exists(path)
        finally:
            with pytest.raises(StopIteration):
                next(gen)


@pytest.mark.parametrize("bucket_type", ["standard_rcu_warm", "hns_rcu_warm"])
def test_benchmark_listing_fixture_rcu_warm_uploads_over_http_without_warmup(
    extended_gcs_factory, bucket_type
):
    """Metadata fixtures on *_rcu_warm upload over HTTP STANDARD without running RCU data warm-up."""
    created = []

    def tracking_factory(**kwargs):
        fs = extended_gcs_factory(**kwargs)
        created.append((kwargs, fs))
        return fs

    params = SimpleNamespace(
        name=f"listing_{bucket_type}",
        bucket_name=TEST_BUCKET,
        bucket_type=bucket_type,
        files=2,
        folders=1,
        depth=0,
        file_size_bytes=0,
        rounds=1,
    )

    with (
        mock.patch.object(
            ExtendedGcsFileSystem,
            "_pipe_file",
            side_effect=AssertionError("ExtendedGcsFileSystem._pipe_file must not run"),
        ),
        mock.patch(
            "gcsfs.tests.perf.microbenchmarks.conftest.time.sleep"
        ) as mock_sleep,
    ):
        gen = bench_conftest._benchmark_listing_fixture_helper(
            tracking_factory,
            params,
            "benchmark-listing-rcu-warm",
            teardown=True,
            require_file_paths=True,
        )
        yielded_gcs, _, file_paths, _, _ = next(gen)
        try:
            assert len(created) == 1
            _, bench_fs = created[0]
            assert bench_fs is yielded_gcs
            mock_sleep.assert_not_called()
            for path in file_paths:
                assert yielded_gcs.exists(path)
        finally:
            with pytest.raises(StopIteration):
                next(gen)


@pytest.mark.parametrize("bucket_type", ["standard_rcu_warm", "hns_rcu_warm"])
def test_benchmark_io_fixture_rcu_warm_reads_on_isolated_fs_closes_it_and_sleeps(
    extended_gcs_factory, bucket_type
):
    """*_rcu_warm warms every file on a separate skip_instance_cache fs, closes it, and sleeps."""
    created = []

    def tracking_factory(**kwargs):
        fs = extended_gcs_factory(**kwargs)
        created.append((kwargs, fs))
        return fs

    params = SimpleNamespace(
        name=f"read_{bucket_type}",
        bucket_name=TEST_BUCKET,
        bucket_type=bucket_type,
        files=2,
        file_size_bytes=128,
        rounds=2,
    )

    with (
        mock.patch.object(bench_conftest, "RCU_WARMUP_SETTLE_SECONDS", 12.5),
        mock.patch(
            "gcsfs.tests.perf.microbenchmarks.conftest.time.sleep"
        ) as mock_sleep,
    ):
        gen = bench_conftest._benchmark_io_fixture_helper(
            tracking_factory,
            params,
            "benchmark-rcu-warm",
            create_files=True,
        )
        yielded_gcs, file_paths, _ = next(gen)
        try:
            assert len(created) == 2
            bench_kwargs, bench_fs = created[0]
            warm_kwargs, warm_fs = created[1]
            assert bench_fs is yielded_gcs
            assert warm_fs is not yielded_gcs
            assert warm_kwargs.get("skip_instance_cache") is True
            # Warm-up fs must already be closed before timed rounds start.
            assert warm_fs._mrd_pool_cache._closed is True
            assert yielded_gcs._mrd_pool_cache._closed is False
            mock_sleep.assert_called_once_with(12.5)
            for path in file_paths:
                assert len(yielded_gcs.cat_file(path)) == 128
        finally:
            with pytest.raises(StopIteration):
                next(gen)


def test_benchmark_io_fixture_rcu_cold_stages_distinct_objects_per_round(
    extended_gcs_factory,
):
    """*_rcu_cold with rounds > 1 stages separate cold objects per round via HTTP rewrite."""
    params = SimpleNamespace(
        name="cat_standard_rcu_cold",
        bucket_name=TEST_BUCKET,
        bucket_type="standard_rcu_cold",
        files=2,
        file_size_bytes=48,
        rounds=3,
    )

    gen = bench_conftest._benchmark_io_fixture_helper(
        extended_gcs_factory,
        params,
        "benchmark-rcu-cold",
        create_files=True,
    )
    gcs, file_paths, yielded_params = next(gen)
    try:
        assert len(yielded_params.round_file_paths) == 3
        assert file_paths == yielded_params.round_file_paths[0]
        all_paths = [p for rp in yielded_params.round_file_paths for p in rp]
        assert len(set(all_paths)) == 6

        r0_bytes = [gcs.cat_file(p) for p in yielded_params.round_file_paths[0]]
        for round_idx in (1, 2):
            assert [
                gcs.cat_file(p) for p in yielded_params.round_file_paths[round_idx]
            ] == r0_bytes
    finally:
        with pytest.raises(StopIteration):
            next(gen)

    for p in all_paths:
        assert not gcs.exists(p)


def test_read_cat_and_cat_ranges_rotate_rcu_cold_files_each_round(extended_gcs_factory):
    """read, cat, and cat_ranges must read each round's staged object paths on *_rcu_cold."""
    # 1. cat single-threaded & multi-threaded
    cat_params = SimpleNamespace(
        name="cat_cold",
        bucket_name=TEST_BUCKET,
        bucket_type="standard_rcu_cold",
        files=2,
        file_size_bytes=32,
        chunk_size_bytes=32,
        threads=1,
        processes=1,
        rounds=3,
        pattern="whole",
        concurrency=None,
    )
    gen = bench_conftest._benchmark_io_fixture_helper(
        extended_gcs_factory, cat_params, "bench-cat-cold", create_files=True
    )
    gcs, file_paths, cat_params = next(gen)
    try:
        cat_seen = []
        orig_cat_file = gcs.cat_file
        with mock.patch.object(
            gcs,
            "cat_file",
            side_effect=lambda p, *a, **kw: (
                cat_seen.append(p),
                orig_cat_file(p, *a, **kw),
            )[1],
        ):
            cat_bench.test_cat_single_threaded(
                _fake_benchmark(), (gcs, file_paths, cat_params), _fake_monitor_cls()
            )
        assert [
            cat_seen[0:2],
            cat_seen[2:4],
            cat_seen[4:6],
        ] == cat_params.round_file_paths

        cat_mt_seen = []
        cat_params.threads = 2
        with mock.patch.object(
            gcs,
            "cat_file",
            side_effect=lambda p, *a, **kw: (
                cat_mt_seen.append(p),
                orig_cat_file(p, *a, **kw),
            )[1],
        ):
            cat_bench.test_cat_multi_threaded(
                _fake_benchmark(), (gcs, file_paths, cat_params), _fake_monitor_cls()
            )
        assert [
            sorted(cat_mt_seen[0:2]),
            sorted(cat_mt_seen[2:4]),
            sorted(cat_mt_seen[4:6]),
        ] == [sorted(rp) for rp in cat_params.round_file_paths]
    finally:
        with pytest.raises(StopIteration):
            next(gen)

    # 2. cat_ranges single-threaded
    cr_params = SimpleNamespace(
        name="cat_ranges_cold",
        bucket_name=TEST_BUCKET,
        bucket_type="hns_rcu_cold",
        files=1,
        file_size_bytes=64,
        chunk_size_bytes=16,
        chunk_sizes_bytes=[16],
        num_ranges=2,
        max_gap=None,
        batch_size=None,
        threads=1,
        processes=1,
        rounds=3,
        pattern="seq",
    )
    gen_cr = bench_conftest._benchmark_io_fixture_helper(
        extended_gcs_factory, cr_params, "bench-cr-cold", create_files=True
    )
    gcs_cr, cr_paths, cr_params = next(gen_cr)
    try:
        cr_seen_rounds = []
        orig_cat_ranges = gcs_cr.cat_ranges
        with mock.patch.object(
            gcs_cr,
            "cat_ranges",
            side_effect=lambda paths, *a, **kw: (
                cr_seen_rounds.append(sorted(set(paths))),
                orig_cat_ranges(paths, *a, **kw),
            )[1],
        ):
            cat_ranges_bench.test_cat_ranges_single_threaded(
                _fake_benchmark(),
                (gcs_cr, cr_paths, cr_params),
                _fake_monitor_cls(),
            )
        assert cr_seen_rounds == cr_params.round_file_paths
    finally:
        with pytest.raises(StopIteration):
            next(gen_cr)

    # 3. read single-threaded & multi-threaded
    read_params = SimpleNamespace(
        name="read_cold",
        bucket_name=TEST_BUCKET,
        bucket_type="standard_rcu_cold",
        files=1,
        file_size_bytes=64,
        chunk_size_bytes=32,
        block_size_bytes=64,
        mrd_pool_size=None,
        min_chunk_size_bytes=0,
        max_chunk_size_bytes=0,
        seq_probability=None,
        runtime=0.02,
        threads=1,
        processes=1,
        rounds=2,
        pattern="seq",
    )
    gen_r = bench_conftest._benchmark_io_fixture_helper(
        extended_gcs_factory, read_params, "bench-read-cold", create_files=True
    )
    gcs_r, r_paths, read_params = next(gen_r)
    try:
        opened_st = []
        orig_open = gcs_r.open

        def spy_open_st(path, *a, **kw):
            opened_st.append(path)
            return orig_open(path, *a, **kw)

        with mock.patch.object(gcs_r, "open", side_effect=spy_open_st):
            read_bench.test_read_single_threaded(
                _fake_benchmark(), (gcs_r, r_paths, read_params), _fake_monitor_cls()
            )
        seen_unique_st = list(dict.fromkeys(opened_st))
        assert seen_unique_st == [rp[0] for rp in read_params.round_file_paths]

        opened_mt = []
        read_params.threads = 2

        def spy_open_mt(path, *a, **kw):
            opened_mt.append(path)
            return orig_open(path, *a, **kw)

        with mock.patch.object(gcs_r, "open", side_effect=spy_open_mt):
            read_bench.test_read_multi_threaded(
                _fake_benchmark(), (gcs_r, r_paths, read_params), _fake_monitor_cls()
            )
        seen_unique_mt = list(dict.fromkeys(opened_mt))
        assert seen_unique_mt == [rp[0] for rp in read_params.round_file_paths]

        # 4. read multi-process
        read_params.threads = 1
        read_params.processes = 2
        mp_paths_per_spawn = []
        mock_ctx = mock.Mock()
        mock_proc = mock.Mock(exitcode=0)

        def capture_process(target, args):
            worker_args = args[1]
            mp_paths_per_spawn.append(worker_args[1])
            return mock_proc

        mock_ctx.Process.side_effect = capture_process

        class MockArray:
            def __setitem__(self, idx, val):
                pass

            def __getitem__(self, idx):
                if isinstance(idx, slice):
                    return [16.0, 16.0]
                return 16.0

        mock_ctx.Array.return_value = MockArray()
        with mock.patch(
            "gcsfs.tests.perf.microbenchmarks.runner.multiprocessing.get_context",
            return_value=mock_ctx,
        ):
            read_bench.test_read_multi_process(
                _fake_benchmark(),
                (gcs_r, r_paths, read_params),
                mock.Mock(return_value="worker_gcs"),
                None,
                _fake_monitor_cls(),
            )
        assert mp_paths_per_spawn == [
            read_params.round_file_paths[0],
            read_params.round_file_paths[0],
            read_params.round_file_paths[1],
            read_params.round_file_paths[1],
        ]
    finally:
        with pytest.raises(StopIteration):
            next(gen_r)


def test_http_gcs_from_reuses_authenticated_credentials_without_reconnecting(
    extended_gcs_factory,
):
    """_http_gcs_from must reuse gcs.credentials.credentials rather than re-running connect(None)."""
    gcs = extended_gcs_factory()
    creds_obj = google.oauth2.credentials.Credentials(
        "test-access-token", expiry=datetime.datetime(2099, 1, 1)
    )
    gcs.credentials.token = None
    gcs.credentials.method = "cloud"
    gcs.credentials.credentials = creds_obj
    gcs.credentials.project = "project"

    http_fs = None
    try:
        with mock.patch(
            "google.auth.default",
            side_effect=AssertionError("must not re-run google.auth.default"),
        ):
            http_fs = bench_conftest._http_gcs_from(gcs)
        assert http_fs.credentials.credentials is creds_obj
    finally:
        if http_fs is not None:
            bench_conftest._close_gcs(http_fs)
