import unittest.mock as mock
from types import SimpleNamespace

import pytest

from gcsfs.extended_gcsfs import ExtendedGcsFileSystem
from gcsfs.tests import layouts
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
    "bucket_type, expected_rcu_rapid_write",
    [
        ("standard_rcu_z", True),
        ("hns_rcu_z", True),
        ("standard_rcu_warm", False),
        ("hns_rcu_cold", False),
        ("zonal", None),
        ("regional", None),
    ],
)
@pytest.mark.parametrize("helper", ["io", "put", "listing"])
def test_benchmark_fixtures_set_rcu_rapid_write_by_bucket_type(
    helper, bucket_type, expected_rcu_rapid_write
):
    """*_rcu_z enables rcu_rapid_write for RAPID writes; warm/cold force False for HTTP staging."""
    created = []

    def tracking_factory(**kwargs):
        created.append(kwargs)
        return mock.MagicMock(spec=ExtendedGcsFileSystem)

    params = SimpleNamespace(
        name=f"{helper}_{bucket_type}",
        bucket_name=TEST_BUCKET,
        bucket_type=bucket_type,
        files=1,
        folders=1,
        depth=0,
        file_size_bytes=0,
        rounds=1,
    )

    with (
        mock.patch.object(bench_conftest, "_prepare_files"),
        mock.patch.object(bench_conftest, "_warm_rcu_files"),
    ):
        if helper == "io":
            gen = bench_conftest._benchmark_io_fixture_helper(
                tracking_factory, params, "benchmark-io", create_files=True
            )
        elif helper == "put":
            gen = bench_conftest._benchmark_put_fixture_helper(
                tracking_factory, params, "benchmark-put"
            )
        else:
            gen = bench_conftest._benchmark_listing_fixture_helper(
                tracking_factory, params, "benchmark-listing"
            )
        next(gen)
        gen.close()

    if expected_rcu_rapid_write is None:
        assert "rcu_rapid_write" not in created[0]
    else:
        assert created[0]["rcu_rapid_write"] is expected_rcu_rapid_write


@pytest.mark.parametrize(
    "bucket_type, bucket_layout",
    [
        ("standard_rcu_warm", layouts.STANDARD_RCU),
        ("standard_rcu_cold", layouts.STANDARD_RCU),
        ("hns_rcu_warm", layouts.HIERARCHICAL_RCU),
        ("hns_rcu_cold", layouts.HIERARCHICAL_RCU),
    ],
)
def test_prepare_files_rcu_warm_and_cold_use_http_on_emulator(
    extended_gcs_factory, bucket_type, bucket_layout
):
    """*_rcu_warm and *_rcu_cold stage STANDARD objects over HTTP through ExtendedGcsFileSystem."""
    gcs = extended_gcs_factory(**bench_conftest.rcu_rapid_write_kwargs(bucket_type))
    paths = [f"{TEST_BUCKET}/rcu-http-{bucket_type}/file_{i}" for i in range(2)]

    try:
        try:
            with (
                mock.patch.object(
                    gcs,
                    "_get_bucket_layout",
                    new_callable=mock.AsyncMock,
                    return_value=bucket_layout,
                ),
                mock.patch(
                    "gcsfs.zb_hns_utils.init_aaow",
                    new_callable=mock.AsyncMock,
                    side_effect=AssertionError("gRPC appendable writer must not run"),
                ),
            ):
                bench_conftest._prepare_files(
                    gcs, paths, file_size=64, bucket_type=bucket_type
                )
        finally:
            gcs.invalidate_storage_layout_cache(TEST_BUCKET)

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
    with mock.patch(
        "gcsfs.zb_hns_utils.init_aaow",
        new_callable=mock.AsyncMock,
        side_effect=AssertionError("gRPC appendable writer must not run"),
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
        mock.patch(
            "gcsfs.zb_hns_utils.init_aaow",
            new_callable=mock.AsyncMock,
            side_effect=AssertionError("gRPC appendable writer must not run"),
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


@pytest.mark.parametrize(
    "bucket_type, file_size_bytes, primes",
    [
        ("standard_rcu_warm", 64, True),
        ("hns_rcu_cold", bench_conftest.SMALL_FILE_UPLOAD_LIMIT, True),
        ("standard_rcu_cold", 2 * bench_conftest.MB, False),
        ("standard_rcu_z", 64, False),
        ("zonal", 64, False),
        ("regional", 64, False),
    ],
)
def test_benchmark_io_fixture_primes_client_for_small_rcu_warm_and_cold_files(
    bucket_type, file_size_bytes, primes
):
    """Small *_rcu_warm/*_rcu_cold runs read a sentinel via gcs to warm its gRPC client."""
    gcs = mock.MagicMock(spec=bench_conftest.ExtendedGcsFileSystem)
    params = SimpleNamespace(
        name=f"cat_{bucket_type}",
        bucket_name="my-bucket",
        bucket_type=bucket_type,
        files=2,
        file_size_bytes=file_size_bytes,
        rounds=1,
    )

    with (
        mock.patch.object(bench_conftest, "_prepare_files"),
        mock.patch.object(bench_conftest, "_warm_rcu_files"),
    ):
        gen = bench_conftest._benchmark_io_fixture_helper(
            lambda **kwargs: gcs,
            params,
            "benchmark-prime",
            create_files=True,
        )
        _, file_paths, yielded_params = next(gen)
        try:
            assert len(gcs.pipe_file.call_args_list) == (1 if primes else 0)
            assert len(gcs.cat_file.call_args_list) == (1 if primes else 0)
            if primes:
                (pipe_path, payload), _ = gcs.pipe_file.call_args
                (cat_path,), _ = gcs.cat_file.call_args
                assert pipe_path.endswith("/_client_warmup")
                assert pipe_path == cat_path
                assert payload == b"\0"
                assert pipe_path not in file_paths
                all_round_paths = [
                    p for rp in yielded_params.round_file_paths for p in rp
                ]
                assert pipe_path not in all_round_paths
                op_names = [
                    c[0] for c in gcs.mock_calls if c[0] in ("pipe_file", "cat_file")
                ]
                assert op_names == ["pipe_file", "cat_file"]
        finally:
            with pytest.raises(StopIteration):
                next(gen)


@pytest.mark.parametrize(
    "bucket_type, bucket_layout",
    [
        ("standard_rcu_warm", layouts.STANDARD_RCU),
        ("hns_rcu_cold", layouts.HIERARCHICAL_RCU),
    ],
)
def test_benchmark_io_fixture_primes_grpc_client_on_extended_fs_for_small_rcu_warm_and_cold(
    bucket_type, bucket_layout
):
    """Fixture setup stages over HTTP and warms gcs's gRPC client via MRD without reading target files."""
    params = SimpleNamespace(
        name=f"cat_{bucket_type}",
        bucket_name="my-bucket",
        bucket_type=bucket_type,
        files=2,
        file_size_bytes=64,
        rounds=1,
    )
    fake_grpc_client = mock.MagicMock()
    fake_grpc_client.grpc_client.transport.close = mock.AsyncMock()
    fake_mrd = mock.MagicMock()
    fake_mrd.persisted_size = 1
    fake_mrd.is_stream_open = True
    fake_mrd.object_name = "warmup"
    fake_mrd.close = mock.AsyncMock()

    async def _fake_download_ranges(ranges):
        for _, length, buf in ranges:
            buf.write(b"\0" * length)

    fake_mrd.download_ranges = mock.AsyncMock(side_effect=_fake_download_ranges)

    async def _fake_http_call(*args, **kwargs):
        return {
            "name": "obj",
            "size": "1",
            "generation": "1",
            "timeFinalized": "2026-01-01T00:00:00Z",
        }

    def _make_fs(**kwargs):
        return ExtendedGcsFileSystem(token="anon", skip_instance_cache=True, **kwargs)

    with (
        mock.patch.object(
            ExtendedGcsFileSystem,
            "_get_bucket_layout",
            new_callable=mock.AsyncMock,
            return_value=bucket_layout,
        ),
        mock.patch.object(
            ExtendedGcsFileSystem,
            "_call",
            new_callable=mock.AsyncMock,
            side_effect=_fake_http_call,
        ),
        mock.patch.object(
            ExtendedGcsFileSystem,
            "_rm",
            new_callable=mock.AsyncMock,
        ),
        mock.patch(
            "gcsfs.extended_gcsfs.AsyncGrpcClient",
            return_value=fake_grpc_client,
        ) as grpc_client_cls,
        mock.patch(
            "gcsfs.zb_hns_utils.init_aaow",
            new_callable=mock.AsyncMock,
            side_effect=AssertionError("gRPC write must not run"),
        ) as init_aaow,
        mock.patch(
            "gcsfs.zb_hns_utils.init_mrd",
            new_callable=mock.AsyncMock,
            return_value=fake_mrd,
        ) as init_mrd,
        mock.patch.object(bench_conftest, "_warm_rcu_files"),
    ):
        gen = bench_conftest._benchmark_io_fixture_helper(
            _make_fs,
            params,
            "benchmark-prime",
            create_files=True,
            gcs_kwargs=bench_conftest.rcu_rapid_write_kwargs(bucket_type),
        )
        gcs, file_paths, yielded_params = next(gen)
        try:
            assert gcs._grpc_client is fake_grpc_client
            grpc_client_cls.assert_called_once()
            init_aaow.assert_not_awaited()
            init_mrd.assert_awaited_once()
            assert init_mrd.await_args is not None
            warmed_client, warmed_bucket, warmed_object, _ = init_mrd.await_args.args
            assert warmed_client is fake_grpc_client
            assert warmed_bucket == "my-bucket"
            assert warmed_object.endswith("/_client_warmup")
            for p in file_paths:
                assert not p.endswith(f"/{warmed_object}")
            assert len(yielded_params.round_file_paths[0]) == 2
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
