"""Unit tests for the default :class:`gcsfs._dircache.DirCacheUpdater` strategy.

These cover the base ``dircache`` update behavior mixed into
:class:`gcsfs.core.GCSFileSystem`: broad ancestor invalidation for deletes and
moves, and the single-level shortcut (invalidate only an already-cached parent)
for writes. The HNS-specific overrides for deletes and moves live in
:class:`gcsfs._dircache.HnsDirCacheUpdater` and are exercised in
``test_extended_hns_gcsfs.py``.

Test placement: keep pure in-memory dircache strategy tests here. Filesystem
integration and bucket-specific routing tests belong in test_core.py,
test_extended_hns_gcsfs.py, integration/test_extended_hns.py, or
test_zonal_file.py according to the bucket type they exercise.

The methods under test are pure in-memory operations on ``self.dircache`` (via
``invalidate_cache`` / ``_parent``), so no GCS backend is required.
"""

import pytest

from gcsfs.core import GCSFileSystem

BUCKET = "dircache-test-bucket"


def _fs():
    return GCSFileSystem(token="anon", skip_instance_cache=True)


def _seed_tree(fs):
    """Cache a small tree: bucket / dir / sub, each with one listing."""
    fs.dircache[BUCKET] = [{"name": f"{BUCKET}/dir", "type": "directory"}]
    fs.dircache[f"{BUCKET}/dir"] = [
        {"name": f"{BUCKET}/dir/sub", "type": "directory"},
        {"name": f"{BUCKET}/dir/f.txt", "type": "file"},
    ]
    fs.dircache[f"{BUCKET}/dir/sub"] = [
        {"name": f"{BUCKET}/dir/sub/g.txt", "type": "file"}
    ]


class TestDirCacheUpdaterWrite:
    @pytest.mark.asyncio
    async def test_write_with_cached_parent_invalidates_only_immediate_parent(self):
        """When the immediate parent is already cached (so it is known to exist),
        a write invalidates only that parent; ancestor caches stay intact because
        adding a file cannot create a new directory in any ancestor's listing."""
        fs = _fs()
        _seed_tree(fs)

        # Parent (BUCKET/dir/sub) is cached.
        await fs._write_file_cache_update(f"{BUCKET}/dir/sub/new.txt")

        assert f"{BUCKET}/dir/sub" not in fs.dircache
        assert f"{BUCKET}/dir" in fs.dircache
        assert BUCKET in fs.dircache

    @pytest.mark.asyncio
    async def test_write_with_uncached_parent_invalidates_ancestors(self):
        """When the immediate parent is not cached, the write may have implicitly
        created it (and intermediate directories), so the parent and every cached
        ancestor are invalidated to avoid hiding the new directory."""
        fs = _fs()
        _seed_tree(fs)

        # Parent (BUCKET/dir/sub/deeper) is NOT cached; its ancestors are.
        await fs._write_file_cache_update(f"{BUCKET}/dir/sub/deeper/new.txt")

        assert f"{BUCKET}/dir/sub" not in fs.dircache
        assert f"{BUCKET}/dir" not in fs.dircache
        assert BUCKET not in fs.dircache


class TestDirCacheUpdaterRm:
    @pytest.mark.asyncio
    async def test_rm_file_invalidates_parent_chain(self):
        fs = _fs()
        _seed_tree(fs)

        await fs._rm_file_cache_update(f"{BUCKET}/dir/sub/g.txt")

        # _rm_file_cache_update delegates to the batch form; the deleted file's
        # parent and ancestors are invalidated.
        assert f"{BUCKET}/dir/sub" not in fs.dircache
        assert f"{BUCKET}/dir" not in fs.dircache
        assert BUCKET not in fs.dircache

    @pytest.mark.asyncio
    async def test_rm_files_invalidates_each_path_and_its_ancestors(self):
        fs = _fs()
        _seed_tree(fs)
        # An unrelated sibling subtree that must be left untouched.
        other = f"{BUCKET}/other"
        fs.dircache[other] = [{"name": f"{other}/h.txt", "type": "file"}]

        await fs._rm_files_cache_update([f"{BUCKET}/dir/sub"])

        # The path itself, its parent, and ancestors are invalidated...
        assert f"{BUCKET}/dir/sub" not in fs.dircache
        assert f"{BUCKET}/dir" not in fs.dircache
        assert BUCKET not in fs.dircache
        # ...but an unrelated subtree is preserved.
        assert other in fs.dircache


class TestDirCacheUpdaterMv:
    @pytest.mark.asyncio
    async def test_mv_invalidates_both_source_and_dest_parents(self):
        fs = _fs()
        src_parent = f"{BUCKET}/srcdir"
        dst_parent = f"{BUCKET}/dstdir"
        fs.dircache[src_parent] = [{"name": f"{src_parent}/a.txt", "type": "file"}]
        fs.dircache[dst_parent] = [{"name": f"{dst_parent}/b.txt", "type": "file"}]

        await fs._mv_file_cache_update(f"{src_parent}/a.txt", f"{dst_parent}/a.txt")

        assert src_parent not in fs.dircache
        assert dst_parent not in fs.dircache


class TestDirCacheUpdaterMkdirRmdir:
    @pytest.mark.asyncio
    async def test_mkdir_invalidates_root_cache(self):
        fs = _fs()
        # Seed the root cache (list of buckets)
        fs.dircache[""] = [{"name": "bucket1", "type": "directory"}]

        # Mock _call to prevent actual network requests
        async def mock_call(*args, **kwargs):
            return {"name": "new-bucket"}

        fs._call = mock_call

        await fs._mkdir("new-bucket")

        # The root cache must be invalidated because a new bucket was created
        assert "" not in fs.dircache

    @pytest.mark.asyncio
    async def test_rmdir_invalidates_root_cache(self):
        fs = _fs()
        # Seed the root cache (list of buckets)
        fs.dircache[""] = [{"name": "bucket1", "type": "directory"}]

        # Mock _call to prevent actual network requests
        async def mock_call(*args, **kwargs):
            return {}

        fs._call = mock_call

        await fs._rmdir("bucket1")

        # The root cache must be invalidated because a bucket was removed
        assert "" not in fs.dircache
