"""Bucket layouts for tests; production code only needs ``BucketLayout.UNKNOWN``."""

from gcsfs.extended_gcsfs import (
    BucketLayout,
    CachingTier,
    NamespaceLayout,
    StorageClass,
)

ZONAL_HIERARCHICAL = BucketLayout(
    StorageClass.RAPID, CachingTier.NONE, NamespaceLayout.HIERARCHICAL
)
STANDARD_HIERARCHICAL = BucketLayout(
    StorageClass.STANDARD, CachingTier.NONE, NamespaceLayout.HIERARCHICAL
)
STANDARD = BucketLayout(
    StorageClass.STANDARD, CachingTier.NONE, NamespaceLayout.NON_HIERARCHICAL
)
STANDARD_RCU = BucketLayout(
    StorageClass.STANDARD,
    CachingTier.RAPID_CACHE_ULTRA,
    NamespaceLayout.NON_HIERARCHICAL,
)
HIERARCHICAL_RCU = BucketLayout(
    StorageClass.STANDARD,
    CachingTier.RAPID_CACHE_ULTRA,
    NamespaceLayout.HIERARCHICAL,
)
STANDARD_RAPID_CACHE = BucketLayout(
    StorageClass.STANDARD, CachingTier.RAPID_CACHE, NamespaceLayout.NON_HIERARCHICAL
)
HIERARCHICAL_RAPID_CACHE = BucketLayout(
    StorageClass.STANDARD, CachingTier.RAPID_CACHE, NamespaceLayout.HIERARCHICAL
)

ALL_BUCKET_LAYOUTS = (
    ZONAL_HIERARCHICAL,
    STANDARD_HIERARCHICAL,
    STANDARD,
    STANDARD_RCU,
    HIERARCHICAL_RCU,
    STANDARD_RAPID_CACHE,
    HIERARCHICAL_RAPID_CACHE,
    BucketLayout.UNKNOWN,
)
