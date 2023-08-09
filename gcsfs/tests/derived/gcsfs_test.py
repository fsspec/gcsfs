import fsspec.tests.abstract as abstract

from gcsfs.tests.derived.gcsfs_fixtures import GcsfsFixtures


class TestGcsfsCopy(abstract.AbstractCopyTests, GcsfsFixtures):
    pass


class TestGcsfsGet(abstract.AbstractGetTests, GcsfsFixtures):
    pass


class TestGcsfsPut(abstract.AbstractPutTests, GcsfsFixtures):
    pass
