import os
import sys
from unittest import mock


class TestConditionalImport:
    def setup_method(self, method):
        """Setup for each test method."""
        self.original_env = os.environ.get("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT")

        # Snapshot original gcsfs modules
        self.original_modules = {
            name: mod for name, mod in sys.modules.items() if name.startswith("gcsfs")
        }

        # Unload gcsfs modules to force re-import during the test
        modules_to_remove = list(self.original_modules.keys())
        for name in modules_to_remove:
            if name in sys.modules:
                del sys.modules[name]

    def teardown_method(self, method):
        """Teardown after each test method."""
        # Reset environment variable to its original state
        if self.original_env is not None:
            os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = self.original_env
        elif "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT" in os.environ:
            del os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"]

        # Clear any gcsfs modules loaded/modified during this test
        modules_to_remove = [name for name in sys.modules if name.startswith("gcsfs")]
        for name in modules_to_remove:
            if name in sys.modules:
                del sys.modules[name]

        # Restore the original gcsfs modules from the snapshot to avoid side effect
        # affecting other tests
        sys.modules.update(self.original_modules)

    def test_experimental_env_is_set_by_default(self):
        """
        Tests gcsfs.GCSFileSystem is extended_gcsfs.ExtendedGcsFileSystem when
        GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT is NOT set and uses default value.
        """
        if "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT" in os.environ:
            del os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"]

        import gcsfs

        assert (
            gcsfs.GCSFileSystem is gcsfs.extended_gcsfs.ExtendedGcsFileSystem
        ), "Should be ExtendedGcsFileSystem"

    def test_experimental_env_set_to_true(self):
        """
        Tests gcsfs.GCSFileSystem is extended_gcsfs.ExtendedGcsFileSystem when
        GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT IS set to true.
        """
        os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = "true"

        import gcsfs

        assert (
            gcsfs.GCSFileSystem is gcsfs.extended_gcsfs.ExtendedGcsFileSystem
        ), "Should be ExtendedGcsFileSystem"

    def test_experimental_env_set_to_false(self):
        """
        Tests gcsfs.GCSFileSystem is core.GCSFileSystem when
        GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT IS set to false.
        """
        os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = "false"

        import gcsfs

        assert (
            gcsfs.GCSFileSystem is gcsfs.core.GCSFileSystem
        ), "Should be core.GCSFileSystem"
        assert not hasattr(
            gcsfs, "ExtendedGcsFileSystem"
        ), "ExtendedGcsFileSystem should not be imported directly on gcsfs"

    def test_version_exists(self):
        """
        Tests that __version__ is imported correctly
        when the _version module exists.
        """
        # Create a fake module that has a __version__ attribute
        mock_version_module = mock.MagicMock()
        mock_version_module.__version__ = "1.2.3"

        # Inject the fake module into sys.modules so 'from ._version import __version__' succeeds
        with mock.patch.dict("sys.modules", {"gcsfs._version": mock_version_module}):
            import gcsfs

            assert gcsfs.__version__ == "1.2.3"

    def test_version_fallback_metadata(self):
        """
        Tests that when _version.py is missing, the version is retrieved
        via importlib.metadata.version.
        """
        # Setting a module to None in sys.modules forces Python to raise a
        # ModuleNotFoundError (which subclasses ImportError) when it is imported.
        with mock.patch.dict("sys.modules", {"gcsfs._version": None}):
            with mock.patch("importlib.metadata.version", return_value="9.9.9"):
                import gcsfs

                assert gcsfs.__version__ == "9.9.9"

    def test_version_fallback_unknown(self):
        """
        Tests that when both _version.py is missing and metadata is unavailable,
        the version falls back to "unknown".
        """
        with mock.patch.dict("sys.modules", {"gcsfs._version": None}):
            # Simulate the package metadata not existing
            with mock.patch(
                "importlib.metadata.version", side_effect=ImportError("Not found")
            ):
                import gcsfs

                assert gcsfs.__version__ == "unknown"
