import os
import sys


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

    def test_experimental_env_unset(self):
        """
        Tests gcsfs.GCSFileSystem is core.GCSFileSystem when
        GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT is NOT set.
        """
        if "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT" in os.environ:
            del os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"]

        import gcsfs

        assert (
            gcsfs.GCSFileSystem is gcsfs.core.GCSFileSystem
        ), "Should be core.GCSFileSystem"
        assert not hasattr(
            gcsfs, "ExtendedGcsFileSystem"
        ), "ExtendedGcsFileSystem should not be imported directly on gcsfs"

    def test_experimental_env_set(self):
        """
        Tests gcsfs.GCSFileSystem is extended_gcsfs.ExtendedGcsFileSystem when
        GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT IS set.
        """
        os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = "true"

        import gcsfs

        assert (
            gcsfs.GCSFileSystem is gcsfs.extended_gcsfs.ExtendedGcsFileSystem
        ), "Should be ExtendedGcsFileSystem"
