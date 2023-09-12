import gcsfs.checkers
import gcsfs.tests.settings
from gcsfs.inventory_report import InventoryReport

TEST_BUCKET = gcsfs.tests.settings.TEST_BUCKET


# Basic integration test to ensure listing returns the correct result.
def test_ls_base(monkeypatch, gcs):
    # First get results from original listing.
    items = gcs.ls(TEST_BUCKET)

    async def mock_fetch_snapshot(*args, **kwargs):
        return [{"name": item} for item in items], []

    # Patch the fetch_snapshot method with the replacement.
    monkeypatch.setattr(InventoryReport, "fetch_snapshot", mock_fetch_snapshot)

    inventory_report_info = {
        "location": "location",
        "id": "id",
        "use_snapshot_listing": False,
    }

    # Then get results from listing with inventory report.
    actual_items = gcs.ls(TEST_BUCKET, inventory_report_info=inventory_report_info)

    # Check equality.
    assert actual_items == items
