import pytest
from gcsfs.inventory_report import InventoryReport

class TestInventoryReport(object):
    """
    Unit tests for the inventory report logic, see 'inventory_report.py'.

    The test cases follow the same ordering as the methods in `inventory.report.py`.
    Each method is covered by either one or more parametrized test cases. Some
    methods include a setup method just above them.
    """

    @pytest.mark.parametrize("inventory_report_info, expected_error", [
        # Check whether missing inventory report info will raise exception.
        ({"location": "us-west", "id": "123"}, \
         "Use snapshot listing is not configured."),
        ({"use_snapshot_listing": True, "id": "123"}, \
         "Inventory report location is not configured."),
        # Check complete inventory report infor will not raise exception.
        ({"use_snapshot_listing": True, "location": "us-west"}, \
         "Inventory report id is not configured."),
        ({"use_snapshot_listing": True, "location": "us-west", "id": "123"}, None),
    ])
    def test_validate_inventory_report_info(
        self, inventory_report_info, expected_error):
        if expected_error is not None:
            with pytest.raises(ValueError) as e_info:
                InventoryReport._validate_inventory_report_info(
                    inventory_report_info=inventory_report_info)
                assert str(e_info.value) == expected_error
        else:
            # If no error is expected, we simply call the function
            # to ensure no exception is raised.
            InventoryReport._validate_inventory_report_info(
                inventory_report_info=inventory_report_info)