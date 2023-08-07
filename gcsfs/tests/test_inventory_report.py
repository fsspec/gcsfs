import pytest
import asyncio
from datetime import datetime, timedelta

from gcsfs.core import GCSFileSystem
from gcsfs.inventory_report import InventoryReport, InventoryReportConfig

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
            
    @pytest.mark.asyncio
    @pytest.mark.parametrize("location, id, exception, expected_result", [
    # Test no error fetching proceeds normally.
    ("us-west", "id1", None, {"config": "config1"}), 
    # Test if the exception is caught successfully.
    ("us-west", "id2", Exception("fetch error"), None),
    ])
    async def test_fetch_raw_inventory_report_config(
        self, location, id, exception, expected_result, mocker):

        # Mocking the gcs_file_system.
        gcs_file_system = mocker.MagicMock()
        gcs_file_system.project = "project"
        
        # Mocking gcs_file_system._call.
        if exception is not None:
            gcs_file_system._call = mocker.MagicMock(side_effect=exception)
        else:
            return_value = asyncio.Future()
            return_value.set_result(expected_result)
            gcs_file_system._call = mocker.MagicMock(return_value=return_value)

        if exception is not None:
            with pytest.raises(Exception) as e_info:
                await InventoryReport._fetch_raw_inventory_report_config(
                    gcs_file_system=gcs_file_system,
                    location=location,
                    id=id)
                assert str(e_info.value) == str(exception)
        else:
            result = await InventoryReport._fetch_raw_inventory_report_config(
                gcs_file_system=gcs_file_system,
                location=location,
                id=id)
            gcs_file_system._call.assert_called_once_with(
                "GET", mocker.ANY, json_out=True)
            assert result == expected_result
    
    def test_parse_raw_inventory_report_config_invalid_date(self, mocker):

        today = datetime.today().date()

        # Get tomorrow's date.
        tomorrow = today + timedelta(days=1)

        # Get the date a week later.
        next_week = today + timedelta(days=7)

        raw_inventory_report_config = {
            "frequencyOptions": {
                "startDate": {
                    "day": tomorrow.day,
                    "month": tomorrow.month,
                    "year": tomorrow.year
                },
                "endDate": {
                    "day": next_week.day,
                    "month": next_week.month,
                    "year": next_week.year
                }
            },
            "objectMetadataReportOptions": mocker.MagicMock(),
            "csvOptions": mocker.MagicMock()
        }

        # If the current date is outside the ranges in the inventory report
        # an exception should be raised.
        with pytest.raises(ValueError):
            InventoryReport._parse_raw_inventory_report_config(
                raw_inventory_report_config=raw_inventory_report_config,
                use_snapshot_listing=mocker.MagicMock())
    
    def test_parse_raw_inventory_report_config_missing_metadata_fields(
            self, mocker):

        raw_inventory_report_config = {
            "frequencyOptions": mocker.MagicMock(),
            "objectMetadataReportOptions": {
                "metadataFields": ["project", "bucket", "name"],
                "storageDestinationOptions": mocker.MagicMock()
            },
              "csvOptions": mocker.MagicMock()
        }

        # When the user wants to use snapshot listing, but object size is not
        # included in the inventory reports, an exception should be raised.
        with pytest.raises(ValueError):
            InventoryReport._parse_raw_inventory_report_config(
                raw_inventory_report_config=raw_inventory_report_config,
                use_snapshot_listing=True)
    
    def test_parse_raw_inventory_report_config_returns_correct_config(self):

        bucket = "bucket"
        destination_path = "path/to/inventory-report"
        metadata_fields = ["project", "bucket", "name", "size"]
        obj_name_idx = metadata_fields.index("name")
        today = datetime.today().date()
        yesterday = today - timedelta(days=1)
        tomorrow = today + timedelta(days=1)
        use_snapshot_listing = False

        csv_options = {
                "recordSeparator": "\n",
                "delimiter": ",",
                "headerRequired": False
        }

        raw_inventory_report_config = {
            "frequencyOptions": {
                "startDate": {
                    "day": yesterday.day,
                    "month": yesterday.month,
                    "year": yesterday.year
                },
                "endDate": {
                    "day": tomorrow.day,
                    "month": tomorrow.month,
                    "year": tomorrow.year
                }
            },
            "objectMetadataReportOptions": {
                "metadataFields": metadata_fields,
                "storageDestinationOptions": {
                    "bucket": bucket,
                    "destinationPath": destination_path
                }
            },
            "csvOptions": csv_options
        }

        try:
            inventory_report_config = InventoryReport. \
                _parse_raw_inventory_report_config(
                raw_inventory_report_config=raw_inventory_report_config,
                use_snapshot_listing=use_snapshot_listing)
            
            assert isinstance(inventory_report_config, InventoryReportConfig)

            assert inventory_report_config.csv_options == csv_options
            assert inventory_report_config.bucket == bucket
            assert inventory_report_config.destination_path == destination_path
            assert inventory_report_config.metadata_fields == metadata_fields
            assert inventory_report_config.obj_name_idx == obj_name_idx

        except Exception as e:
            pytest.fail(f"Unexpected exception: {e}.")

    @pytest.mark.asyncio
    async def test_fetch_inventory_report_metadata_no_reports(self, mocker):
        
        # Create a mock for GCSFileSystem.
        gcs_file_system = mocker.MagicMock(spec=GCSFileSystem)

        # Mock the _call method to return a page with two items
        # and then a page with one item and without next page token.
        gcs_file_system._call.side_effect = [{"items": [], "nextPageToken": None}]

        # Create a mock for InventoryReportConfig.
        inventory_report_config = mocker.MagicMock(spec=InventoryReportConfig)
        inventory_report_config.bucket = "bucket_name"
        inventory_report_config.destination_path = "destination_path"

        # If no inventory report metadata is fetched, an exception should be raised.
        with pytest.raises(ValueError) as e_info:
            await InventoryReport._fetch_inventory_report_metadata(
                gcs_file_system=gcs_file_system, 
                inventory_report_config=inventory_report_config)
            assert e_info.value == "No inventory reports to fetch. \
                Check if your inventory report is set up correctly."

    @pytest.mark.asyncio
    async def test_fetch_inventory_report_metadata_multiple_calls(self, mocker):

        # Create a mock for GCSFileSystem.
        gcs_file_system = mocker.MagicMock(spec=GCSFileSystem)

        # Mock the _call method to return a page with two items
        # and then a page with one item and without next page token.
        gcs_file_system._call.side_effect = [{"items": ["item1", "item2"], \
            "nextPageToken": "token1"}, {"items": ["item3"], "nextPageToken": None}]

        # Create a mock for InventoryReportConfig.
        inventory_report_config = mocker.MagicMock(spec=InventoryReportConfig)
        inventory_report_config.bucket = "bucket_name"
        inventory_report_config.destination_path = "destination_path"

        result = await InventoryReport._fetch_inventory_report_metadata(
            gcs_file_system=gcs_file_system,
            inventory_report_config=inventory_report_config)

        # Check that _call was called with the right arguments.
        calls = [mocker.call("GET", "b/{}/o", 'bucket_name',
                            prefix='destination_path', json_out=True),
                mocker.call("GET", "b/{}/o", 'bucket_name',
                    prefix='destination_path', pageToken="token1", json_out=True)]
        gcs_file_system._call.assert_has_calls(calls)

        # Check that the function correctly processed the response
        # and returned the right result.
        assert result == ["item1", "item2", "item3"]
    
    @pytest.mark.parametrize("unsorted_inventory_report_metadata, expected", [
    (
        # Input.
        [
            {"timeCreated": "2023-08-01T12:00:00Z"},
            {"timeCreated": "2023-08-02T12:00:00Z"},
            {"timeCreated": "2023-08-03T12:00:00Z"},
        ],
        # Expected output.
        [
            {"timeCreated": "2023-08-03T12:00:00Z"},
            {"timeCreated": "2023-08-02T12:00:00Z"},
            {"timeCreated": "2023-08-01T12:00:00Z"},
        ],
    ),
    (
        # Input.
        [
            {"timeCreated": "2023-08-01T12:00:00Z"},
            {"timeCreated": "2023-07-31T12:00:00Z"},
            {"timeCreated": "2023-08-02T12:00:00Z"},
        ],
        # Expected output.
        [
            {"timeCreated": "2023-08-02T12:00:00Z"},
            {"timeCreated": "2023-08-01T12:00:00Z"},
            {"timeCreated": "2023-07-31T12:00:00Z"},
        ],
    )])
    def test_sort_inventory_report_metadata(
        self, unsorted_inventory_report_metadata, expected):
        result = InventoryReport._sort_inventory_report_metadata(
            unsorted_inventory_report_metadata=unsorted_inventory_report_metadata)
        assert result == expected




# Test fields of the inventory report config is correctly stored.
class TestInventoryReportConfig:
    def test_inventory_report_config_creation(self):

        csv_options = {}
        bucket = "bucket"
        destination_path = ""
        metadata_fields = []
        obj_name_idx = 0

        inventory_report_config = InventoryReportConfig(
            csv_options=csv_options,
            bucket=bucket,
            destination_path=destination_path,
            metadata_fields=metadata_fields,
            obj_name_idx=obj_name_idx
        )
    
        assert inventory_report_config.csv_options == csv_options
        assert inventory_report_config.bucket == bucket
        assert inventory_report_config.destination_path == destination_path
        assert inventory_report_config.metadata_fields == metadata_fields
        assert inventory_report_config.obj_name_idx == obj_name_idx