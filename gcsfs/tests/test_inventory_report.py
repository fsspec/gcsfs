import asyncio
from datetime import datetime, timedelta
from unittest import mock

import pytest

from gcsfs.core import GCSFileSystem
from gcsfs.inventory_report import InventoryReport, InventoryReportConfig


class TestInventoryReport(object):
    """
    Unit tests for the inventory report logic, see 'inventory_report.py'.

    The test cases follow the same ordering as the methods in `inventory.report.py`.
    Each method is covered by either one or more parametrized test cases. Some
    methods include a setup method just above them.
    """

    @pytest.mark.parametrize(
        "inventory_report_info, expected_error",
        [
            # Check whether missing inventory report info will raise exception.
            (
                {"location": "us-west", "id": "123"},
                "Use snapshot listing is not configured.",
            ),
            (
                {"use_snapshot_listing": True, "id": "123"},
                "Inventory report location is not configured.",
            ),
            (
                {"use_snapshot_listing": True, "location": "us-west"},
                "Inventory report id is not configured.",
            ),
            # Check complete inventory report info will not raise exception.
            ({"use_snapshot_listing": True, "location": "us-west", "id": "123"}, None),
        ],
    )
    def test_validate_inventory_report_info(
        self, inventory_report_info, expected_error
    ):
        if expected_error is not None:
            with pytest.raises(ValueError) as e_info:
                InventoryReport._validate_inventory_report_info(
                    inventory_report_info=inventory_report_info
                )
                assert str(e_info.value) == expected_error
        else:
            # If no error is expected, we simply call the function
            # to ensure no exception is raised.
            InventoryReport._validate_inventory_report_info(
                inventory_report_info=inventory_report_info
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "location, id, exception, expected_result",
        [
            # Test no error fetching proceeds normally.
            ("us-west", "id1", None, {"config": "config1"}),
            # Test if the exception is caught successfully.
            ("us-west", "id2", Exception("fetch error"), None),
        ],
    )
    async def test_fetch_raw_inventory_report_config(
        self, location, id, exception, expected_result
    ):
        # Mocking the gcs_file_system.
        gcs_file_system = mock.MagicMock()
        gcs_file_system.project = "project"

        # Mocking gcs_file_system._call.
        if exception is not None:
            gcs_file_system._call = mock.MagicMock(side_effect=exception)
        else:
            return_value = asyncio.Future()
            return_value.set_result(expected_result)
            gcs_file_system._call = mock.MagicMock(return_value=return_value)

        if exception is not None:
            with pytest.raises(Exception) as e_info:
                await InventoryReport._fetch_raw_inventory_report_config(
                    gcs_file_system=gcs_file_system, location=location, id=id
                )
                assert str(e_info.value) == str(exception)
        else:
            result = await InventoryReport._fetch_raw_inventory_report_config(
                gcs_file_system=gcs_file_system, location=location, id=id
            )
            gcs_file_system._call.assert_called_once_with(
                "GET", mock.ANY, json_out=True
            )
            assert result == expected_result

    def test_parse_raw_inventory_report_config_invalid_date(self):
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
                    "year": tomorrow.year,
                },
                "endDate": {
                    "day": next_week.day,
                    "month": next_week.month,
                    "year": next_week.year,
                },
            },
            "objectMetadataReportOptions": mock.MagicMock(),
            "csvOptions": mock.MagicMock(),
        }

        # If the current date is outside the ranges in the inventory report
        # an exception should be raised.
        with pytest.raises(ValueError):
            InventoryReport._parse_raw_inventory_report_config(
                raw_inventory_report_config=raw_inventory_report_config,
                use_snapshot_listing=mock.MagicMock(),
            )

    def test_parse_raw_inventory_report_config_missing_metadata_fields(self):
        raw_inventory_report_config = {
            "frequencyOptions": mock.MagicMock(),
            "objectMetadataReportOptions": {
                "metadataFields": ["project", "bucket", "name"],
                "storageDestinationOptions": mock.MagicMock(),
            },
            "csvOptions": mock.MagicMock(),
        }

        # When the user wants to use snapshot listing, but object size is not
        # included in the inventory reports, an exception should be raised.
        with pytest.raises(ValueError):
            InventoryReport._parse_raw_inventory_report_config(
                raw_inventory_report_config=raw_inventory_report_config,
                use_snapshot_listing=True,
            )

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
            "headerRequired": False,
        }

        raw_inventory_report_config = {
            "frequencyOptions": {
                "startDate": {
                    "day": yesterday.day,
                    "month": yesterday.month,
                    "year": yesterday.year,
                },
                "endDate": {
                    "day": tomorrow.day,
                    "month": tomorrow.month,
                    "year": tomorrow.year,
                },
            },
            "objectMetadataReportOptions": {
                "metadataFields": metadata_fields,
                "storageDestinationOptions": {
                    "bucket": bucket,
                    "destinationPath": destination_path,
                },
            },
            "csvOptions": csv_options,
        }

        try:
            inventory_report_config = (
                InventoryReport._parse_raw_inventory_report_config(
                    raw_inventory_report_config=raw_inventory_report_config,
                    use_snapshot_listing=use_snapshot_listing,
                )
            )

            assert isinstance(inventory_report_config, InventoryReportConfig)

            assert inventory_report_config.csv_options == csv_options
            assert inventory_report_config.bucket == bucket
            assert inventory_report_config.destination_path == destination_path
            assert inventory_report_config.metadata_fields == metadata_fields
            assert inventory_report_config.obj_name_idx == obj_name_idx

        except Exception as e:
            pytest.fail(f"Unexpected exception: {e}.")

    @pytest.mark.asyncio
    async def test_fetch_inventory_report_metadata_no_reports(self):
        # Create a mock for GCSFileSystem.
        gcs_file_system = mock.MagicMock(spec=GCSFileSystem)

        # Mock the _call method to return a page with two items
        # and then a page with one item and without next page token.
        gcs_file_system._call.side_effect = [{"items": [], "nextPageToken": None}]

        # Create a mock for InventoryReportConfig.
        inventory_report_config = mock.MagicMock(spec=InventoryReportConfig)
        inventory_report_config.bucket = "bucket_name"
        inventory_report_config.destination_path = "destination_path"

        # If no inventory report metadata is fetched, an exception should be raised.
        match = "No inventory reports to fetch. Check if \
                your inventory report is set up correctly."
        with pytest.raises(ValueError, match=match):
            await InventoryReport._fetch_inventory_report_metadata(
                gcs_file_system=gcs_file_system,
                inventory_report_config=inventory_report_config,
            )

    @pytest.mark.asyncio
    async def test_fetch_inventory_report_metadata_multiple_calls(self):
        # Create a mock for GCSFileSystem.
        gcs_file_system = mock.MagicMock(spec=GCSFileSystem)

        # Mock the _call method to return a page with two items
        # and then a page with one item and without next page token.
        gcs_file_system._call.side_effect = [
            {"items": ["item1", "item2"], "nextPageToken": "token1"},
            {"items": ["item3"], "nextPageToken": None},
        ]

        # Create a mock for InventoryReportConfig.
        inventory_report_config = mock.MagicMock(spec=InventoryReportConfig)
        inventory_report_config.bucket = "bucket_name"
        inventory_report_config.destination_path = "destination_path"

        result = await InventoryReport._fetch_inventory_report_metadata(
            gcs_file_system=gcs_file_system,
            inventory_report_config=inventory_report_config,
        )

        # Check that _call was called with the right arguments.
        calls = [
            mock.call(
                "GET", "b/{}/o", "bucket_name", prefix="destination_path", json_out=True
            ),
            mock.call(
                "GET",
                "b/{}/o",
                "bucket_name",
                prefix="destination_path",
                pageToken="token1",
                json_out=True,
            ),
        ]
        gcs_file_system._call.assert_has_calls(calls)

        # Check that the function correctly processed the response
        # and returned the right result.
        assert result == ["item1", "item2", "item3"]

    @pytest.mark.parametrize(
        "unsorted_inventory_report_metadata, expected",
        [
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
            ),
        ],
    )
    def test_sort_inventory_report_metadata(
        self, unsorted_inventory_report_metadata, expected
    ):
        result = InventoryReport._sort_inventory_report_metadata(
            unsorted_inventory_report_metadata=unsorted_inventory_report_metadata
        )
        assert result == expected

    @pytest.fixture(
        params=[
            # Unique most recent day, same datetime.
            (
                [
                    {"name": "report1", "timeCreated": "2023-08-02T12:00:00.000Z"},
                    {"name": "report2", "timeCreated": "2023-08-01T12:00:00.000Z"},
                ],
                # Expected results.
                ["report1"],
            ),
            # Multiple most recent day, same datetime.
            (
                [
                    {"name": "report1", "timeCreated": "2023-08-02T12:00:00.000Z"},
                    {"name": "report2", "timeCreated": "2023-08-02T12:00:00.000Z"},
                    {"name": "report3", "timeCreated": "2023-08-01T12:00:00.000Z"},
                ],
                # Expected results.
                ["report1", "report2"],
            ),
            # Multiple most recent day, different datetimes (same day, different hour).
            (
                [
                    {"name": "report1", "timeCreated": "2023-08-02T12:00:00.000Z"},
                    {"name": "report2", "timeCreated": "2023-08-02T11:00:00.000Z"},
                    {"name": "report3", "timeCreated": "2023-08-01T12:00:00.000Z"},
                ],
                # Expected results.
                ["report1", "report2"],
            ),
        ]
    )
    def download_inventory_report_content_setup(self, request):
        bucket = "bucket"
        gcs_file_system = mock.MagicMock()
        inventory_report_metadata, expected_reports = request.param

        # We are accessing the third argument as the return value,
        # since it is the object name in the function.
        # We are also encoding the content, since the actual method call needs
        # to decode the content.
        async_side_effect = mock.AsyncMock(
            side_effect=lambda *args, **kwargs: ("_header", args[3].encode())
        )
        gcs_file_system._call = async_side_effect
        return gcs_file_system, inventory_report_metadata, bucket, expected_reports

    @pytest.mark.asyncio
    async def test_download_inventory_report_content(
        self, download_inventory_report_content_setup
    ):
        (
            gcs_file_system,
            inventory_report_metadata,
            bucket,
            expected_reports,
        ) = download_inventory_report_content_setup

        result = await InventoryReport._download_inventory_report_content(
            gcs_file_system=gcs_file_system,
            inventory_report_metadata=inventory_report_metadata,
            bucket=bucket,
        )

        # Verify the mocked downloaded reports match (ordering does not matter).
        assert sorted(result) == sorted(expected_reports)

    @pytest.mark.parametrize(
        "inventory_report_line, use_snapshot_listing, \
        inventory_report_config_attrs, delimiter, bucket, expected",
        [
            # Test case 1: use snapshot listing with specific metadata
            # fields and delimiter.
            (
                "object1,value1,value2",
                True,
                {"obj_name_idx": 0, "metadata_fields": ["name", "field1", "field2"]},
                ",",
                "bucket",
                {"name": "object1", "field1": "value1", "field2": "value2"},
            ),
            # Test case 2: do not use snapshot listing and only fetch the name.
            (
                "object1,value1,value2",
                False,
                {"obj_name_idx": 0, "metadata_fields": ["name", "field1", "field2"]},
                ",",
                "bucket",
                {"name": "object1"},
            ),
        ],
    )
    def test_parse_inventory_report_line(
        self,
        inventory_report_line,
        use_snapshot_listing,
        inventory_report_config_attrs,
        delimiter,
        bucket,
        expected,
    ):
        # Mock InventoryReportConfig.
        inventory_report_config = mock.MagicMock(spec=InventoryReportConfig)
        inventory_report_config.obj_name_idx = inventory_report_config_attrs.get(
            "obj_name_idx"
        )
        inventory_report_config.metadata_fields = inventory_report_config_attrs.get(
            "metadata_fields"
        )

        # Mock GCSFileSystem.
        gcs_file_system = mock.MagicMock(spec=GCSFileSystem)
        gcs_file_system._process_object = mock.Mock(side_effect=lambda obj, bucket: obj)

        result = InventoryReport._parse_inventory_report_line(
            inventory_report_line=inventory_report_line,
            use_snapshot_listing=use_snapshot_listing,
            gcs_file_system=gcs_file_system,
            inventory_report_config=inventory_report_config,
            delimiter=delimiter,
            bucket=bucket,
        )

        assert result == expected

    @pytest.fixture(
        params=[
            # One file, one lines.
            (["header \n line1"], {"recordSeparator": "\n", "headerRequired": True}),
            (["line1"], {"recordSeparator": "\n", "headerRequired": False}),
            (
                ["header \r\n line1"],
                {"recordSeparator": "\r\n", "headerRequired": True},
            ),
            (["line1"], {"recordSeparator": "\r\n", "headerRequired": False}),
            # One file, multiple lines.
            (
                ["header \n line1 \n line2 \n line3"],
                {"recordSeparator": "\n", "headerRequired": True},
            ),
            (
                ["line1 \n line2 \n line3"],
                {"recordSeparator": "\n", "headerRequired": False},
            ),
            (
                ["header \r\n line1 \r\n line2 \r\n line3"],
                {"recordSeparator": "\r\n", "headerRequired": True},
            ),
            (
                ["line1 \r\n line2 \r\n line3"],
                {"recordSeparator": "\r\n", "headerRequired": False},
            ),
            # Multiple files.
            (
                ["line1", "line2 \n line3"],
                {"recordSeparator": "\n", "headerRequired": False},
            ),
            (
                ["header \n line1", "header \n line2 \n line3"],
                {"recordSeparator": "\n", "headerRequired": True},
            ),
        ]
    )
    def parse_inventory_report_content_setup(self, request):
        # Mock the necessary parameters.
        gcs_file_system = mock.MagicMock()
        bucket = mock.MagicMock()
        use_snapshot_listing = mock.MagicMock()

        # Parse the content and config data.
        inventory_report_content = request.param[0]
        inventory_report_config = request.param[1]
        record_separator = inventory_report_config["recordSeparator"]
        header_required = inventory_report_config["headerRequired"]

        # Construct custom inventory report config.
        inventory_report_config = mock.MagicMock(spec=InventoryReportConfig)
        inventory_report_config.csv_options = {
            "recordSeparator": record_separator,
            "headerRequired": header_required,
        }

        # Stub parse_inventory_report_line method.
        InventoryReport._parse_inventory_report_line = mock.MagicMock(
            side_effect="parsed_inventory_report_line"
        )

        return (
            gcs_file_system,
            inventory_report_content,
            inventory_report_config,
            bucket,
            use_snapshot_listing,
        )

    def test_parse_inventory_reports(self, parse_inventory_report_content_setup):
        (
            gcs_file_system,
            inventory_report_content,
            inventory_report_config,
            bucket,
            use_snapshot_listing,
        ) = parse_inventory_report_content_setup

        record_separator = inventory_report_config.csv_options["recordSeparator"]
        header_required = inventory_report_config.csv_options["headerRequired"]

        # Number of inventory reports.
        num_inventory_reports = len(inventory_report_content)

        # Tota, number of object metadata lines.
        total_lines_in_reports = sum(
            content.count(record_separator) + 1 for content in inventory_report_content
        )

        # Remove the header line for each line if header is present.
        total_lines_in_reports -= num_inventory_reports * 1 if header_required else 0

        result = InventoryReport._parse_inventory_report_content(
            gcs_file_system=gcs_file_system,
            inventory_report_content=inventory_report_content,
            inventory_report_config=inventory_report_config,
            use_snapshot_listing=use_snapshot_listing,
            bucket=bucket,
        )

        # Assert that the number of objects returned is correct.
        assert len(result) == total_lines_in_reports

        # Assert parse_inventory_report_line was called the correct
        # number of times.
        assert (
            InventoryReport._parse_inventory_report_line.call_count
            == total_lines_in_reports
        )

    @pytest.mark.parametrize(
        "use_snapshot_listing, prefix, mock_objects, expected_result",
        [
            # Not using snapshot, no prefix, directory, all matched.
            (
                False,
                None,
                [{"name": "prefix/object1"}, {"name": "prefix/object2"}],
                ([{"name": "prefix/object1"}, {"name": "prefix/object2"}], []),
            ),
            # Not using snapshot, no prefix, no directory, all matched.
            (
                False,
                None,
                [{"name": "object1"}, {"name": "object2"}],
                ([{"name": "object1"}, {"name": "object2"}], []),
            ),
            # Not using snapshot, prefix, directory, all matched.
            (
                False,
                "prefix",
                [{"name": "prefix/object1"}, {"name": "prefix/object2"}],
                ([{"name": "prefix/object1"}, {"name": "prefix/object2"}], []),
            ),
            # Not using snapshot, prefix, directory, some matched.
            (
                False,
                "prefix",
                [{"name": "prefix/object1"}, {"name": "object2"}],
                ([{"name": "prefix/object1"}], []),
            ),
            # Not using snapshot, prefix, directory, none matched.
            (False, "prefix", [{"name": "a/object1"}, {"name": "b/object2"}], ([], [])),
            # Not using snapshot, prefix, no directory, all matched.
            (
                False,
                "object",
                [{"name": "object1"}, {"name": "object2"}],
                ([{"name": "object1"}, {"name": "object2"}], []),
            ),
            # Not using snapshot, prefix, no directory, some matched.
            (
                False,
                "object",
                [{"name": "object1"}, {"name": "obj2"}],
                ([{"name": "object1"}], []),
            ),
            # Not using snapshot, prefix, no directory, none matched.
            (False, "object", [{"name": "obj1"}, {"name": "obj2"}], ([], [])),
            # Using snapshot, no prefix, no directory.
            (
                True,
                None,
                [{"name": "object1"}, {"name": "object2"}],
                ([{"name": "object1"}, {"name": "object2"}], []),
            ),
            # Using snapshot, no prefix, a single directory.
            (
                True,
                None,
                [{"name": "object1"}, {"name": "dir/object2"}],
                ([{"name": "object1"}], ["dir/"]),
            ),
            # Using snapshot, no prefix, multiple directories.
            (
                True,
                None,
                [
                    {"name": "object1"},
                    {"name": "dir1/object2"},
                    {"name": "dir2/object3"},
                ],
                ([{"name": "object1"}], ["dir1/", "dir2/"]),
            ),
            # Using snapshot, no prefix, same directory multiple times.
            (
                True,
                None,
                [
                    {"name": "object1"},
                    {"name": "dir1/object2"},
                    {"name": "dir1/object3"},
                ],
                ([{"name": "object1"}], ["dir1/"]),
            ),
            # Using snapshot, prefix, no directory.
            (
                True,
                "object",
                [{"name": "object1"}, {"name": "object2"}],
                ([{"name": "object1"}, {"name": "object2"}], []),
            ),
            # Using snapshot, prefix, a single directory.
            (
                True,
                "dir1/",
                [{"name": "dir1/dir2/object1"}, {"name": "dir1/object2"}],
                ([{"name": "dir1/object2"}], ["dir1/dir2/"]),
            ),
            # Using snapshot, prefix, multiple directories.
            (
                True,
                "dir1/",
                [
                    {"name": "dir1/dir2/object1"},
                    {"name": "dir1/dir3/object2"},
                    {"name": "dir1/object3"},
                ],
                ([{"name": "dir1/object3"}], ["dir1/dir2/", "dir1/dir3/"]),
            ),
            # Using snapshot, prefix, same directory multiple times.
            (
                True,
                "dir1/",
                [
                    {"name": "dir1/dir2/object1"},
                    {"name": "dir1/dir2/object2"},
                    {"name": "dir1/object3"},
                ],
                ([{"name": "dir1/object3"}], ["dir1/dir2/"]),
            ),
            # Sanity check from the examples given by the JSON API.
            # https://cloud.google.com/storage/docs/json_api/v1/objects/list
            (
                True,
                None,
                [
                    {"name": "a/b"},
                    {"name": "a/c"},
                    {"name": "d"},
                    {"name": "e"},
                    {"name": "e/f"},
                    {"name": "e/g/h"},
                ],
                ([{"name": "d"}, {"name": "e"}], ["a/", "e/"]),
            ),
            (
                True,
                "e/",
                [
                    {"name": "a/b"},
                    {"name": "a/c"},
                    {"name": "d"},
                    {"name": "e"},
                    {"name": "e/f"},
                    {"name": "e/g/h"},
                ],
                ([{"name": "e/f"}], ["e/g/"]),
            ),
            (
                True,
                "e",
                [
                    {"name": "a/b"},
                    {"name": "a/c"},
                    {"name": "d"},
                    {"name": "e"},
                    {"name": "e/f"},
                    {"name": "e/g/h"},
                ],
                ([{"name": "e"}], ["e/"]),
            ),
        ],
    )
    def test_construct_final_snapshot(
        self, use_snapshot_listing, prefix, mock_objects, expected_result
    ):
        # Construct the final snapshot.
        result = InventoryReport._construct_final_snapshot(
            objects=mock_objects,
            prefix=prefix,
            use_snapshot_listing=use_snapshot_listing,
        )

        # Assert the expected outcomes.
        items, prefixes = result
        expected_items, expected_prefixes = expected_result
        assert items == expected_items
        assert sorted(prefixes) == sorted(expected_prefixes)


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
            obj_name_idx=obj_name_idx,
        )

        assert inventory_report_config.csv_options == csv_options
        assert inventory_report_config.bucket == bucket
        assert inventory_report_config.destination_path == destination_path
        assert inventory_report_config.metadata_fields == metadata_fields
        assert inventory_report_config.obj_name_idx == obj_name_idx
