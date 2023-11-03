import unittest
from ingestion.rows_to_json import rows_to_json, CustomEncoder
import json
import datetime
from unittest.mock import patch, MagicMock
from decimal import Decimal


class TestRowsToJsonFunction(unittest.TestCase):

    @patch('ingestion.rows_to_json.get_conn')
    def test_rows_to_json(self, mock_get_conn):
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        table_name = 'sales_order'
        last_timestamp = '2023-11-01 14:20:00.000'
        mock_cursor.fetchall.return_value = (
            [
                5030,
                datetime.datetime(2023, 11, 1, 14, 22, 10, 329000),
                datetime.datetime(2023, 11, 1, 14, 22, 10, 329000),
                186, 11, 17, 51651, 3.25, 2,
                datetime.date(2023, 11, 6),
                datetime.date(2023, 11, 5),
                27,
            ],
        )
        mock_cursor.excecute.return_value = (
            [
                5030,
                datetime.datetime(2023, 11, 1, 14, 22, 10, 329000),
                datetime.datetime(2023, 11, 1, 14, 22, 10, 329000),
                186, 11, 17, 51651, 3.25, 2,
                datetime.date(2023, 11, 6),
                datetime.date(2023, 11, 5),
                27,
            ],
        )
        mock_cursor.description = [
            ('sales_order_id',
             23,
             None,
             None,
             None,
             None,
             None),
            ('created_at',
             1114,
             None,
             None,
             None,
             None,
             None),
            ('last_updated',
             1114,
             None,
             None,
             None,
             None,
             None),
            ('design_id',
             23,
             None,
             None,
             None,
             None,
             None),
            ('staff_id',
             23,
             None,
             None,
             None,
             None,
             None),
            ('counterparty_id',
             23,
             None,
             None,
             None,
             None,
             None),
            ('units_sold',
             23,
             None,
             None,
             None,
             None,
             None),
            ('unit_price',
             701,
             None,
             None,
             None,
             None,
             None),
            ('currency_id',
             23,
             None,
             None,
             None,
             None,
             None),
            ('agreed_delivery_date',
             1082,
             None,
             None,
             None,
             None,
             None),
            ('agreed_payment_date',
             1082,
             None,
             None,
             None,
             None,
             None),
            ('agreed_delivery_location_id',
             23,
             None,
             None,
             None,
             None,
             None)]

        expected_result = {
            "table_name": "sales_order",
            "column_names": [
                "sales_order_id",
                "created_at",
                "last_updated",
                "design_id",
                "staff_id",
                "counterparty_id",
                "units_sold",
                "unit_price",
                "currency_id",
                "agreed_delivery_date",
                "agreed_payment_date",
                "agreed_delivery_location_id"
            ],
            "record_count": 1,
            "data": [
                [
                    5030,
                    "2023-11-01T14:22:10.329000",
                    "2023-11-01T14:22:10.329000",
                    186,
                    11,
                    17,
                    51651,
                    3.25,
                    2,
                    "2023-11-06",
                    "2023-11-05",
                    27
                ]
            ]
        }

        actual_json = rows_to_json(table_name, last_timestamp)

        self.assertEqual(json.loads(actual_json), expected_result)

    @patch('ingestion.rows_to_json.get_conn')
    def test_invalid_datetime_format(self, mock_get_conn):
        table_name = 'sales_order'
        last_timestamp = '2023-11-01-14:20:00'
        expected_error_message = "Error: invalid last_timestamp format"

        actual_json = rows_to_json(table_name, last_timestamp)
        actual_result = json.loads(actual_json)

        self.assertTrue("error" in actual_result)
        self.assertEqual(actual_result["error"], expected_error_message)

    @patch('ingestion.rows_to_json.get_conn')
    def test_invalid_table_name(self, mock_get_conn):
        table_name = 'non_existent_table'
        last_timestamp = '2023-11-01 14:20:00.000'
        expected_error = \
            "Error: Table 'non_existent_table' is not a valid totesys table."

        actual_json = rows_to_json(table_name, last_timestamp)
        actual_result = json.loads(actual_json)

        self.assertTrue("error" in actual_result)
        self.assertEqual(actual_result["error"], expected_error)


class TestCustomEncoder(unittest.TestCase):
    def setUp(self):
        self.encoder = CustomEncoder()

    def test_datetime_serialization(self):
        now = datetime.datetime.now()
        serialized = self.encoder.default(now)
        expected = now.isoformat()
        self.assertEqual(serialized, expected)

    def test_date_serialization(self):
        today = datetime.date.today()
        serialized = self.encoder.default(today)
        expected = today.isoformat()
        self.assertEqual(serialized, expected)

    def test_decimal_serialization(self):
        decimal_val = Decimal('123.456')
        serialized = self.encoder.default(decimal_val)
        expected = float(decimal_val)
        self.assertEqual(serialized, expected)
