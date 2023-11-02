import unittest
from data_ingestion.src.ingestion.rows_to_json import rows_to_json
import os
import json
from unittest.mock import patch
import pg8000


class TestRowsToJsonFunction(unittest.TestCase):
    @patch('data_ingestion.src.ingestion.rows_to_json.get_conn')
    def test_rows_to_json(self, mock_get_conn):

        host = 'localhost'
        database = 'test_database'
        user = os.getlogin()
        password = os.environ.get("DB_PASSWORD")
        table_name = 'sales_order'
        last_timestamp = '2023-11-01 14:20:00.000'
        conn = pg8000.connect(
            user=user,
            password=password,
            host=host,
            database=database)
        mock_get_conn.return_value = conn

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

        actual_json = rows_to_json(
            host,
            database,
            user,
            password,
            table_name,
            last_timestamp)
        actual_result = json.loads(actual_json)
        assert actual_result == expected_result

    @patch('data_ingestion.src.ingestion.rows_to_json.get_conn')
    def test_invalid_datetime_format(self, mock_get_conn):
        host = 'localhost'
        database = 'test_database'
        user = os.getlogin()
        password = os.environ.get("DB_PASSWORD")
        table_name = 'sales_order'
        last_timestamp = '2023-11-01-14:20:00'
        conn = pg8000.connect(
            user=user,
            password=password,
            host=host,
            database=database)
        mock_get_conn.return_value = conn

        actual_json = rows_to_json(
            host,
            database,
            user,
            password,
            table_name,
            last_timestamp)
        actual_result = json.loads(actual_json)

        self.assertTrue("error" in actual_result)
        self.assertIn("should be in the format", actual_result["error"])

    @patch('data_ingestion.src.ingestion.rows_to_json.get_conn')
    def test_invalid_table_name(self, mock_get_conn):
        host = 'localhost'
        database = 'test_database'
        user = os.getlogin()
        password = os.environ.get("DB_PASSWORD")
        table_name = 'non_existent_table'
        last_timestamp = '2023-11-01 14:20:00.000'
        conn = pg8000.connect(
            user=user,
            password=password,
            host=host,
            database=database)
        mock_get_conn.return_value = conn

        actual_json = rows_to_json(
            host,
            database,
            user,
            password,
            table_name,
            last_timestamp)
        actual_result = json.loads(actual_json)

        self.assertTrue("error" in actual_result)
        self.assertIn("is not a valid totesys table", actual_result["error"])
