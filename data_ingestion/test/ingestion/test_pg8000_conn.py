# import os
# import pytest
# from pg8000.native import Connection, DatabaseError

# from ingestion.pg8000_conn import get_conn


# def test_that_interface_error_raised_with_incorrect_user_info():
#     with pytest.raises(DatabaseError):
#         DB_HOST = os.environ['HOST']
#         DB_DB = os.environ['DATABASE']
#         DB_USER = 'dummy_user'
#         DB_PASSWORD = os.environ['PASSWORD']
#         return (Connection(user=DB_USER, host=DB_HOST,
#                            database=DB_DB, password=DB_PASSWORD))


# def test_that_expected_output_received_from_successful_connection():
#     column_names, rows = main()
#     print(column_names)
#     assert column_names == (['sales_order_id',
#                              'created_at',
#                              'last_updated',
#                              'design_id',
#                              'staff_id',
#                              'counterparty_id',
#                              'units_sold',
#                              'unit_price',
#                              'currency_id',
#                              'agreed_delivery_date',
#                              'agreed_payment_date',
#                              'agreed_delivery_location_id'])

#     assert len(rows) == 10
#     for row in rows:
#         assert len(row) == 12

import unittest
from unittest.mock import Mock, patch
import ingestion.pg8000_conn


class TestGetConn(unittest.TestCase):
    @patch('pg8000.connect', autospec=True)
    def test_get_conn_success(self, mock_connect):
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        conn = ingestion.pg8000_conn.get_conn()
        self.assertEqual(conn, mock_conn)

    @patch('pg8000.connect', autospec=True)
    def test_get_conn_exception(self, mock_connect):
        mock_connect.side_effect = Exception("Connection failed")
        with self.assertRaises(Exception):
            ingestion.pg8000_conn.get_conn()


if __name__ == '__main__':
    unittest.main()
