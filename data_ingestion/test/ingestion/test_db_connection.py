from pg8000 import connect
import pytest
from unittest.mock import MagicMock, Mock, patch

from ingestion.pg8000_conn import get_conn
from ingestion.db_credentials import get_credentials


# @patch('pg8000.connect', autospec=True)
# def test_get_conn_success(mock_connect):
#     mock_conn = Mock()
#     mock_connect.return_value = mock_conn
#     conn = get_conn('test_database')
#     assert conn == mock_conn


credentials = {
    'hostname': 'localhost',
    'username': 'test_user',
    'database': 'test_database',
    'password': 'test_pw'}


def test_get_conn_connects():

    # test_conn()
    # with patch(get_credentials('test_database'), return_value=MagicMock(credentials)):

    # def test_conn():
    # credentials = {}
    # credentials = {'hostname': 'localhost',
    #                'username': 'test_user',
    #                'database': 'test_database',
    #                'password': 'test_pw'}
    # with patch('ingestion.db_credentials.get_credentials', return_value=credentials):

    # with patch.dict(credentials, {
    #     'hostname': 'localhost',
    #     'username': 'test_user',
    #     'database': 'test_database',
    #         'password': 'test_pw'}) as credentials:

    # @patch(credentials['hostname'], return_value='localhost')
    # @patch(credentials['database'], return_value='test_database')
    # @patch(credentials['username'], return_value='test_user')
    # @patch(credentials['password'], return_value='test_pw')
    def test_conn():
        with patch(get_credentials('test_database'), return_value=MagicMock(credentials)):
            return get_conn('test_database')

    conn = test_conn()
    cursor = conn.cursor()
    query = f"SELECT * FROM sales_order ORDER BY sales_order_id DESC LIMIT 10;"
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    assert column_names == ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id',
                            'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id']
