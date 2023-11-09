from pg8000 import DatabaseError
import pytest
from unittest.mock import patch

from src.ingestion.pg8000_conn import get_conn


credentials = {
    'hostname': 'localhost',
    'username': 'test_user',
    'database': 'test_database',
    'password': 'test_pw'}


@patch("src.ingestion.pg8000_conn.Connection")
def test_get_conn_connects(patched_connect):

    get_conn(credentials)
    patched_connect.assert_called_once_with(
        user='test_user',
        host='localhost',
        database='test_database',
        password='test_pw')


@patch("src.ingestion.pg8000_conn.Connection")
def test_DB_error_returned_for_invalid_credentials(patched_connect):
    patched_connect.side_effect = DatabaseError('Error connecting to DB')
    with pytest.raises(DatabaseError):
        get_conn(credentials)
