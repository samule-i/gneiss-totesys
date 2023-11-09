import json
import pytest
import pandas as pd
from unittest.mock import Mock, patch

from parquet_to_olap.parquet_to_olap import lambda_handler


@pytest.fixture(scope='function')
def fake_event():
    with open('test/_fake_events/par2olap_handler_event.json') as file:
        data = file.read()
    event = json.loads(data)
    return event


@pytest.fixture(scope='function')
def broken_event():
    with open('test/_fake_events/par2olap_handler_broken_event.json') as file:
        data = file.read()
    event = json.loads(data)
    return event


@patch('parquet_to_olap.parquet_to_olap.get_conn')
@patch('parquet_to_olap.parquet_to_olap.get_credentials')
@patch('parquet_to_olap.parquet_to_olap.parquet_to_sql')
@patch('parquet_to_olap.parquet_to_olap.parquet_event')
def test_that_parquet_to_sql_called_once_with_correct_arguments(
        patched_parquet_event,
        patched_parquet_to_sql,
        patched_get_credentials,
        patched_get_conn,
        fake_event):
    df = pd.DataFrame()
    table_name = 'dim_location'
    patched_parquet_event.return_value = df
    conn = Mock()
    patched_get_conn.return_value = conn
    lambda_handler(fake_event, None)

    patched_parquet_to_sql.assert_called_once_with(df, table_name, conn)
    conn.close.assert_called_once()


@patch('parquet_to_olap.parquet_to_olap.get_conn')
@patch('parquet_to_olap.parquet_to_olap.get_credentials')
@patch('parquet_to_olap.parquet_to_olap.parquet_to_sql')
@patch('parquet_to_olap.parquet_to_olap.parquet_event')
def test_that_ValueError_raised_for_invalid_table(
        patched_parquet_event,
        patched_parquet_to_sql,
        patched_get_credentials,
        patched_get_conn,
        broken_event):
    with pytest.raises(ValueError):
        lambda_handler(broken_event, None)
