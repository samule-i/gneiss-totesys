import pytest
import pandas as pd
from unittest.mock import Mock, patch
from moto import mock_s3
from pg8000 import Connection

from parquet_to_olap.parquet_to_sql_transformation import parquet_to_sql


@mock_s3
def test_that_error_raised_if_first_argument_not_dataframe():
    conn = Mock()
    target_table = 'dim_location'
    with pytest.raises(TypeError):
        parquet_to_sql('hello', target_table, conn)


@mock_s3
def test_that_error_raised_if_second_argument_not_valid_table():
    conn = Mock()
    dataframe = pd.DataFrame()
    target_table = 'dim_building'
    with pytest.raises(ValueError):
        parquet_to_sql(dataframe, target_table, conn)


@mock_s3
def test_that_error_raised_if_fourth_argument_not_pg8000_conn():
    conn = Mock()
    dataframe = pd.DataFrame()
    target_table = 'dim_location'
    with pytest.raises(TypeError):
        parquet_to_sql(dataframe, target_table, conn)


@patch('parquet_to_olap.parquet_to_sql_transformation.wr.postgresql')
def test_that_to_sql_method_called_once_with_valid_input(patched_wr):
    conn = Mock(spec=Connection)
    dataframe = pd.DataFrame()
    target_table = 'dim_location'
    target_pkey_column = 'location_id'
    parquet_to_sql(dataframe, target_table, conn)
    patched_wr.to_sql.assert_called()
    patched_wr.to_sql.assert_called_once_with(
        df=dataframe,
        table=target_table,
        schema='public',
        mode='upsert',
        con=conn,
        upsert_conflict_columns=[target_pkey_column]
    )


@patch('parquet_to_olap.parquet_to_sql_transformation.wr.postgresql')
def test_that_Exception_raised_for_any_other_error(patched_wr):
    patched_wr.to_sql.side_effect = Exception
    with pytest.raises(Exception):
        conn = Mock(spec=Connection)
        dataframe = pd.DataFrame()
        target_table = 'dim_location'
        parquet_to_sql(dataframe, target_table, conn)
