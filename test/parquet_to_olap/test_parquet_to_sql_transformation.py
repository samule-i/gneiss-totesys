import pytest
import pandas as pd
from unittest.mock import Mock, patch
from moto import mock_s3
from pg8000.native import Connection

from parquet_to_olap.parquet_to_sql_transformation import (
    parquet_to_sql,
    generate_sql_list_for_dataframe,
)


@mock_s3
def test_that_error_raised_if_first_argument_not_dataframe():
    conn = Mock()
    target_table = "dim_location"
    with pytest.raises(TypeError):
        parquet_to_sql("hello", target_table, conn)


@mock_s3
def test_that_error_raised_if_second_argument_not_valid_table():
    conn = Mock()
    dataframe = pd.DataFrame()
    target_table = "dim_building"
    with pytest.raises(ValueError):
        parquet_to_sql(dataframe, target_table, conn)


@mock_s3
def test_that_error_raised_if_fourth_argument_not_pg8000_conn():
    conn = Mock()
    dataframe = pd.DataFrame()
    target_table = "dim_location"
    with pytest.raises(TypeError):
        parquet_to_sql(dataframe, target_table, conn)


@patch(
    "parquet_to_olap.parquet_to_sql_transformation."
    "generate_sql_list_for_dataframe"
)
def test_that_to_sql_method_called_once_with_valid_input(patched_sql_list):
    patched_sql_list.return_value = ["query1"]
    conn = Mock(spec=Connection)
    dataframe = pd.DataFrame()
    target_table = "dim_location"
    target_pkey_column = "location_id"
    parquet_to_sql(dataframe, target_table, conn)
    patched_sql_list.assert_called_once_with(
        dataframe,
        target_table,
        target_pkey_column,
    )
    conn.run.assert_called_with("query1")


@patch(
    "parquet_to_olap.parquet_to_sql_transformation."
    "generate_sql_list_for_dataframe"
)
def test_that_Exception_raised_for_any_other_error(patched_sql_list):
    patched_sql_list.side_effect = Exception
    with pytest.raises(Exception):
        conn = Mock(spec=Connection)
        dataframe = pd.DataFrame()
        target_table = "dim_location"
        parquet_to_sql(dataframe, target_table, conn)


def test_generate_sql_list_returns_list_with_correct_number_of_queries():
    df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    result = generate_sql_list_for_dataframe(df, "test_table", "test_id")
    expected_result = [
        (
            "INSERT INTO test_table (col1, col2) VALUES "
            "('1', '3') "
            "on conflict (test_id) "
            "do update set (col1, col2) = (excluded.col1, excluded.col2);"
        ),
        (
            "INSERT INTO test_table (col1, col2) VALUES "
            "('2', '4') "
            "on conflict (test_id) "
            "do update set (col1, col2) = (excluded.col1, excluded.col2);"
        ),
    ]
    assert result == expected_result
