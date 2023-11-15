import os
import json
import pytest
import pandas as pd
from unittest.mock import Mock, patch

from parquet_to_olap.parquet_to_olap import lambda_handler, process_file


os.environ["PARQUET_S3_DATA_ID"] = "test_bucket"


# @pytest.fixture(scope="function")
# def fake_event():
#     with open("test/_fake_events/par2olap_handler_event.json") as file:
#         data = file.read()
#     event = json.loads(data)
#     return event


# @pytest.fixture(scope="function")
# def broken_event():
#     with open("test/_fake_events/par2olap_handler_broken_event.json") as file:
#         data = file.read()
#     event = json.loads(data)
#     return event


# @patch("parquet_to_olap.parquet_to_olap.get_conn")
# @patch("parquet_to_olap.parquet_to_olap.get_credentials")
# @patch("parquet_to_olap.parquet_to_olap.parquet_to_sql")
# @patch("parquet_to_olap.parquet_to_olap.parquet_event")
# def test_that_parquet_to_sql_called_once_with_correct_arguments(
#     patched_parquet_event,
#     patched_parquet_to_sql,
#     patched_get_credentials,
#     patched_get_conn,
#     fake_event,
# ):
#     df = pd.DataFrame()
#     table_name = "dim_location"
#     patched_parquet_event.return_value = df
#     conn = Mock()
#     patched_get_conn.return_value = conn
#     lambda_handler(fake_event, None)

#     patched_parquet_to_sql.assert_called_once_with(df, table_name, conn)
#     conn.close.assert_called_once()


# @patch("parquet_to_olap.parquet_to_olap.get_conn")
# @patch("parquet_to_olap.parquet_to_olap.get_credentials")
# @patch("parquet_to_olap.parquet_to_olap.parquet_to_sql")
# @patch("parquet_to_olap.parquet_to_olap.parquet_event")
# def test_that_ValueError_raised_for_invalid_table(
#     patched_parquet_event,
#     patched_parquet_to_sql,
#     patched_get_credentials,
#     patched_get_conn,
#     broken_event,
# ):
#     with pytest.raises(ValueError):
#         lambda_handler(broken_event, None)


@patch("parquet_to_olap.parquet_to_olap.process_file")
@patch("parquet_to_olap.parquet_to_olap.get_conn")
@patch("parquet_to_olap.parquet_to_olap.get_credentials")
@patch("parquet_to_olap.parquet_to_olap.read_json_file_from_bucket")
def test_lambda_handler_calls_all_required_functions(
    patched_read_json,
    patched_get_credentials,
    patched_get_conn,
    patched_process_file,
):
    fake_event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test_bucket"},
                    "object": {"key": "manifest.json"},
                }
            }
        ]
    }
    patched_read_json.return_value = {"files": ["file1.parquet"]}

    dummy_credentials = {"a": 1, "b": 2}
    patched_get_credentials.return_value = dummy_credentials

    dummy_conn = Mock()
    patched_get_conn.return_value = dummy_conn

    lambda_handler(fake_event, None)

    patched_read_json.assert_called_once_with("test_bucket", "manifest.json")
    patched_get_credentials.assert_called_once_with("db_credentials_olap")
    patched_get_conn.assert_called_once_with(dummy_credentials)
    patched_process_file.assert_called_once_with(
        dummy_conn, "test_bucket", "file1.parquet"
    )
    dummy_conn.close.assert_called()


@patch("parquet_to_olap.parquet_to_olap.parquet_to_sql")
@patch("parquet_to_olap.parquet_to_olap.parquet_S3_key")
def test_process_file_calls_parquet_to_sql_with_correct_inputs(
    patched_parquet_S3_key, patched_parquet_to_sql
):
    dummy_conn = Mock()
    dummy_df = pd.DataFrame()
    patched_parquet_S3_key.return_value = dummy_df
    process_file(
        dummy_conn, "test_bucket", "dim_location/2023-11-15/10:00:00.parquet"
    )

    patched_parquet_S3_key.assert_called_once_with(
        "test_bucket", "dim_location/2023-11-15/10:00:00.parquet"
    )
    patched_parquet_to_sql.assert_called_once_with(
        dummy_df, "dim_location", dummy_conn
    )


@patch("parquet_to_olap.parquet_to_olap.parquet_S3_key")
def test_process_file_raises_error_for_invalid_table_name(
    patched_parquet_S3_key,
):
    dummy_conn = Mock()
    with pytest.raises(ValueError):
        process_file(
            dummy_conn,
            "test_bucket",
            "invalid_table/2023-11-15/10:00:00.parquet",
        )
