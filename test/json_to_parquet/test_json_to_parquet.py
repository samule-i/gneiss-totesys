from moto import mock_s3
import os
import pytest
import pandas as pd
from unittest.mock import patch
from json_to_parquet.json_to_parquet import lambda_handler


os.environ["PARQUET_S3_DATA_ID"] = "test_bucket"
out_bucket = os.environ["PARQUET_S3_DATA_ID"]

os.environ["S3_DATA_ID"] = "ingestion_test_bucket"


@patch("json_to_parquet.json_to_parquet.bucket_list")
@patch("json_to_parquet.json_to_parquet.transform_address")
@patch("json_to_parquet.json_to_parquet.dim_staff")
@patch("json_to_parquet.json_to_parquet.json_S3_key")
@patch("json_to_parquet.json_to_parquet.json_event")
def test_correct_transformation_functions_are_called(
    patched_json_event,
    patched_json_s3_key,
    patched_dim_staff,
    patched_transform_address,
    patched_bucket_list,
):
    patched_json_event.return_value = {
        "files": [
            "address/2023-11-14/14:20:01.json",
            "staff/2023-11-14/14:20:01.json",
        ]
    }
    patched_json_s3_key.side_effect = [
        {"table_name": "address"},
        {"table_name": "staff"},
    ]
    patched_bucket_list.return_value = {"dim_date/dim_date.parquet"}
    patched_dim_staff.return_value = pd.DataFrame()
    patched_transform_address.return_value = pd.DataFrame()

    lambda_handler(None, None)

    patched_transform_address.assert_called_once()
    patched_dim_staff.assert_called_once()


@patch("json_to_parquet.json_to_parquet.json_S3_key")
@patch("json_to_parquet.json_to_parquet.json_event")
def test_raises_key_error_if_payload_has_no_table_name_key(
    patched_json_event, patched_S3_key
):
    patched_json_event.return_value = {
        "files": ["address/2023-11-14/14:20:01.json"]
    }
    patched_S3_key.return_value = {"not_table_name": 0}

    with pytest.raises(KeyError):
        lambda_handler(None, None)


@mock_s3
@patch("json_to_parquet.json_to_parquet.write_pq_to_s3")
@patch("json_to_parquet.json_to_parquet.date_dimension")
@patch("json_to_parquet.json_to_parquet.bucket_list")
@patch("json_to_parquet.json_to_parquet.transform_address")
@patch("json_to_parquet.json_to_parquet.json_S3_key")
@patch("json_to_parquet.json_to_parquet.json_event")
def test_calls_date_dimension_if_not_created_yet(
    patched_json_event,
    patched_S3_key,
    patched_transform_address,
    patched_bucket_list,
    patched_date_dimension,
    patched_write_pq_to_s3,
):
    patched_json_event.return_value = {
        "files": ["address/2023-11-14/14:20:01.json"]
    }
    patched_S3_key.return_value = {"table_name": "address"}
    patched_transform_address.return_value = pd.DataFrame()
    patched_bucket_list.return_value = {}
    fake_date_df = pd.DataFrame()
    patched_date_dimension.return_value = fake_date_df

    lambda_handler(None, None)

    patched_date_dimension.assert_called_once()
    patched_write_pq_to_s3.assert_called_once_with(
        out_bucket, "dim_date/dim_date.parquet", fake_date_df
    )


@patch("json_to_parquet.json_to_parquet.bucket_list")
@patch("json_to_parquet.json_to_parquet.transform_address")
@patch("json_to_parquet.json_to_parquet.json_S3_key")
@patch("json_to_parquet.json_to_parquet.json_event")
def test_raises_value_error_if_df_not_returned_from_transformation_func(
    patched_json_event,
    patched_S3_key,
    patched_transform_address,
    patched_bucket_list,
):
    patched_json_event.return_value = {
        "files": ["address/2023-11-14/14:20:01.json"]
    }
    patched_S3_key.return_value = {"table_name": "address"}
    patched_bucket_list.return_value = {"dim_date/dim_date.parquet": 0}
    patched_transform_address.return_value = None

    with pytest.raises(ValueError):
        lambda_handler(None, None)


@patch("json_to_parquet.json_to_parquet.write_pq_to_s3")
@patch("json_to_parquet.json_to_parquet.bucket_list")
@patch("json_to_parquet.json_to_parquet.transform_address")
@patch("json_to_parquet.json_to_parquet.json_S3_key")
@patch("json_to_parquet.json_to_parquet.json_event")
def test_calls_write_pq_to_s3_with_correct_values(
    patched_json_event,
    patched_S3_key,
    patched_transform_address,
    patched_bucket_list,
    patched_write_pq_to_s3,
):
    patched_json_event.return_value = {
        "files": ["address/2023-11-14/14:20:01.json"]
    }
    patched_S3_key.return_value = {"table_name": "address"}
    patched_bucket_list.return_value = {"dim_date/dim_date.parquet": 0}
    transformed_df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    patched_transform_address.return_value = transformed_df

    lambda_handler(None, None)

    patched_write_pq_to_s3.assert_called_once_with(
        out_bucket, "dim_location/2023-11-14/14:20:01.json", transformed_df
    )
