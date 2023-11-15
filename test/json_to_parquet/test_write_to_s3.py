import boto3
import os
from moto import mock_s3
from unittest.mock import patch, Mock
from json_to_parquet.write_pq_to_s3 import write_pq_to_s3
import pandas as pd

os.environ["DATA_BUCKET"] = "gneiss_bucket"
fake_data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


@mock_s3
@patch("json_to_parquet.write_pq_to_s3.boto3.client")
def test_removes_json_from_name(patched_boto_client):
    bucket = "bucket"
    fake_s3 = Mock()
    patched_boto_client.return_value = fake_s3
    parquet_bytes = fake_data.to_parquet()

    write_pq_to_s3(bucket, "example.json", fake_data)

    fake_s3.put_object.assert_called_once_with(
        Bucket=bucket, Key="example.parquet", Body=parquet_bytes
    )


@mock_s3
@patch("json_to_parquet.write_pq_to_s3.boto3.client")
def test_adds_parquet_to_name(patched_boto_client):
    bucket = "bucket"
    fake_s3 = Mock()
    patched_boto_client.return_value = fake_s3
    parquet_bytes = fake_data.to_parquet()

    write_pq_to_s3(bucket, "example", fake_data)

    fake_s3.put_object.assert_called_once_with(
        Bucket=bucket, Key="example.parquet", Body=parquet_bytes
    )


@mock_s3
@patch("json_to_parquet.write_pq_to_s3.boto3.client")
def test_doesnt_add_parquet_if_not_needed(patched_boto_client):
    bucket = "bucket"
    fake_s3 = Mock()
    patched_boto_client.return_value = fake_s3
    parquet_bytes = fake_data.to_parquet()

    write_pq_to_s3(bucket, "example.parquet", fake_data)

    fake_s3.put_object.assert_called_once_with(
        Bucket=bucket, Key="example.parquet", Body=parquet_bytes
    )
