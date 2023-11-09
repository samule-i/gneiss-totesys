import boto3
import os
from moto import mock_s3
from unittest.mock import patch, MagicMock
from json_to_parquet.write_pq_to_s3 import write_pq_to_s3
import pandas as pd

os.environ["DATA_BUCKET"] = "gneiss_bucket"
fake_data = pd.DataFrame({'a': [1, 2, 3],
                          'b': [4, 5, 6]})


@mock_s3
def test_removes_json_from_name():
    botopath = 'json_to_parquet.write_pq_to_s3.boto3.client'
    with patch(botopath) as mock:
        mock_client = MagicMock()
        mock.return_value = mock_client

        client = boto3.client('s3')
        client.create_bucket(
            Bucket='bucket',
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )
        write_pq_to_s3('bucket', 'example.json', fake_data)
        calls = mock_client.put_object.call_args_list
        assert calls[0][1]['Key'] == 'example.parquet'


@mock_s3
def test_adds_parquet_to_name():
    botopath = 'json_to_parquet.write_pq_to_s3.boto3.client'
    with patch(botopath) as mock:
        mock_client = MagicMock()
        mock.return_value = mock_client

        client = boto3.client('s3')
        client.create_bucket(
            Bucket='bucket',
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )
        write_pq_to_s3('bucket', 'example', fake_data)
        calls = mock_client.put_object.call_args_list
        assert calls[0][1]['Key'] == 'example.parquet'


@mock_s3
def test_doesnt_add_parquet_if_not_needed():
    botopath = 'json_to_parquet.write_pq_to_s3.boto3.client'
    with patch(botopath) as mock:
        mock_client = MagicMock()
        mock.return_value = mock_client

        client = boto3.client('s3')
        client.create_bucket(
            Bucket='bucket',
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )
        write_pq_to_s3('bucket', 'example.parquet', fake_data)
        calls = mock_client.put_object.call_args_list
        assert calls[0][1]['Key'] == 'example.parquet'
