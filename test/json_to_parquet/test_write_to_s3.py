import boto3
import os
from moto import mock_s3
from unittest.mock import patch
from json_to_parquet.write_pq_to_s3 import write_pq_to_s3
import pandas as pd

os.environ["DATA_BUCKET"] = "gneiss_bucket"
fake_data = pd.DataFrame({'a': [1, 2, 3],
                          'b': [4, 5, 6]})


@mock_s3
def test_removes_json_from_name():
    patchpath = 'json_to_parquet.write_pq_to_s3.s3'
    bucket = 'bucket'
    with patch(patchpath) as mock:
        client = boto3.client('s3')
        client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )
        write_pq_to_s3(bucket, 'example.json', fake_data)
        calls = mock.to_parquet.call_args_list
        assert calls[0][1]['path'] == f's3://{bucket}/example.parquet'


@mock_s3
def test_adds_parquet_to_name():
    patchpath = 'json_to_parquet.write_pq_to_s3.s3'
    bucket = 'bucket'
    with patch(patchpath) as mock:
        client = boto3.client('s3')
        client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )
        write_pq_to_s3(bucket, 'example', fake_data)
        calls = mock.to_parquet.call_args_list
        assert calls[0][1]['path'] == f's3://{bucket}/example.parquet'


@mock_s3
def test_doesnt_add_parquet_if_not_needed():
    patchpath = 'json_to_parquet.write_pq_to_s3.s3'
    bucket = 'bucket'
    with patch(patchpath) as mock:
        client = boto3.client('s3')
        client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )
        write_pq_to_s3(bucket, 'example.parquet', fake_data)
        calls = mock.to_parquet.call_args_list
        assert calls[0][1]['path'] == f's3://{bucket}/example.parquet'
