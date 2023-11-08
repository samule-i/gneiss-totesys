from moto import mock_s3
import boto3
from botocore.exceptions import ClientError
import pytest
import awswrangler as wr
import pandas as pd
import json
from src.get_s3_file import parquet_S3_key, parquet_event


@pytest.fixture(scope='function')
def fake_event():
    with open('test/fake_events/put_file.json') as file:
        data = file.read()
    event = json.loads(data)
    return event


@mock_s3
def test_get_s3_returns_parquet_data():
    s3_client = boto3.client('s3')
    s3_client.create_bucket(
        Bucket='bucket',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )

    df1 = pd.read_parquet('./test/test_parquet_files/address_1.parquet')

    wr.s3.to_parquet(
        df=df1,
        path='s3://bucket/address_1.parquet'
    )
    df2 = pd.read_parquet('./test/test_parquet_files/address_2.parquet')
    wr.s3.to_parquet(
        df=df2,
        path='s3://bucket/address_2.parquet'
    )

    df1_reloaded = parquet_S3_key('bucket', 'address_1.parquet')

    df2_reloaded = parquet_S3_key('bucket', 'address_2.parquet')

    df1_json = df1.to_json()
    df1_reloaded_json = df1_reloaded.to_json()
    df2_reloaded_json = df2_reloaded.to_json()

    assert df1_reloaded_json != df2_reloaded_json
    assert df1_json == df1_reloaded_json


@mock_s3
def test_get_input_file_returns_correct_data(fake_event):
    bucket = fake_event['Records'][0]["s3"]["bucket"]["name"]

    s3_client = boto3.client('s3')
    s3_client.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )

    df1 = pd.read_parquet('./test/test_parquet_files/address_1.parquet')

    wr.s3.to_parquet(
        df=df1,
        path='s3://bucket/address_1.parquet'
    )

    df1_reloaded = parquet_event(fake_event)
    df1_json = df1.to_json()
    df1_reloaded_json = df1_reloaded.to_json()

    assert df1_json == df1_reloaded_json


@mock_s3
def test_get_raises_error_when_no_bucket():
    with pytest.raises(ClientError):
        parquet_S3_key('none_bucket', 'none_key')


@mock_s3
def test_get_raises_error_when_no_key():
    s3_client = boto3.client('s3')
    s3_client.create_bucket(
        Bucket='test',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    with pytest.raises(wr.exceptions.NoFilesFound):
        parquet_S3_key('test', 'none_key')


@mock_s3
def test_get_logs_error_when_no_bucket(caplog):
    with pytest.raises(ClientError):
        parquet_S3_key('none_bucket', 'none_key')

    message_list = caplog.text.split('\n')
    assert message_list[0][0:5] == 'ERROR'
    expected_message = (
        'An error occurred (NoSuchBucket) when calling the ListObjectsV2 '
        'operation: The specified bucket does not exist')
    assert caplog.records[0].message == expected_message


@mock_s3
def test_get_logs_error_when_no_key(caplog):
    s3_client = boto3.client('s3')
    s3_client.create_bucket(
        Bucket='test',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    with pytest.raises(wr.exceptions.NoFilesFound):
        parquet_S3_key('test', 'none_key')

    message_list = caplog.text.split('\n')
    assert message_list[0][0:5] == 'ERROR'
    expected_message = 'No files Found on: s3://test/none_key.'
    assert caplog.records[0].message == expected_message
