from moto import mock_s3
import boto3
from botocore.exceptions import ClientError
import pytest
from json_to_parquet.get_s3_file import json_S3_key, json_event


@mock_s3
def test_get_s3_returns_json_data():
    s3_client = boto3.client('s3')
    s3_client.create_bucket(
        Bucket='bucket',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    s3_client.put_object(
        Bucket='bucket',
        Key='file.json',
        Body='{"a": "b"}'
    )

    data = json_S3_key('bucket', 'file.json')
    assert data["a"] == "b"


@mock_s3
def test_get_input_file_returns_correct_data(fake_event):
    bucket = fake_event['Records'][0]["s3"]["bucket"]["name"]
    key = fake_event['Records'][0]['s3']['object']['key']

    s3_client = boto3.client('s3')
    s3_client.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body='{"a": "b"}'
    )

    data = json_event(fake_event)
    assert data["a"] == "b"


@mock_s3
def test_get_raises_error_when_no_bucket():
    with pytest.raises(ClientError):
        json_S3_key('none_bucket', 'none_key')


@mock_s3
def test_get_raises_error_when_no_key():
    s3_client = boto3.client('s3')
    s3_client.create_bucket(
        Bucket='test',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    with pytest.raises(ClientError):
        json_S3_key('test', 'none_key')


@mock_s3
def test_get_logs_error_when_no_bucket(caplog):
    with pytest.raises(ClientError):
        json_S3_key('none_bucket', 'none_key')

    message_list = caplog.text.split('\n')
    assert message_list[0][0:5] == 'ERROR'
    expected_message = 'NoSuchBucket'
    assert caplog.records[0].message == expected_message


@mock_s3
def test_get_logs_error_when_no_key(caplog):
    s3_client = boto3.client('s3')
    s3_client.create_bucket(
        Bucket='test',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    with pytest.raises(ClientError):
        json_S3_key('test', 'none_key')

    message_list = caplog.text.split('\n')
    assert message_list[0][0:5] == 'ERROR'
    expected_message = 'NoSuchKey'
    assert caplog.records[0].message == expected_message