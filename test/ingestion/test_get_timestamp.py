from ingestion.get_timestamp import (
    get_last_ingestion_timestamp,
    update_last_ingestion_timestamp)
import boto3
from moto import mock_s3
import pytest
from unittest.mock import patch
import os
os.environ['S3_DATA_ID'] = 'ingestion-test-bucket'


@pytest.fixture
def s3_boto():
    with mock_s3():
        s3 = boto3.client("s3")
        location = {'LocationConstraint': 'eu-west-2'}
        s3.create_bucket(
            Bucket='test-bucket-geni',
            CreateBucketConfiguration=location)
        yield s3


@mock_s3
def test_valid_table_name(s3_boto):
    key = 'timestamps.json'
    data = '{"last_timestamp": "2023-11-05 12:00:00"}'
    location = {'LocationConstraint': 'eu-west-2'}
    s3_boto.create_bucket(
        Bucket=os.environ['S3_DATA_ID'],
        CreateBucketConfiguration=location)
    s3_boto.put_object(Bucket=os.environ['S3_DATA_ID'], Key=key, Body=data)
    expected = "2023-11-05 12:00:00"
    actual = get_last_ingestion_timestamp()
    assert actual == expected


def test_returns_default_timestamp(s3_boto):
    location = {'LocationConstraint': 'eu-west-2'}
    s3_boto.create_bucket(
        Bucket=os.environ['S3_DATA_ID'],
        CreateBucketConfiguration=location)
    expected = "1970-01-01 00:00:00"
    actual = get_last_ingestion_timestamp()
    assert actual == expected


@patch('ingestion.get_timestamp.boto3.client')
def test_returns_exception(mock_client):
    mock_client.side_effect = ValueError
    with pytest.raises(ValueError):
        get_last_ingestion_timestamp()


@mock_s3
def test_update_timestamp(s3_boto):
    key = 'timestamps.json'
    initial_timestamp = '{"last_timestamp": "2023-11-05 12:00:00"}'
    updated_timestamp = '{"last_timestamp": "2023-11-05 13:00:00"}'
    location = {'LocationConstraint': 'eu-west-2'}
    s3_boto.create_bucket(
        Bucket=os.environ['S3_DATA_ID'],
        CreateBucketConfiguration=location)
    s3_boto.put_object(
        Bucket=os.environ['S3_DATA_ID'],
        Key=key,
        Body=initial_timestamp)
    update_last_ingestion_timestamp("2023-11-05 13:00:00")
    response = s3_boto.get_object(Bucket=os.environ['S3_DATA_ID'], Key=key)
    updated_json_data = response['Body'].read().decode('utf-8')
    assert updated_timestamp == updated_json_data


@patch('ingestion.get_timestamp.boto3.client')
def test_updater_returns_exception(mock_client):
    mock_client.side_effect = ValueError
    with pytest.raises(ValueError):
        update_last_ingestion_timestamp('not_a_timestamp')
