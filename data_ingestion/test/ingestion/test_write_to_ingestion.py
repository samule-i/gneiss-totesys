from ingestion.write_JSON import write_to_ingestion
import boto3
from moto import mock_s3
import pytest
from botocore.exceptions import ClientError
import logging
logger = logging.getLogger('MyLogger')
logger.setLevel(logging.ERROR)


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
def test_the_data_has_been_uploaded_to_S3_using_ls(s3_boto):
    bucket = 'test-bucket-geni'
    key = "test"

    data = '[{"name":"John", "age":30, "car":3}]'

    location = {'LocationConstraint': 'eu-west-2'}
    s3_boto.create_bucket(Bucket=bucket, CreateBucketConfiguration=location)
    s3_boto.put_object(Bucket=bucket, Key=key, Body=data)
    write_to_ingestion(data, bucket, key)
    response = s3_boto.list_objects_v2(Bucket=bucket)
    assert response['Contents'][0]['Key'] == 'test'


@mock_s3
def test_function_handles_runtime_error(s3_boto):
    bucket = 'test-bucket-geni'
    key = 'test'
    data = []
    location = {'LocationConstraint': 'eu-west-2'}
    s3_boto.create_bucket(Bucket=bucket, CreateBucketConfiguration=location)
    with pytest.raises(RuntimeError):
        write_to_ingestion(data, bucket, key)


def test_function_handles_when_there_is_no_existing_bucket():
    bucket = 'test-bucket-eni'
    key = 'test'
    data = '[{"name":"John", "age":30, "car":3}]'
    try:
        write_to_ingestion(data, bucket, key)
    except ClientError as c:
        assert c.response['Error']['Code'] == 'NoSuchBucket'
