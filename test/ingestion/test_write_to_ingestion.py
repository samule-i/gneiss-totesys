import json
from ingestion.write_JSON import write_to_ingestion
import boto3
from moto import mock_s3
import pytest
from botocore.exceptions import ClientError
import logging
from datetime import date as dt
from unittest.mock import patch


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
    data = {
        "table_name": "sales_order",
        "column_names": [
            "sales_order_id",
            "created_at",
            "last_updated",
            "design_id",
            "staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "agreed_delivery_date",
            "agreed_payment_date",
            "agreed_delivery_location_id"
        ],
        "record_count": 1,
        "data": [
                [
                    5030,
                    "2023-11-01T14:22:10.329000",
                    "2023-11-01T14:22:10.329000",
                    186,
                    11,
                    17,
                    51651,
                    3.25,
                    2,
                    "2023-11-06",
                    "2023-11-05",
                    27
                ]
        ]
    }
    json_data = json.dumps(data)
    location = {'LocationConstraint': 'eu-west-2'}
    s3_boto.create_bucket(Bucket=bucket, CreateBucketConfiguration=location)
    write_to_ingestion(json_data, bucket)
    response = s3_boto.list_objects_v2(Bucket=bucket)
    assert (f"sales_order/{dt.today()}" in response['Contents'][0]['Key'])


@mock_s3
def test_function_handles_runtime_error(s3_boto):
    bucket = 'test-bucket-geni'
    data = []
    location = {'LocationConstraint': 'eu-west-2'}
    s3_boto.create_bucket(Bucket=bucket, CreateBucketConfiguration=location)
    with pytest.raises(RuntimeError):
        write_to_ingestion(data, bucket)


@mock_s3
def test_function_handles_when_there_is_no_existing_bucket():
    bucket = 'test-bucket-eni'
    data = {"name": "John", "age": 30, "car": 3}
    json_data = json.dumps(data)
    try:
        write_to_ingestion(json_data, bucket)
    except ClientError as c:
        assert c.response['Error']['Code'] == 'NoSuchBucket'


@patch("ingestion.write_JSON.boto3.client")
def test_no_file_created_when_there_is_no_new_data(patched_boto3):
    data_with_no_rows = {
        "table_name": "sales_order",
        "column_names": ["a", "b", "c"],
        "record_count": 0,
        "rows": []
    }
    data_str = json.dumps(data_with_no_rows)
    write_to_ingestion(data_str, "bucket")
    patched_boto3.put_object.assert_not_called()
