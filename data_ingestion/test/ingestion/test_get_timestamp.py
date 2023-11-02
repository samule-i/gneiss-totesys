from ingestion.get_timestamp import get_latest_timestamp
import pytest
import boto3
import os
from moto import mock_s3

INGESTION_BUCKET_NAME = os.environ["S3_DATA"]


@pytest.fixture
def s3_boto():
    with mock_s3():
        s3 = boto3.client("s3")
        location = {"LocationConstraint": "eu-west-2"}
        s3.create_bucket(
            Bucket=INGESTION_BUCKET_NAME, CreateBucketConfiguration=location
        )
        yield s3


def test_get_latest_timestamp_returns_default_when_no_files_for_table(s3_boto):
    previous_timestamp = get_latest_timestamp("sales_order")
    assert previous_timestamp == "2000-01-01 00:00:00.000"


def test_get_latest_timestamp_returns_most_recent_timestamp_for_table(s3_boto):
    s3_boto.put_object(
        Bucket=INGESTION_BUCKET_NAME,
        Key="sales_order_2023-10-23 12:17:09.792.json",
        Body="abc",
    )
    s3_boto.put_object(
        Bucket=INGESTION_BUCKET_NAME,
        Key="sales_order_2023-10-23 11:31:10.112.json",
        Body="abc",
    )
    s3_boto.put_object(
        Bucket=INGESTION_BUCKET_NAME,
        Key="sales_order_2023-10-23 10:37:09.902.json",
        Body="abc",
    )
    previous_timestamp = get_latest_timestamp("sales_order")
    assert previous_timestamp == "2023-10-23 12:17:09.792"


def test_get_latest_timestamp_only_gets_results_for_desired_table(s3_boto):
    s3_boto.put_object(
        Bucket=INGESTION_BUCKET_NAME,
        Key="sales_order_2023-10-23 12:17:09.792.json",
        Body="abc",
    )
    s3_boto.put_object(
        Bucket=INGESTION_BUCKET_NAME,
        Key="purchase_order_2023-10-23 10:37:09.902.json",
        Body="abc",
    )
    previous_timestamp = get_latest_timestamp("purchase_order")
    assert previous_timestamp == "2023-10-23 10:37:09.902"
