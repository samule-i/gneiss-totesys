from moto import mock_s3
import boto3
from botocore.exceptions import ClientError
import pytest
import awswrangler as wr
import pandas as pd
import json
from parquet_to_olap.get_parquet_s3_file import (
    parquet_S3_key,
    parquet_event,
    read_json_file_from_bucket,
)


@pytest.fixture(scope="function")
def fake_event():
    with open("test/_fake_events/par2olap_put_file.json") as file:
        data = file.read()
    event = json.loads(data)
    return event


@mock_s3
def test_get_s3_returns_parquet_data():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket="bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    df1 = pd.read_parquet(
        "./test/parquet_to_olap/" "test_parquet_files/address_1.parquet"
    )

    wr.s3.to_parquet(df=df1, path="s3://bucket/address_1.parquet")
    df2 = pd.read_parquet(
        "./test/parquet_to_olap/" "test_parquet_files/address_2.parquet"
    )
    wr.s3.to_parquet(df=df2, path="s3://bucket/address_2.parquet")

    df1_reloaded = parquet_S3_key("bucket", "address_1.parquet")

    df2_reloaded = parquet_S3_key("bucket", "address_2.parquet")

    df1_json = df1.to_json()
    df1_reloaded_json = df1_reloaded.to_json()
    df2_reloaded_json = df2_reloaded.to_json()

    assert df1_reloaded_json != df2_reloaded_json
    assert df1_json == df1_reloaded_json


@mock_s3
def test_get_input_file_returns_correct_data(fake_event):
    bucket = fake_event["Records"][0]["s3"]["bucket"]["name"]

    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    df1 = pd.read_parquet(
        "./test/parquet_to_olap/" "test_parquet_files/address_1.parquet"
    )

    wr.s3.to_parquet(df=df1, path="s3://bucket/address_1.parquet")

    df1_reloaded = parquet_event(fake_event)
    df1_json = df1.to_json()
    df1_reloaded_json = df1_reloaded.to_json()

    assert df1_json == df1_reloaded_json


@mock_s3
def test_get_raises_error_when_no_bucket():
    with pytest.raises(ClientError):
        parquet_S3_key("none_bucket", "none_key")


@mock_s3
def test_get_raises_error_when_no_key():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket="test",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with pytest.raises(ClientError):
        parquet_S3_key("test", "none_key")


@mock_s3
def test_get_logs_error_when_no_bucket(caplog):
    with pytest.raises(ClientError):
        parquet_S3_key("none_bucket", "none_key")

    expected_message = (
        "An error occurred (NoSuchBucket) when calling the GetObject "
        "operation: The specified bucket does not exist"
    )
    assert caplog.records[-1].levelname == "ERROR"
    assert caplog.records[-1].message == expected_message


@mock_s3
def test_get_logs_error_when_no_key(caplog):
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket="test",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with pytest.raises(ClientError):
        parquet_S3_key("test", "none_key")

    expected_message = (
        "An error occurred (NoSuchKey) when calling the GetObject "
        "operation: The specified key does not exist."
    )
    assert caplog.records[-1].levelname == "ERROR"
    assert caplog.records[-1].message == expected_message


@mock_s3
def test_read_json_file_returns_dict_with_correct_contents():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.put_object(
        Bucket="test_bucket", Key="test_key", Body='{"a": "b"}'
    )
    result = read_json_file_from_bucket("test_bucket", "test_key")

    assert result == {"a": "b"}


@mock_s3
def test_read_json_file_raises_client_error_if_file_unavailable():
    with pytest.raises(ClientError):
        result = read_json_file_from_bucket("test_bucket", "test_key")
