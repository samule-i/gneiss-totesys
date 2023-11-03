from ingestion.get_timestamp import (
    get_latest_timestamp,
    get_filename_for_table_data,
)
import pytest
import boto3
import os
import json
from moto import mock_s3

TEST_BUCKET_NAME = "ingestion-test-bucket"
os.environ["S3_DATA_ID"] = TEST_BUCKET_NAME


@pytest.fixture
def s3_boto():
    with mock_s3():
        s3 = boto3.client("s3")
        location = {"LocationConstraint": "eu-west-2"}
        s3.create_bucket(
            Bucket=TEST_BUCKET_NAME, CreateBucketConfiguration=location
        )
        yield s3


def test_get_latest_timestamp_returns_default_when_no_files_for_table(s3_boto):
    previous_timestamp = get_latest_timestamp("sales_order")
    assert previous_timestamp == "2000-01-01 00:00:00.000"


def test_get_latest_timestamp_returns_most_recent_timestamp_for_table(s3_boto):
    s3_boto.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key="sales_order_2023-10-23 12:17:09.792.json",
        Body="abc",
    )
    s3_boto.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key="sales_order_2023-10-23 11:31:10.112.json",
        Body="abc",
    )
    s3_boto.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key="sales_order_2023-10-23 10:37:09.902.json",
        Body="abc",
    )
    previous_timestamp = get_latest_timestamp("sales_order")
    assert previous_timestamp == "2023-10-23 12:17:09.792"


def test_get_latest_timestamp_only_gets_results_for_desired_table(s3_boto):
    s3_boto.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key="sales_order_2023-10-23 12:17:09.792.json",
        Body="abc",
    )
    s3_boto.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key="purchase_order_2023-10-23 10:37:09.902.json",
        Body="abc",
    )
    previous_timestamp = get_latest_timestamp("purchase_order")
    assert previous_timestamp == "2023-10-23 10:37:09.902"


def test_get_filename_for_table_data_returns_filename_for_small_dataset():
    input_data = json.dumps(
        {
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
                "agreed_delivery_location_id",
            ],
            "record_count": 1,
            "data": [
                [
                    5030,
                    "2023-11-01 14:22:10.329",
                    "2023-11-01 14:22:10.329",
                    186,
                    11,
                    17,
                    51651,
                    3.25,
                    2,
                    "2023-11-06",
                    "2023-11-05",
                    27,
                ]
            ],
        }
    )

    filename = get_filename_for_table_data(input_data)
    assert filename == "sales_order_2023-11-01 14:22:10.329.json"


def test_get_filename_for_table_data_works_for_different_table_data():
    address_data = json.dumps(
        {
            "table_name": "address",
            "column_names": [
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
                "created_at",
                "last_updated",
            ],
            "record_count": 1,
            "data": [
                [
                    "6826 Herzog Via",
                    None,
                    "Avon",
                    "New Patienceburgh",
                    "28441",
                    "Turkey",
                    "1803 637401",
                    "2022-11-03 14:20:49.962",
                    "2022-11-03 14:20:49.962",
                ]
            ],
        }
    )

    filename = get_filename_for_table_data(address_data)
    assert filename == "address_2022-11-03 14:20:49.962.json"


def test_get_filename_for_table_data_raises_error_for_missing_keys(
    caplog,
):
    input_data = json.dumps(
        {
            "record_count": 1,
        }
    )

    with pytest.raises(ValueError) as e:
        get_filename_for_table_data(input_data)

    assert (
        e.value.args[0]
        == "get_filename_for_table_data: Required key missing in input."
    )


def test_get_filename_for_table_data_logs_error_for_missing_keys(caplog):
    try:
        input_data = json.dumps(
            {
                "record_count": 1,
            }
        )
        get_filename_for_table_data(input_data)
    except Exception:
        pass

    assert (
        "get_filename_for_table_data: Required key missing in input."
        in caplog.text
    )

    for log in caplog.records:
        if (
            log.message
            == "get_filename_for_table_data: Required key missing in input."
        ):
            assert log.levelname == "ERROR"


def test_get_filename_for_table_data_raises_error_for_wrong_data_type():
    input_data = {"record_count": 1}

    with pytest.raises(ValueError) as e:
        get_filename_for_table_data(input_data)

    assert (
        e.value.args[0]
        == "get_filename_for_table_data: Input must be string type."
    )


def test_get_filename_for_table_data_logs_error_for_wrong_data_type(caplog):
    try:
        input_data = {"record_count": 1}
        get_filename_for_table_data(input_data)
    except Exception:
        pass

    assert (
        "get_filename_for_table_data: Input must be string type."
        in caplog.text
    )

    for log in caplog.records:
        if (
            log.message
            == "get_filename_for_table_data: Input must be string type."
        ):
            assert log.levelname == "ERROR"


def test_get_filename_for_table_data_raises_error_for_empty_input():
    with pytest.raises(ValueError) as e:
        get_filename_for_table_data(None)

    assert e.value.args[0] == "get_filename_for_table_data: No input received."


def test_get_filename_for_table_data_logs_error_for_empty_input(caplog):
    try:
        get_filename_for_table_data(None)
    except Exception:
        pass

    assert "get_filename_for_table_data: No input received." in caplog.text

    for log in caplog.records:
        if log.message == "get_filename_for_table_data: No input received.":
            assert log.levelname == "ERROR"


def test_get_filename_for_table_data_raises_error_if_no_rows():
    data_with_empty_rows = json.dumps(
        {
            "table_name": "address",
            "column_names": [
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
                "created_at",
                "last_updated",
            ],
            "record_count": 10,
            "data": [],
        }
    )
    with pytest.raises(ValueError) as e:
        get_filename_for_table_data(data_with_empty_rows)

    assert (
        e.value.args[0] == "get_filename_for_table_data: 'data' has no rows."
    )


def test_get_filename_for_table_data_logs_error_if_no_rows(caplog):
    data_with_empty_rows = json.dumps(
        {
            "table_name": "address",
            "column_names": [
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
                "created_at",
                "last_updated",
            ],
            "record_count": 10,
            "data": [],
        }
    )
    try:
        get_filename_for_table_data(data_with_empty_rows)
    except Exception:
        pass

    assert "get_filename_for_table_data: 'data' has no rows." in caplog.text

    for log in caplog.records:
        if log.message == "get_filename_for_table_data: 'data' has no rows.":
            assert log.levelname == "ERROR"


def test_get_filename_for_table_data_raises_error_if_JSON_is_invalid():
    invalid_json_input = '{ "data: something, "other_thing": 0 "oops}'

    with pytest.raises(ValueError) as e:
        get_filename_for_table_data(invalid_json_input)

    assert (
        e.value.args[0]
        == "get_filename_for_table_data: Input not valid JSON format."
    )


def test_get_filename_for_table_data_logs_error_if_JSON_is_invalid(caplog):
    invalid_json_input = '{ "data: something, "other_thing": 0 "oops}'

    try:
        get_filename_for_table_data(invalid_json_input)
    except Exception:
        pass

    assert (
        "get_filename_for_table_data: Input not valid JSON format."
        in caplog.text
    )

    for log in caplog.records:
        if (
            log.message
            == "get_filename_for_table_data: Input not valid JSON format."
        ):
            assert log.levelname == "ERROR"


def test_get_filename_for_table_raises_error_if_last_updated_not_found():
    data_missing_last_update = json.dumps(
        {
            "table_name": "address",
            "column_names": [
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
                "created_at",
            ],
            "record_count": 10,
            "data": [
                [
                    "6826 Herzog Via",
                    None,
                    "Avon",
                    "New Patienceburgh",
                    "28441",
                    "Turkey",
                    "1803 637401",
                    "2022-11-03 14:20:49.962",
                    "2022-11-03 14:20:49.962",
                ]
            ],
        }
    )

    with pytest.raises(ValueError) as e:
        get_filename_for_table_data(data_missing_last_update)

    assert (
        e.value.args[0]
        == "get_filename_for_table_data: 'last_updated' not 'column_names'."
    )


def test_get_filename_for_table_logs_error_if_last_updated_not_found(caplog):
    data_missing_last_update = json.dumps(
        {
            "table_name": "address",
            "column_names": [
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
                "created_at",
            ],
            "record_count": 10,
            "data": [
                [
                    "6826 Herzog Via",
                    None,
                    "Avon",
                    "New Patienceburgh",
                    "28441",
                    "Turkey",
                    "1803 637401",
                    "2022-11-03 14:20:49.962",
                    "2022-11-03 14:20:49.962",
                ]
            ],
        }
    )

    try:
        get_filename_for_table_data(data_missing_last_update)
    except Exception:
        pass

    assert (
        "get_filename_for_table_data: 'last_updated' not 'column_names'."
        in caplog.text
    )

    for log in caplog.records:
        if log.message == (
            "get_filename_for_table_data: 'last_updated' not 'column_names'."
        ):
            assert log.levelname == "ERROR"
