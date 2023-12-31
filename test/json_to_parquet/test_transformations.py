import json
from json_to_parquet.transformations import (
    transform_sales_order,
    transform_address,
    transform_design,
)
from ingestion.rows_to_json import CustomEncoder
import pytest


def test_sales_order_returns_correct_columns_and_rows():
    json_data_sales_order = json.dumps(
        {
            "table_name": "sales_order",
            "column_names": [
                "sales_order_id",
                "created_at",
                "last_updated",
                "staff_id",
                "counterparty_id",
                "units_sold",
                "unit_price",
                "currency_id",
                "design_id",
                "agreed_payment_date",
                "agreed_delivery_date",
                "agreed_delivery_location_id",
            ],
            "record_count": 3,
            "data": [
                [
                    5030,
                    "2023-11-01 14:22:10.329000",
                    "2023-11-01 14:22:10.329000",
                    186,
                    11,
                    51651,
                    3.25,
                    2,
                    186,
                    "2023-11-06",
                    "2023-11-05",
                    27,
                ],
                [
                    5029,
                    "2023-11-01 14:22:10.124000",
                    "2023-11-01 14:22:10.124000",
                    39,
                    13,
                    57395,
                    3.49,
                    1,
                    39,
                    "2023-11-04",
                    "2023-11-02",
                    26,
                ],
                [
                    5028,
                    "2023-11-01 13:33:10.231000",
                    "2023-11-01 13:33:10.231000",
                    229,
                    20,
                    34701,
                    3.97,
                    1,
                    229,
                    "2023-11-03",
                    "2023-11-04",
                    28,
                ],
            ],
        },
        indent=4,
        cls=CustomEncoder,
    )

    df_sales_order = transform_sales_order(json_data_sales_order)

    expected_columns = [
        "sales_record_id",
        "sales_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "sales_staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_payment_date",
        "agreed_delivery_date",
        "agreed_delivery_location_id",
    ]

    expected_rows = [
        [1,
            5030,
            "2023-11-01",
            "14:22:10.329000",
            "2023-11-01",
            "14:22:10.329000",
            186,
            11,
            51651,
            3.25,
            2,
            186,
            "2023-11-06",
            "2023-11-05",
            27,
         ],
        [2,
            5029,
            "2023-11-01",
            "14:22:10.124000",
            "2023-11-01",
            "14:22:10.124000",
            39,
            13,
            57395,
            3.49,
            1,
            39,
            "2023-11-04",
            "2023-11-02",
            26,
         ],
        [3,
            5028,
            "2023-11-01",
            "13:33:10.231000",
            "2023-11-01",
            "13:33:10.231000",
            229,
            20,
            34701,
            3.97,
            1,
            229,
            "2023-11-03",
            "2023-11-04",
            28,
         ],
    ]
    df_sales_order['created_date'] = df_sales_order[
        'created_date'].astype(str)
    df_sales_order['created_time'] = df_sales_order[
        'created_time'].astype(str)
    df_sales_order['last_updated_date'] = df_sales_order[
        'last_updated_date'].astype(str)
    df_sales_order['last_updated_time'] = df_sales_order[
        'last_updated_time'].astype(str)
    df_sales_order['agreed_payment_date'] = df_sales_order[
        'agreed_payment_date'].astype(
        str)
    df_sales_order['agreed_delivery_date'] = df_sales_order[
        'agreed_delivery_date'].astype(
        str)

    assert list(df_sales_order.columns) == expected_columns
    assert list(df_sales_order.values[0]) == expected_rows[0]
    assert list(df_sales_order.values[1]) == expected_rows[1]
    assert list(df_sales_order.values[2]) == expected_rows[2]


def test_transform_sales_order_handles_value_error():
    incorrect_data = json.dumps(
        {
            "table_name": "staff",
            "column_names": [
                "staff_id",
                "first_name",
                "last_name",
                "department_id",
                "email_address",
                "created_at",
                "last_updated",
            ],
            "record_count": 3,
            "data": [
                [
                    1,
                    "John",
                    "Doe",
                    1,
                    "john.doe@example.com",
                    "2023-11-08T12:00:00",
                    "2023-11-08T12:00:00",
                ],
                [
                    2,
                    "Jane",
                    "Smith",
                    2,
                    "jane.smith@example.com",
                    "2023-11-08T12:01:00",
                    "2023-11-08T12:01:00",
                ],
                [
                    3,
                    "Mike",
                    "Johnson",
                    1,
                    "mike.johnson@example.com",
                    "2023-11-08T12:02:00",
                    "2023-11-08T12:02:00",
                ],
            ],
        },
        indent=4,
        cls=CustomEncoder,
    )
    with pytest.raises(ValueError):
        transform_sales_order(incorrect_data)


def test_transform_sales_order_exceptions():
    incorrect_data = json.dumps(
        {
            "table_name": "sales_order",
            "column_names": [
                "created_at",
                "last_updated",
                "staff_id",
                "counterparty_id",
                "units_sold",
                "unit_price",
                "currency_id",
                "design_id",
                "agreed_payment_date",
                "agreed_delivery_date",
                "agreed_delivery_location_id",
            ],
            "record_count": 1,
            "data": [
                [
                    "2023-11-01 14:22:10.329000",
                    "2023-11-01 14:22:10.329000",
                    186,
                    11,
                    51651,
                    3.25,
                    2,
                    186,
                    "2023-11-06",
                    "2023-11-05",
                    27,
                ]

            ],
        },
        indent=4,
        cls=CustomEncoder,
    )
    with pytest.raises(Exception):
        transform_sales_order(incorrect_data)


def test_transform_address_returns_correct_columns_and_rows():
    address_data = json.dumps(
        {
            "table_name": "address",
            "column_names": [
                "address_id",
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
            "record_count": 3,
            "data": [
                [
                    1,
                    "123 Main St",
                    "Apt 45",
                    "Downtown",
                    "New York",
                    "10001",
                    "USA",
                    "555-123-4567",
                    "2023-11-08T12:00:00",
                    "2023-11-08T12:00:00",
                ],
                [
                    2,
                    "456 Elm Rd",
                    "Suite 101",
                    "Midtown",
                    "Los Angeles",
                    "90001",
                    "USA",
                    "555-789-0123",
                    "2023-11-08T12:01:00",
                    "2023-11-08T12:01:00",
                ],
                [
                    3,
                    "789 Oak Ave",
                    "Apt 202",
                    "Uptown",
                    "Chicago",
                    "60601",
                    "USA",
                    "555-234-5678",
                    "2023-11-08T12:02:00",
                    "2023-11-08T12:02:00",
                ],
            ],
        },
        indent=4,
        cls=CustomEncoder,
    )
    expected_columns = [
        "location_id",
        "address_line_1",
        "address_line_2",
        "district",
        "city",
        "postal_code",
        "country",
        "phone",
    ]
    expected_rows = [
        [
            1,
            "123 Main St",
            "Apt 45",
            "Downtown",
            "New York",
            "10001",
            "USA",
            "555-123-4567",
        ],
        [
            2,
            "456 Elm Rd",
            "Suite 101",
            "Midtown",
            "Los Angeles",
            "90001",
            "USA",
            "555-789-0123",
        ],
        [
            3,
            "789 Oak Ave",
            "Apt 202",
            "Uptown",
            "Chicago",
            "60601",
            "USA",
            "555-234-5678",
        ],
    ]
    addressdf = transform_address(address_data)
    assert list(addressdf.columns) == expected_columns
    assert list(addressdf.values[0]) == expected_rows[0]
    assert list(addressdf.values[1]) == expected_rows[1]
    assert list(addressdf.values[2]) == expected_rows[2]


def test_transform_address_handles_value_error():
    incorrect_data = json.dumps(
        {
            "table_name": "staff",
            "column_names": [
                "staff_id",
                "first_name",
                "last_name",
                "department_id",
                "email_address",
                "created_at",
                "last_updated",
            ],
            "record_count": 3,
            "data": [
                [
                    1,
                    "John",
                    "Doe",
                    1,
                    "john.doe@example.com",
                    "2023-11-08T12:00:00",
                    "2023-11-08T12:00:00",
                ],
                [
                    2,
                    "Jane",
                    "Smith",
                    2,
                    "jane.smith@example.com",
                    "2023-11-08T12:01:00",
                    "2023-11-08T12:01:00",
                ],
                [
                    3,
                    "Mike",
                    "Johnson",
                    1,
                    "mike.johnson@example.com",
                    "2023-11-08T12:02:00",
                    "2023-11-08T12:02:00",
                ],
            ],
        },
        indent=4,
        cls=CustomEncoder,
    )
    with pytest.raises(ValueError):
        transform_address(incorrect_data)


def test_transform_address_handles_exceptions():
    incorrect_data = json.dumps(
        {
            "table_name": "address",
            "column_names": [
                "address_id",
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
                    1,
                    "Apt 45",
                    "Downtown",
                    "New York",
                    "10001",
                    "USA",
                    "555-123-4567",
                    "2023-11-08T12:00:00",
                    "2023-11-08T12:00:00",
                ]
            ],
        },
        indent=4,
        cls=CustomEncoder,
    )
    with pytest.raises(Exception):
        transform_address(incorrect_data)


def test_transform_design_returns_the_correct_columns_and_rows():
    design_data = json.dumps(
        {
            "table_name": "design",
            "column_names": [
                "design_id",
                "created_at",
                "last_updated",
                "design_name",
                "file_location",
                "file_name",
            ],
            "record_count": 3,
            "data": [
                [
                    1,
                    "2023-11-08T12:00:00",
                    "2023-11-08T12:00:00",
                    "Design 1",
                    "/designs/1",
                    "design1.jpg",
                ],
                [
                    2,
                    "2023-11-08T12:01:00",
                    "2023-11-08T12:01:00",
                    "Design 2",
                    "/designs/2",
                    "design2.jpg",
                ],
                [
                    3,
                    "2023-11-08T12:02:00",
                    "2023-11-08T12:02:00",
                    "Design 3",
                    "/designs/3",
                    "design3.jpg",
                ],
            ],
        },
        indent=4,
        cls=CustomEncoder,
    )

    expected_columns = [
        "design_id",
        "design_name",
        "file_location",
        "file_name",
    ]

    expected_rows = [
        [1, "Design 1", "/designs/1", "design1.jpg"],
        [2, "Design 2", "/designs/2", "design2.jpg"],
        [3, "Design 3", "/designs/3", "design3.jpg"],
    ]
    designdf = transform_design(design_data)
    assert list(designdf.columns) == expected_columns
    assert list(designdf.values[0]) == expected_rows[0]
    assert list(designdf.values[1]) == expected_rows[1]
    assert list(designdf.values[2]) == expected_rows[2]


def test_transform_design_handles_value_error():
    incorrect_data = json.dumps(
        {
            "table_name": "staff",
            "column_names": [
                "staff_id",
                "first_name",
                "last_name",
                "department_id",
                "email_address",
                "created_at",
                "last_updated",
            ],
            "record_count": 3,
            "data": [
                [
                    1,
                    "John",
                    "Doe",
                    1,
                    "john.doe@example.com",
                    "2023-11-08T12:00:00",
                    "2023-11-08T12:00:00",
                ],
                [
                    2,
                    "Jane",
                    "Smith",
                    2,
                    "jane.smith@example.com",
                    "2023-11-08T12:01:00",
                    "2023-11-08T12:01:00",
                ],
                [
                    3,
                    "Mike",
                    "Johnson",
                    1,
                    "mike.johnson@example.com",
                    "2023-11-08T12:02:00",
                    "2023-11-08T12:02:00",
                ],
            ],
        },
        indent=4,
        cls=CustomEncoder,
    )
    with pytest.raises(ValueError):
        transform_design(incorrect_data)


def test_transform_design_exception_handling():
    incorrect_data = json.dumps(
        {
            "table_name": "design",
            "column_names": [
                "design_id",
                "created_at",
                "last_updated",
                "design_name",
                "file_location",
            ],
            "record_count": 3,
            "data": [
                [
                    1,
                    "2023-11-08T12:00:00",
                    "2023-11-08T12:00:00",
                    "Design 1",
                    "/designs/1",
                ],
            ],
        },
        indent=4,
        cls=CustomEncoder,
    )

    with pytest.raises(Exception):
        transform_design(incorrect_data)
