from ingestion.rows_to_json import CustomEncoder
import json
from json_to_parquet.transformations import (transform_purchase_order,
                                             transform_payment,
                                             transform_transaction,
                                             transform_payment_type)
import pytest


def test_purchase_order_returns_correct_columns_and_rows():
    json_data_purchase_order = json.dumps(
        {
            "table_name": "purchase_order",
            "column_names": [
                "purchase_order_id",
                "created_at",
                "last_updated",
                "staff_id",
                "counterparty_id",
                "item_code",
                "item_quantity",
                "item_unit_price",
                "currency_id",
                "agreed_delivery_date",
                "agreed_payment_date",
                "agreed_delivery_location_id"
            ],
            "record_count": 3,
            "data": [
                [
                    6010,
                    "2023-11-01 14:22:10.329000",
                    "2023-11-01 14:22:10.329000",
                    186,
                    11,
                    "ABC123",
                    10,
                    25.99,
                    2,
                    "2023-11-06",
                    "2023-11-05",
                    27
                ],
                [
                    6011,
                    "2023-11-01 14:22:10.124000",
                    "2023-11-01 14:22:10.124000",
                    39,
                    13,
                    "XYZ456",
                    5,
                    19.95,
                    1,
                    "2023-11-04",
                    "2023-11-02",
                    26
                ],
                [
                    6012,
                    "2023-11-01 13:33:10.231000",
                    "2023-11-01 13:33:10.231000",
                    229,
                    20,
                    "PQR789",
                    8,
                    34.50,
                    1,
                    "2023-11-03",
                    "2023-11-04",
                    28
                ]
            ]
        },
        indent=4,
        cls=CustomEncoder
    )
    df_purchase_order = transform_purchase_order(json_data_purchase_order)
    expected_columns = [
        "purchase_record_id",
        "purchase_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "staff_id",
        "counterparty_id",
        "item_code",
        "item_quantity",
        "item_unit_price",
        "currency_id",
        "agreed_delivery_date",
        "agreed_payment_date",
        "agreed_delivery_location_id",
    ]
    expected_rows = [[1,
                      6010,
                      "2023-11-01",
                      "14:22:10.329000",
                      "2023-11-01",
                      "14:22:10.329000",
                      186,
                      11,
                      "ABC123",
                      10,
                      25.99,
                      2,
                      "2023-11-06",
                      "2023-11-05",
                      27
                      ],
                     [2,
                      6011,
                      "2023-11-01",
                      "14:22:10.124000",
                      "2023-11-01",
                      "14:22:10.124000",
                      39,
                      13,
                      "XYZ456",
                      5,
                      19.95,
                      1,
                      "2023-11-04",
                      "2023-11-02",
                      26
                      ],
                     [3,
                      6012,
                      "2023-11-01",
                      "13:33:10.231000",
                      "2023-11-01",
                      "13:33:10.231000",
                      229,
                      20,
                      "PQR789",
                      8,
                      34.50,
                      1,
                      "2023-11-03",
                      "2023-11-04",
                      28
                      ]]
    df_purchase_order['created_date'] = df_purchase_order[
        'created_date'].astype(str)
    df_purchase_order['created_time'] = df_purchase_order[
        'created_time'].astype(str)
    df_purchase_order['last_updated_date'] = df_purchase_order[
        'last_updated_date'].astype(str)
    df_purchase_order['last_updated_time'] = df_purchase_order[
        'last_updated_time'].astype(str)
    df_purchase_order['agreed_delivery_date'] = df_purchase_order[
        'agreed_delivery_date'].astype(
        str)
    df_purchase_order['agreed_payment_date'] = df_purchase_order[
        'agreed_payment_date'].astype(
        str)

    assert list(df_purchase_order.columns) == expected_columns
    print(list(df_purchase_order.values[0]))

    assert list(df_purchase_order.values[0]) == expected_rows[0]
    assert list(df_purchase_order.values[1]) == expected_rows[1]
    assert list(df_purchase_order.values[2]) == expected_rows[2]


def test_transform_purchase_order_handles_value_error():
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
        transform_purchase_order(incorrect_data)


def test_transform_purchase_order_handles_exceptions():
    incorrect_data = json.dumps(
        {
            "table_name": "purchase_order",
            "column_names": [
                "purchase_order_id",
                "created_at",
                "last_updated",
                "staff_id",
                "counterparty_id",
                "item_quantity",
                "item_unit_price",
                "currency_id",
                "agreed_delivery_date",
                "agreed_payment_date",
                "agreed_delivery_location_id"
            ],
            "record_count": 3,
            "data": [
                [
                    6010,
                    "2023-11-01 14:22:10.329000",
                    "2023-11-01 14:22:10.329000",
                    186,
                    11,
                    10,
                    25.99,
                    2,
                    "2023-11-06",
                    "2023-11-05",
                    27
                ]

            ]
        },
        indent=4,
        cls=CustomEncoder
    )
    with pytest.raises(Exception):
        transform_purchase_order(incorrect_data)


def test_transform_payment_returns_correct_columns_and_rows():

    json_data_payment = json.dumps(
        {
            "table_name": "payment",
            "column_names": [
                "payment_id",
                "created_at",
                "last_updated",
                "transaction_id",
                "counterparty_id",
                "payment_amount",
                "currency_id",
                "payment_type_id",
                "paid",
                "payment_date",
                "company_ac_number",
                "counterparty_ac_number"
            ],
            "record_count": 3,
            "data": [
                [
                    7001,
                    "2023-11-01 14:22:10.329000",
                    "2023-11-01 14:22:10.329000",
                    301,
                    21,
                    500.50,
                    2,
                    1,
                    True,
                    "2023-11-06",
                    12345678,
                    87654321
                ],
                [
                    7002,
                    "2023-11-01 14:22:10.124000",
                    "2023-11-01 14:22:10.124000",
                    302,
                    15,
                    250.75,
                    1,
                    2,
                    False,
                    "2023-11-04",
                    98765432,
                    54321098
                ],
                [
                    7003,
                    "2023-11-01 13:33:10.231000",
                    "2023-11-01 13:33:10.231000",
                    303,
                    25,
                    1000.00,
                    1,
                    3,
                    True,
                    "2023-11-03",
                    11112222,
                    33334444
                ]
            ]
        },
        indent=4,
        cls=CustomEncoder
    )

    df_payment = transform_payment(json_data_payment)
    expected_columns = [
        "payment_record_id",
        "payment_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "transaction_id",
        "counterparty_id",
        "payment_amount",
        "currency_id",
        "payment_type_id",
        "paid",
        "payment_date",
    ]

    expected_rows = [
        [1,
            7001,
            "2023-11-01",
            "14:22:10.329000",
            "2023-11-01",
            "14:22:10.329000",
            301,
            21,
            500.50,
            2,
            1,
            True,
            "2023-11-06",
         ],
        [2,
            7002,
            "2023-11-01",
            "14:22:10.124000",
            "2023-11-01",
            "14:22:10.124000",
            302,
            15,
            250.75,
            1,
            2,
            False,
            "2023-11-04",
         ],
        [3,
            7003,
            "2023-11-01",
            "13:33:10.231000",
            "2023-11-01",
            "13:33:10.231000",
            303,
            25,
            1000.00,
            1,
            3,
            True,
            "2023-11-03",
         ]
    ]
    df_payment['created_date'] = df_payment[
        'created_date'].astype(str)
    df_payment['created_time'] = df_payment[
        'created_time'].astype(str)
    df_payment['last_updated_date'] = df_payment[
        'last_updated_date'].astype(str)
    df_payment['last_updated_time'] = df_payment[
        'last_updated_time'].astype(str)
    df_payment['payment_date'] = df_payment[
        'payment_date'].astype(
        str)

    assert list(df_payment.columns) == expected_columns
    assert list(df_payment.values[0]) == expected_rows[0]
    assert list(df_payment.values[1]) == expected_rows[1]
    assert list(df_payment.values[2]) == expected_rows[2]


def test_transform_payment_handles_value_error():
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
                ]
            ],
        },
        indent=4,
        cls=CustomEncoder,
    )
    with pytest.raises(ValueError):
        transform_payment(incorrect_data)


def test_transform_payment_handles_exceptions():
    incorrect_data = json.dumps(
        {
            "table_name": "payment",
            "column_names": [
                "payment_id",
                "created_at",
                "last_updated",
                "transaction_id",
                "counterparty_id",
                "payment_amount",
                "currency_id",
                "payment_type_id",
                "payment_date",
                "company_ac_number",
                "counterparty_ac_number"
            ],
            "record_count": 3,
            "data": [
                [
                    7001,
                    "2023-11-01 14:22:10.329000",
                    "2023-11-01 14:22:10.329000",
                    301,
                    21,
                    500.50,
                    2,
                    1,
                    "2023-11-06",
                    12345678,
                    87654321
                ]
            ]
        },
        indent=4,
        cls=CustomEncoder
    )
    with pytest.raises(Exception):
        transform_payment(incorrect_data)


def test_transform_transaction_returns_collect_columns_and_rows():
    json_transaction_data = json.dumps({
        "table_name": "transaction",
        "column_names": [
            "transaction_id",
            "transaction_type",
            "sales_order_id",
            "purchase_order_id",
            "created_at",
            "last_updated"
        ],
        "record_count": 3,
        "data": [
            [101, "SALE", 6010, 0,
             "2023-11-08T12:00:00", "2023-11-08T12:00:00"],
            [102, "PURCHASE", 0, 6011,
             "2023-11-08T12:01:00", "2023-11-08T12:01:00"],
            [103, "SALE", 6012, 0,
             "2023-11-08T12:02:00", "2023-11-08T12:02:00"]
        ]
    },
        indent=4,
        cls=CustomEncoder, )

    df_transactions = transform_transaction(json_transaction_data)

    expected_columns = [
        "transaction_id",
        "transaction_type",
        "sales_order_id",
        "purchase_order_id"
    ]

    expected_rows = [
        [101, "SALE", 6010, 0],
        [102, "PURCHASE", 0, 6011],
        [103, "SALE", 6012, 0]
    ]
    assert list(df_transactions.columns) == expected_columns
    assert list(df_transactions.values[0]) == expected_rows[0]
    assert list(df_transactions.values[1]) == expected_rows[1]
    assert list(df_transactions.values[2]) == expected_rows[2]


def test_transform_transaction_handles_value_error():
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
                ]
            ],
        },
        indent=4,
        cls=CustomEncoder,
    )
    with pytest.raises(ValueError):
        transform_transaction(incorrect_data)


def test_transform_transaction_handles_exceptions():
    incorrect_data = json.dumps({
        "table_name": "transaction",
        "column_names": [
            "transaction_id",
            "sales_order_id",
            "purchase_order_id",
            "created_at",
            "last_updated"
        ],
        "record_count": 1,
        "data": [
            [101, 6010, 0, "2023-11-08T12:00:00", "2023-11-08T12:00:00"]
        ]
    },
        indent=4,
        cls=CustomEncoder, )

    with pytest.raises(Exception):
        transform_transaction(incorrect_data)


def test_transform_payment_type_returns_collect_columns_and_rows():
    payment_type_data = json.dumps({
        "table_name": "payment_type",
        "column_names": [
            "payment_type_id",
            "payment_type_name",
            "created_at",
            "last_updated"
        ],
        "record_count": 3,
        "data": [
            [1, "SALES_RECEIPT",
             "2023-11-08T12:00:00", "2023-11-08T12:00:00"],
            [2, "SALESREFUND",
             "2023-11-08T12:01:00", "2023-11-08T12:01:00"],
            [3, "PURCHASE_PAYMENT",
             "2023-11-08T12:02:00", "2023-11-08T12:02:00"]
        ]
    },
        indent=4,
        cls=CustomEncoder, )
    df_payment_type = transform_payment_type(payment_type_data)

    expected_columns = [
        "payment_type_id",
        "payment_type_name"
    ]
    expected_rows = [
        [1, "SALES_RECEIPT"],
        [2, "SALESREFUND"],
        [3, "PURCHASE_PAYMENT"]
    ]
    assert list(df_payment_type.columns) == expected_columns
    assert list(df_payment_type.values[0]) == expected_rows[0]
    assert list(df_payment_type.values[1]) == expected_rows[1]
    assert list(df_payment_type.values[2]) == expected_rows[2]


def test_transform_payment_type_value_error():
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
                ]
            ],
        },
        indent=4,
        cls=CustomEncoder,
    )
    with pytest.raises(ValueError):
        transform_payment_type(incorrect_data)


def test_transform_payment_type_handles_exceptions():
    incorrect_data = json.dumps({
        "table_name": "payment_type",
        "column_names": [
            "payment_type_id",
            "created_at",
            "last_updated"
        ],
        "record_count": 1,
        "data": [
            [1, "2023-11-08T12:00:00", "2023-11-08T12:00:00"],

        ]
    },
        indent=4,
        cls=CustomEncoder, )
    with pytest.raises(Exception):
        transform_payment_type(incorrect_data)
