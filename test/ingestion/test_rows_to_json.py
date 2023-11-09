from src.ingestion.rows_to_json import rows_to_json, CustomEncoder
from unittest.mock import Mock
import json
import pytest
import datetime
from decimal import Decimal


def test_rows_to_json_executes_correct_query_string():
    query_str = """SELECT * FROM sales_order WHERE
            CAST(last_updated AS TIMESTAMP) >
            CAST(:last_timestamp AS TIMESTAMP)"""
    conn = Mock()
    timestamp = "2023-11-06 10:00:00.000"
    rows_to_json("sales_order", timestamp, conn)

    conn.run.assert_called_once_with(query_str, last_timestamp=timestamp)


def test_rows_to_json_returns_JSON_string_with_correct_results():
    conn = Mock()
    conn.row_count = 3
    conn.columns = [
        {"name": "sales_order_id"},
        {"name": "created_at"},
        {"name": "last_updated"},
        {"name": "design_id"},
        {"name": "staff_id"},
        {"name": "counterparty_id"},
        {"name": "units_sold"},
        {"name": "unit_price"},
        {"name": "currency_id"},
        {"name": "agreed_delivery_date"},
        {"name": "agreed_payment_date"},
        {"name": "agreed_delivery_location_id"},
    ]
    conn.run.return_value = [
        [
            5030,
            datetime.datetime(2023, 11, 1, 14, 22, 10, 329000),
            datetime.datetime(2023, 11, 1, 14, 22, 10, 329000),
            186,
            11,
            17,
            51651,
            Decimal(3.25),
            2,
            datetime.date(2023, 11, 6),
            datetime.date(2023, 11, 5),
            27,
        ],
        [
            5029,
            datetime.datetime(2023, 11, 1, 14, 22, 10, 124000),
            datetime.datetime(2023, 11, 1, 14, 22, 10, 124000),
            39,
            13,
            7,
            57395,
            Decimal(3.49),
            1,
            datetime.date(2023, 11, 2),
            datetime.date(2023, 11, 4),
            26,
        ],
        [
            5028,
            datetime.datetime(2023, 11, 1, 13, 33, 10, 231000),
            datetime.datetime(2023, 11, 1, 13, 33, 10, 231000),
            229,
            20,
            13,
            34701,
            Decimal(3.97),
            1,
            datetime.date(2023, 11, 4),
            datetime.date(2023, 11, 3),
            28,
        ],
    ]
    timestamp = "2023-11-06 10:00:00.000"
    result = rows_to_json("sales_order", timestamp, conn)
    expected_result = json.dumps(
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
            "record_count": 3,
            "data": [
                [
                    5030,
                    datetime.datetime(2023, 11, 1, 14, 22, 10, 329000),
                    datetime.datetime(2023, 11, 1, 14, 22, 10, 329000),
                    186,
                    11,
                    17,
                    51651,
                    Decimal(3.25),
                    2,
                    datetime.date(2023, 11, 6),
                    datetime.date(2023, 11, 5),
                    27,
                ],
                [
                    5029,
                    datetime.datetime(2023, 11, 1, 14, 22, 10, 124000),
                    datetime.datetime(2023, 11, 1, 14, 22, 10, 124000),
                    39,
                    13,
                    7,
                    57395,
                    Decimal(3.49),
                    1,
                    datetime.date(2023, 11, 2),
                    datetime.date(2023, 11, 4),
                    26,
                ],
                [
                    5028,
                    datetime.datetime(2023, 11, 1, 13, 33, 10, 231000),
                    datetime.datetime(2023, 11, 1, 13, 33, 10, 231000),
                    229,
                    20,
                    13,
                    34701,
                    Decimal(3.97),
                    1,
                    datetime.date(2023, 11, 4),
                    datetime.date(2023, 11, 3),
                    28,
                ],
            ],
        },
        indent=4,
        cls=CustomEncoder,
    )
    assert result == expected_result


def test_rows_to_json_raises_error_for_invalid_timestamp():
    conn = Mock()
    with pytest.raises(ValueError):
        rows_to_json("sales_order", "abc", conn)


def test_rows_to_json_raises_error_for_invalid_table_name():
    conn = Mock()
    with pytest.raises(ValueError):
        rows_to_json("not_a_table", "2023-11-06 10:00:00.000", conn)
