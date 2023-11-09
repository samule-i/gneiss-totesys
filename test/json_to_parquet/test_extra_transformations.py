from ingestion.rows_to_json import CustomEncoder
import json
from json_to_parquet.transformations import transform_purchase_order


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
                      "2023-11-01 ",
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
    assert list(df_purchase_order.columns) == expected_columns
    assert df_purchase_order.values[0][0] == expected_rows[0][0]
    assert df_purchase_order.values[0][1] == expected_rows[0][1]
    assert df_purchase_order.values[0][2] == expected_rows[0][2]
