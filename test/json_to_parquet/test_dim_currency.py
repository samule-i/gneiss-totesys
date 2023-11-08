from json_to_parquet.dim_currency import currency_transform
import pytest
import pandas as pd

def test_gives_correct_format(currency_json):
    result = currency_transform(currency_json)

    assert result.loc[0, 'currency_id'] == 1
    assert result.loc[0, 'currency_code'] == 'GBP'
    assert result.loc[0, 'currency_name'] == 'Great British Pound'


def test_raise_error_if_given_wrong_table():
    stored_data = {
        "table_name": "users",
        "column_names": [
            "currency_id",
            "currency_code",
            "created_at",
            "last_updated"
        ],
        "record_count": 1,
        "data": [
            [
                1,
                "GBP",
                "2022-11-03T14:20:49.962000",
                "2022-11-03T14:20:49.962000"
            ]
        ]
    }
    with pytest.raises(ValueError):
        currency_transform(stored_data)


def test_raise_error_if_given_an_unexpected_row_value():
    stored_data = {
        "table_name": "currency",
        "column_names": [
            "currency_id",
            "currency_code",
            "created_at",
            "last_updated"
        ],
        "record_count": 1,
        "data": [
            [
                1,
                "GOLD",
                "2022-11-03T14:20:49.962000",
                "2022-11-03T14:20:49.962000"
            ]
        ]
    }
    with pytest.raises(KeyError):
        currency_transform(stored_data)
