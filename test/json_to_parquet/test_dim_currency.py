from json_to_parquet.dim_currency import currency_transform
import pytest
from json_to_parquet.dim_currency import IncompatibleTableException
from copy import deepcopy


def test_gives_correct_format(currency_json):
    result = currency_transform(currency_json)
    expected = {
        'currency_id': [1, 2, 3],
        'currency_code': ['GBP', 'USD', 'EUR'],
        'currency_name': ['Great British Pound', 'US Dollar', 'Euro']
    }
    assert result == expected


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
    with pytest.raises(IncompatibleTableException):
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
