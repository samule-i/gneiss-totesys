from json_to_parquet.dim_currency import currency_transform


def test_gives_correct_format():
    stored_data = {
        "table_name": "currency",
        "column_names": [
            "currency_id",
            "currency_code",
            "created_at",
            "last_updated"
        ],
        "record_count": 3,
        "data": [
            [
                1,
                "GBP",
                "2022-11-03T14:20:49.962000",
                "2022-11-03T14:20:49.962000"
            ],
            [
                2,
                "USD",
                "2022-11-03T14:20:49.962000",
                "2022-11-03T14:20:49.962000"
            ],
            [
                3,
                "EUR",
                "2022-11-03T14:20:49.962000",
                "2022-11-03T14:20:49.962000"
            ]
        ]
    }
    result = currency_transform(stored_data)
    expected = {
        'currency_id': [1, 2, 3],
        'currency_code': ['GBP', 'USD', 'EUR'],
        'currency_name': ['Great British Pound', 'US Dollar', 'Euro']
    }
    assert result == expected
