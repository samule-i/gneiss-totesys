import pytest
import json


@pytest.fixture(scope='function')
def currency_json():
    data = {
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
    return data


@pytest.fixture(scope='function')
def fake_event():
    with open('test/_fake_events/put_file.json') as file:
        data = file.read()
    event = json.loads(data)
    return event
