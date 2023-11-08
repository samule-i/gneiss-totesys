import pytest
import json


@pytest.fixture(scope='function')
def fake_event():
    with open('test/_fake_events/put_file.json') as file:
        data = file.read()
    event = json.loads(data)
    return event
