from ingestion.ingestion import lambda_handler


def test_ingestion():
    assert lambda_handler('', '') is True
