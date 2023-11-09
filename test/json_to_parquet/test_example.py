from src.json_to_parquet.example import examplefn


def test_example_works():
    assert examplefn(1) == 'example'
    assert examplefn(None) is None
