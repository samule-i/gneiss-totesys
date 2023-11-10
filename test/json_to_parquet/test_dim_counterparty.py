import json
from json_to_parquet.dim_counterparty import dim_counterparty
from moto import mock_s3
import boto3
import os


@mock_s3
def test_values_return_correctly():
    bucket = 'abcde'
    os.environ['S3_DATA_ID'] = bucket
    with open('test/sample_jsons/sample_address.json', 'r') as file:
        address = file.read()
    with open('test/sample_jsons/sample_counterparty.json', 'r') as file:
        counterparty = file.read()

    client = boto3.client('s3')
    client = boto3.client('s3')
    client.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    client.put_object(
        Bucket=bucket,
        Key='address/file.json',
        Body=address
    )
    client.put_object(
        Bucket=bucket,
        Key='.id_lookup/address/1',
        Body='address/file.json'
    )
    client.put_object(
        Bucket=bucket,
        Key='.id_lookup/address/2',
        Body='address/file.json'
    )

    df = dim_counterparty(counterparty)
    result = df.loc[df['counterparty_legal_name'] == 'test_name1']
    result = result['counterparty_legal_country'].item()
    assert result == 'fake country'

    result = df.loc[df['counterparty_legal_name'] == 'test_name2']
    result = result['counterparty_legal_country'].item()
    assert result == 'test country'


@mock_s3
def test_drops_unused_addresses():
    bucket = 'abcde'
    os.environ['S3_DATA_ID'] = bucket
    with open('test/sample_jsons/sample_address.json', 'r') as file:
        address = file.read()
    with open('test/sample_jsons/sample_counterparty.json', 'r') as file:
        counterparty = file.read()

    as_json = json.loads(address)
    as_json['data'].append([
        99,
        "I",
        None,
        "SHOULD",
        "NOT",
        "BE",
        "HERE",
        "NOVAL",
        "2022-11-03T14:20:49.962000",
        "2022-11-03T14:20:49.962000"
    ])

    client = boto3.client('s3')
    client = boto3.client('s3')
    client.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    client.put_object(
        Bucket=bucket,
        Key='address/file.json',
        Body=address
    )
    client.put_object(
        Bucket=bucket,
        Key='.id_lookup/address/1',
        Body='address/file.json'
    )
    client.put_object(
        Bucket=bucket,
        Key='.id_lookup/address/2',
        Body='address/file.json'
    )

    df = dim_counterparty(counterparty)
    result = df['counterparty_legal_phone_number']
    assert 'NOVAL' not in result
