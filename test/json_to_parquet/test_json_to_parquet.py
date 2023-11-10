from moto import mock_s3
import boto3
import json
import os
from unittest.mock import patch
from json_to_parquet.json_to_parquet import lambda_handler


os.environ['PARQUET_S3_DATA_ID'] = 'test_bucket'


@mock_s3
def test_currency_fn_called_for_json(currency_event, currency_json):
    out_bucket = 'test_bucket'
    in_bucket = currency_event['Records'][0]['s3']['bucket']['name']
    in_key = currency_event['Records'][0]['s3']['object']['key']

    s3_client = boto3.client('s3')
    s3_client.create_bucket(
        Bucket=in_bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    s3_client = boto3.client('s3')

    s3_client.create_bucket(
        Bucket=out_bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )

    s3_client.put_object(
        Bucket=in_bucket,
        Key=in_key,
        Body=json.dumps(currency_json)
    )
    with patch('json_to_parquet.dim_currency.currency_transform') as mocked:
        lambda_handler(currency_event, '')
        assert mocked.assert_called
