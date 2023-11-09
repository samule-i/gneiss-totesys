import boto3
from botocore.exceptions import ClientError
import json
from json_to_parquet.custom_log import logger
log = logger(__name__)


def json_event(event: dict) -> dict:
    '''Returns a json-like dict from an S3 bucket

    bucket must exist & key must match a json file that exists.
    '''
    bucket = event['Records'][0]["s3"]["bucket"]["name"]
    key = event['Records'][0]["s3"]["object"]["key"]
    data = json_S3_key(bucket, key)
    return data


def json_S3_key(bucket: str, key: str) -> dict:
    '''Returns a json-like dict from an S3 bucket

    bucket must exist & key must match a json file that exists.
    '''
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        bytes = response['Body']
    except ClientError as e:
        log.error(f'{e.response["Error"]["Code"]}')
        log.info(f'File: {bucket}/{key} is unavailable')
        raise (e)

    data = bytes.read().decode('utf-8')
    json_data = json.loads(data)
    return json_data


def bucket_list(bucket) -> list[str]:
    '''returns a list of keys from the bucket containing parquet files
    '''
    client = boto3.client('s3')
    try:
        response = client.list_objects_v2(Bucket=bucket)
    except ClientError as e:
        log.error(f'{e.response["Error"]["Code"]}')
        log.info(f'File: {bucket} is unavailable')
        raise (e)

    keys = [item['Key'] for item in response.get('Contents', [])]
    return keys


def keys_of_specific_table(bucket, table):
    pass


def json_from_row_id(bucket: str, table_name: str, idx: int) -> list:
    '''Reads the path of the JSON file that contains the row
    with the index provided
    '''
    client = boto3.client('s3')
    try:
        response = client.get_object(
            Bucket=bucket,
            Key=f'.id_lookup/{table_name}/{idx}'  # ../ingestion/write_JSON.py
        )
        bytes = response['Body'].read()
        response = client.get_object(
            Bucket=bucket,
            Key=bytes.decode('utf-8')
        )
        bytes = response['Body'].read()
        return json.loads(bytes.decode())
    except ClientError as e:
        log.error(f'{e.response["Error"]["Code"]}')
        log.info(f'File: {bucket} is unavailable')
        raise (e)
