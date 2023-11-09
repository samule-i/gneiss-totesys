import boto3
from botocore.exceptions import ClientError
import json
import logging
import os


log = logging.getLogger('get_s3_file')
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
log_fmt = logging.Formatter(
    '''%(levelname)s - %(message)s - %(name)s -
    %(module)s/%(funcName)s()''')
handler.setFormatter(log_fmt)
log.addHandler(handler)


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


def bucket_list() -> list[str]:
    '''returns a list of keys from the bucket containing parquet files
    '''
    client = boto3.client('s3')
    bucket = os.environ['PARQUET_S3_DATA_ID']
    try:
        response = client.list_objects_v2(
            Bucket=bucket)
    except ClientError as e:
        log.error(f'{e.response["Error"]["Code"]}')
        log.info(f'File: {bucket} is unavailable')
        raise (e)

    keys = [item['Key'] for item in response['Contents']]
    return keys
