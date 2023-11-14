import boto3
from botocore.exceptions import ClientError
import json
from utils.custom_log import totesys_logger
log = totesys_logger()


def json_event(event: dict) -> dict:
    '''Returns a json-like dict from an S3 bucket

    bucket must exist & key must match a json file that exists.
    '''
    bucket = event['Records'][0]["s3"]["bucket"]["name"]
    key = event['Records'][0]["s3"]["object"]["key"]
    key = key.replace('%3A', ':')
    data = json_S3_key(bucket, key)
    return data


def json_S3_key(bucket: str, key: str) -> dict:
    '''Returns a json-like dict from an S3 bucket

    bucket must exist & key must match a json file that exists.
    '''
    key = key.replace('%3A', ':')
    log.info(f' [S3] Called with {bucket}&{key}')
    try:
        s3_client = boto3.client('s3')
        log.info(f'Accessing {bucket}/{key}')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        bytes = response['Body']
    except ClientError as e:
        log.error(f'{e.response["Error"]["Code"]}')
        log.info(f'File: {bucket}/{key} is unavailable')
        raise (e)
    log.info(f' [S3] completed with {bucket}&{key}')
    data = bytes.read().decode('utf-8')
    json_data = json.loads(data)
    return json_data


def bucket_list(bucket) -> list[str]:
    '''returns a list of keys from the bucket containing parquet files
    '''
    client = boto3.client('s3')
    try:
        log.info(f'Accessing {bucket}')
        response = client.list_objects_v2(Bucket=bucket)
    except ClientError as e:
        log.error(f'{e.response["Error"]["Code"]}')
        log.info(f'File: {bucket} is unavailable')
        raise (e)

    keys = [item['Key'] for item in response.get('Contents', [])]
    return keys


def key_from_row_id(bucket: str, table_name: str, idx: int | str) -> str:
    '''Reads the path of the JSON file that contains the row
    with the index provided
    '''
    client = boto3.client('s3')
    log.info(f'Getting key for {bucket}/{table_name}/{idx}')
    table_index_lookup = f'.id_lookup/{table_name}.json.notrigger'
    try:
        response = client.get_object(
            Bucket=bucket,
            Key=table_index_lookup  # ../ingestion/write_JSON.py
        )
        bytes = response['Body'].read()
        data = json.loads(bytes)
        wanted_key = data['indexes'][f'{idx}']
        return wanted_key
    except ClientError as e:
        log.error(f'{e.response["Error"]["Code"]}')
        log.info(
            f'''Key or Bucket: {bucket} is unavailable''')
        raise (e)


def json_from_row_id(bucket: str, table_name: str, idx: int) -> dict:
    '''Reads the path of the JSON file that contains the row
    with the index provided
    '''
    client = boto3.client('s3')
    log.info(f'Getting data for {bucket}/{table_name}/{idx}')
    try:
        key = key_from_row_id(bucket, table_name, idx)
        key = key.replace('%3A', ':')
        log.info(f'Found Key: {key}')
        response = client.get_object(
            Bucket=bucket,
            Key=key
        )
        bytes = response['Body'].read()
        return json.loads(bytes.decode())
    except ClientError as e:
        log.error(f'{e.response["Error"]["Code"]}')
        log.info(
            f'''Key or Bucket: {bucket} is unavailable''')
        raise (e)
