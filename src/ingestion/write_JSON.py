import logging
from botocore.exceptions import ClientError
import boto3
from datetime import date, datetime
import json

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.ERROR)


def write_to_ingestion(data, bucket) -> str | None:
    """moves a JSON to a S3 Bucket.

    Keyword arguments:
    data -- the JSON data
    bucket -- S3 Bucket to be used
    key -- the name of the object
    """
    dict = json.loads(data)
    table_name = dict.get('table_name')
    date_today = date.today()
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    json_key = f"{table_name}/{date_today}/{current_time}.json"
    try:
        s3 = boto3.client('s3', region_name='eu-west-2')
        if dict.get("record_count", 0) == 0:
            logger.info(f"write_to_ingestion: no records for '{table_name}'.")
            return
        s3.put_object(
            Bucket=bucket,
            Key=json_key,
            Body=data)
    except ClientError as c:
        if c.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f'No such bucket - {bucket}')
        else:
            raise
    except Exception as e:
        logger.error(e)
        raise RuntimeError
    logger.info(f'Completed writing to: {json_key} ')
    return json_key


def write_lookup(json_body: dict, bucket_name: str, json_key: str):
    '''Writes the s3 key to 's3://bucket/.lookup/table/id'
    '''
    s3 = boto3.client('s3')
    table = json_body['table_name']
    rows = json_body['data']
    logger.info(f'Writing to {table}...')
    count = 0
    table_index_lookup = f'.id_lookup/{table}.json.notrigger'
    try:
        response = s3.get_object(
            Bucket=bucket_name,
            Key=table_index_lookup
        )
        body = response['Body'].read()
        body = json.loads(body)
    except ClientError:
        body = {
            'table_name': table,
            'indexes': {}
        }

    for row in rows:
        id = row[0]
        body['indexes'][id] = json_key
        logger.info(f'Writing {table}/{json_key} to {table_index_lookup}')
        count += 1
    s3.put_object(
        Body=json.dumps(body),
        Bucket=bucket_name,
        Key=table_index_lookup)

    logger.info(f'Wrote {count} entries')
