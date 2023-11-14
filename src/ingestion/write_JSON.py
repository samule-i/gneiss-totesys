from botocore.exceptions import ClientError
import boto3
from datetime import date, datetime
import json
from utils.custom_log import totesys_logger

log = totesys_logger()


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
            log.info(f"write_to_ingestion: no records for '{table_name}'.")
            return
        s3.put_object(
            Bucket=bucket,
            Key=json_key,
            Body=data)
    except ClientError as c:
        if c.response['Error']['Code'] == 'NoSuchBucket':
            log.error(f'No such bucket - {bucket}')
        else:
            raise
    except Exception as e:
        log.error(e)
        raise RuntimeError
    log.info(f'Completed writing to: {json_key} ')
    return json_key


def write_lookup(json_body: dict, bucket_name: str, json_key: str):
    '''Writes the s3 key to 's3://bucket/.lookup/table/id'
    '''
    s3 = boto3.client('s3')
    table = json_body['table_name']
    rows = json_body['data']
    log.info(f'Writing to {table}...')
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
    if not json_key:
        log.info('Nothing to write')
        return
    log.info(f'Writing {table}/{json_key} to {table_index_lookup}')
    for row in rows:
        id = row[0]
        body['indexes'][id] = json_key
        count += 1
    s3.put_object(
        Body=json.dumps(body),
        Bucket=bucket_name,
        Key=table_index_lookup)

    log.info(f'Wrote {count} entries')
