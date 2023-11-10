import boto3
from json_to_parquet.custom_log import logger
log = logger(__name__)


def write_pq_to_s3(bucket: str, key: str, dataframe):
    '''Writes dataframe contents to S3 as parquet
    Does not validate inputs
    '''
    if key.endswith('.json'):
        key = key[:-5]

    if not key.endswith('.parquet'):
        key = f'{key}.parquet'
    key = key.replace('%3A', ':')
    log.info(f'writing to {bucket}/{key}')
    client = boto3.client('s3')
    dataframe.to_parquet('/tmp/df.parquet')
    client.put_object(Bucket=bucket,
                      Key=key,
                      Body='/tmp/write_file.parquet')
