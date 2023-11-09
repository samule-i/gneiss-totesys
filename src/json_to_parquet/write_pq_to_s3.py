import os
import boto3


def write_pq_to_s3(key: str, dataframe):
    '''Writes dataframe contents to S3
    '''
    if key.endswith('.json'):
        key = key[:-5]

    if not key.endswith('.parquet'):
        key = f'{key}.parquet'

    bucket = os.environ['DATA_BUCKET']
    client = boto3.client('s3')
    dataframe.to_parquet('/tmp/df.parquet')
    client.put_object(Bucket=bucket,
                      Key=key,
                      Body='/tmp/write_file.parquet')
