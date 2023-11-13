from utils.custom_log import logger
from awswrangler import s3
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
    path = f's3://{bucket}/{key}'
    s3.to_parquet(
        df=dataframe,
        path=path
    )
