
import logging
from botocore.exceptions import ClientError
import boto3

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.ERROR)


def write_to_ingestion(data, bucket, key):
    """moves a JSON to a S3 Bucket.

    Keyword arguments:
    data -- the JSON data
    bucket -- S3 Bucket to be used
    key -- the name of the object
    """
    try:
        s3 = boto3.client('s3', region_name='eu-west-2')
        s3.put_object(Bucket=bucket, Key=key, Body=data)
    except ClientError as c:
        if c.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f'No such bucket - {bucket}')
        else:
            raise
    except Exception as e:
        logger.error(e)
        raise RuntimeError
