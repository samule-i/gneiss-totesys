import logging
from botocore.exceptions import ClientError
import boto3
from datetime import date, datetime
import json

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.ERROR)


def write_to_ingestion(data, bucket):
    """moves a JSON to a S3 Bucket.

    Keyword arguments:
    data -- the JSON data
    bucket -- S3 Bucket to be used
    key -- the name of the object
    """
    try:
        s3 = boto3.client('s3', region_name='eu-west-2')
        dict = json.loads(data)
        table_name = dict.get('table_name')

        if dict.get("record_count", 0) == 0:
            logger.info(f"write_to_ingestion: no records for '{table_name}'.")
            return

        date_today = date.today()
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        s3.put_object(
            Bucket=bucket,
            Key=f"{table_name}/{date_today}/{current_time}.json",
            Body=data)
    except ClientError as c:
        if c.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f'No such bucket - {bucket}')
        else:
            raise
    except Exception as e:
        logger.error(e)
        raise RuntimeError
