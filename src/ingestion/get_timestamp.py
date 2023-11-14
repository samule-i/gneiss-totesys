import boto3
from botocore.exceptions import ClientError
from utils.custom_log import totesys_logger
import json
import os

log = totesys_logger()


def get_last_ingestion_timestamp():
    """
    Get the last ingestion timestamp
    Returns:
        str: The last ingestion timestamp for the ingeston lambda
    """
    timestamp_key = 'timestamps.json.notrigger'
    try:
        s3 = boto3.client('s3', region_name='eu-west-2')
        response = s3.get_object(
            Bucket=os.environ['S3_DATA_ID'],
            Key=timestamp_key)
        json_data = response['Body'].read().decode('utf-8')
        timestamp = json.loads(json_data)
        log.info(f"Timestamp retrieved: {timestamp}")
        return timestamp['last_timestamp']
    except ClientError:
        return "1970-01-01 00:00:00"
    except Exception as e:
        log.error(f"Error: {str(e)}")
        raise e


def update_last_ingestion_timestamp(new_timestamp):
    """
    Update last ingestion timestamp  in timestamp.json.
    Args:
        new_timestamp (str): The new timestamp value to set by lamba handler.
    Returns:
        None
    """
    timestamp_key = 'timestamps.json.notrigger'
    try:
        s3 = boto3.client('s3', region_name='eu-west-2')
        timestamp = {
            "last_timestamp": new_timestamp}
        updated_json_data = json.dumps(timestamp)
        s3.put_object(
            Bucket=os.environ['S3_DATA_ID'],
            Key=timestamp_key,
            Body=updated_json_data
        )
        log.info(f"Timestamp stored: {new_timestamp}")
    except Exception as e:
        log.error(f'{e}')
        raise e
