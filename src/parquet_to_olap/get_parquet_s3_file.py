from botocore.exceptions import ClientError
import boto3
import pandas as pd
from utils.custom_log import logger

log = logger()


def parquet_event(event: dict):
    """Returns a parquet file from an S3 bucket
    bucket must exist & key must match a parquet file that exists.
    """
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    key = key.replace("%3A", ":")
    data = parquet_S3_key(bucket, key)
    return data


def parquet_S3_key(bucket: str, key: str):
    """Returns a parquet file from an S3 bucket
    bucket must exist & key must match a parquet file that exists.
    """
    try:
        s3 = boto3.client("s3", region_name="eu-west-2")
        response = s3.get_object(Bucket=bucket, Key=key)
        data = response["Body"].read()
        with open("/tmp/df.parquet", "wb") as f:
            f.write(data)
        df = pd.read_parquet("/tmp/df.parquet")
        log.info(f"parquet file loaded: {bucket}/{key}")
    except ClientError as e:
        log.error(f"{e}")
        log.info(f"File: {bucket}/{key} is unavailable")
        raise (e)

    return df
