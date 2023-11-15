from utils.custom_log import totesys_logger
from awswrangler import s3
from time import perf_counter
import pandas as pd
import boto3

log = totesys_logger()


def write_pq_to_s3(bucket: str, key: str, dataframe):
    """Writes dataframe contents to S3 as parquet
    Does not validate inputs
    """
    if key.endswith(".json"):
        key = key[:-5]

    if not key.endswith(".parquet"):
        key = f"{key}.parquet"
    key = key.replace("%3A", ":")
    log.info(f"writing to {bucket}/{key}")
    path = f"s3://{bucket}/{key}"

    parquet_bytes = dataframe.to_parquet()
    s3 = boto3.client("s3", region_name="eu-west-2")
    s3.put_object(Bucket=bucket, Key=key, Body=parquet_bytes)

    log.info(f"File stored: {path}")
