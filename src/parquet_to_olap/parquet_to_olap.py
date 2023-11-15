import os
from utils.db_credentials import get_credentials
from utils.pg8000_conn import get_conn
from utils.custom_log import totesys_logger
from parquet_to_olap.get_parquet_s3_file import (
    parquet_S3_key,
    read_json_file_from_bucket,
)
from parquet_to_olap.parquet_to_sql_transformation import (
    parquet_to_sql,
    olap_table_names,
)

log = totesys_logger()


def lambda_handler(event, context):
    """
    Function to be invoked upon parquet s3 bucket trigger.
    Writes parquet file to OLAP database.

    Args:
        event: AWS event
        context: Not necessary/optional
    """
    log.info(f"event: {event}")
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    manifest = read_json_file_from_bucket(bucket, key)
    log.info(f"Processing manifest: {manifest}")

    credentials = get_credentials("db_credentials_olap")
    conn = get_conn(credentials)

    for file_key in manifest["files"]:
        process_file(conn, bucket, file_key)

    log.info("Closing connection...")
    conn.close()

    return {"statusCode": 200}


def process_file(conn, bucket, file_key):
    df = parquet_S3_key(bucket, file_key)
    forward_slash_index = file_key.find("/")
    table_name = file_key[:forward_slash_index]
    log.info(f"Loading file: {file_key}")
    try:
        if table_name not in olap_table_names:
            log.error(f"Table name not recognised: {table_name}")
            raise ValueError(f"Table name not recognised: {table_name}")

        parquet_to_sql(df, table_name, conn)
    except Exception as e:
        log.error(f"{e}")
        raise e

    log.info(f"File processed successfully: {file_key}")
