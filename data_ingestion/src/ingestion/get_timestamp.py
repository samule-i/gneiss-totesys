import boto3
import logging
import os


def get_latest_timestamp(table_name):
    """Return the timestamp from the most recent JSON file for the given
       table_name. If there are no files, a date far in the past is returned.

    Args:
        table_name (str): table name to check for JSON files

    Returns:
        str: timestamp string in the form of YYYY-MM-DD HH:MM:SS:SSS
    """
    logger = logging.getLogger("ingestion_logger")
    logger.info(f"get_latest_timestamp: {table_name}")

    s3_resource = boto3.resource("s3")
    previous_timestamp = "2000-01-01 00:00:00.000"
    bucket_name = os.environ["S3_DATA_ID"]

    for file in s3_resource.Bucket(bucket_name).objects.filter(
        Prefix=table_name
    ):
        file_timestamp = file.key[len(table_name) + 1:]
        file_timestamp = file_timestamp.replace(".json", "")
        previous_timestamp = max(previous_timestamp, file_timestamp)

    logger.info(
        f"get_latest_timestamp table: {table_name}, timestamp: "
        f"{previous_timestamp}"
    )
    return previous_timestamp
