import boto3
import logging
import os
import json


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


def get_filename_for_table_data(input_data):
    """Returns filename to use when storing the supplied input data to s3.

    Args:
        data (str): JSON string. I.e. output of convert_psql_table_to_json.

    Raises:
        ValueError: if the input data does not include required keys or
                    is not proper format.

    Returns:
        str: file name to use e.g. "sales_order_2023-10-23 12:17:09.792.json"
    """
    logger = logging.getLogger("ingestion_logger")

    if message := table_data_is_invalid(input_data):
        message = "get_filename_for_table_data: " + message
        logger.error(message)
        logger.debug("input_data: " + str(input_data))
        raise ValueError(message)

    try:
        data = json.loads(input_data)

        filename = data["table_name"] + "_"
        timestamp = "2000-01-01 00:00:00.000"

        last_updated_index = data["column_names"].index("last_updated")

        for row in data["data"]:
            timestamp = max(timestamp, row[last_updated_index])

        filename += timestamp + ".json"

        logger.info(f"get_filename_for_table_data: '{filename}'")
        return filename
    except Exception as e:
        logger.error(e)


def table_data_is_invalid(input_data):
    """Checks the input JSON string includes all the data required to determine
       a new filename.

    Args:
        input_data (str): JSON string.

    Returns:
        str/bool: if validation fails, the reason is returned as string.
                  if validation passes, False is returned.
    """
    if input_data is None:
        return "No input received."
    if type(input_data) is not str:
        return "Input must be string type."

    try:
        data = json.loads(input_data)
    except json.decoder.JSONDecodeError:
        return "Input not valid JSON format."

    required_keys = ["table_name", "column_names", "data"]

    for key in required_keys:
        if key not in data:
            return "Required key missing in input."

    if len(data["data"]) == 0:
        return "'data' has no rows."

    if "last_updated" not in data["column_names"]:
        return "'last_updated' not 'column_names'."

    return False
