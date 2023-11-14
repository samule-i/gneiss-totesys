from utils.db_credentials import get_credentials
from utils.pg8000_conn import get_conn
from utils.custom_log import totesys_logger
from parquet_to_olap.get_parquet_s3_file import parquet_event
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
    df = parquet_event(event)
    credentials = get_credentials("db_credentials_olap")
    conn = get_conn(credentials)

    event_key = event["Records"][0]["s3"]["object"]["key"]
    forward_slash_index = event_key.find("/")
    table_name = event_key[:forward_slash_index]
    log.info(f"file: {event_key}")
    try:
        if table_name not in olap_table_names:
            log.error(f"Table name not recognised in event {event}")
            raise ValueError(f"Table name not recognised in event {event}")

        parquet_to_sql(df, table_name, conn)
    except Exception as e:
        log.error(f"{e}")
        raise e
    finally:
        log.info("Closing connection...")
        conn.close()

    log.info(f"File processed successfully: {event_key}")
    return {"statusCode": 200}
