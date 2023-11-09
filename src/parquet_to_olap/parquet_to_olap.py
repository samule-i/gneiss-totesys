import logging
from ingestion.db_credentials import get_credentials
from ingestion.pg8000_conn import get_conn
from parquet_to_olap.get_parquet_s3_file import parquet_event
from parquet_to_olap.parquet_to_sql_transformation import \
    parquet_to_sql, olap_table_names

log = logging.getLogger('parquet_to_olap_lambda_handler')
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
log_fmt = logging.Formatter(
    '''%(levelname)s - %(message)s - %(name)s -
    %(module)s/%(funcName)s()''')
handler.setFormatter(log_fmt)
log.addHandler(handler)


def lambda_handler(event, context):
    """
    Function to be invoked upon parquet s3 bucket trigger.
    Writes parquet file to OLAP database.

    Args:
        event: AWS event
        context: Not necessary/optional
    """
    df = parquet_event(event)
    credentials = get_credentials("db_credentials_olap")
    conn = get_conn(credentials)

    event_key = event['Records'][0]["s3"]["object"]["key"]
    forward_slash_index = event_key.find('/')
    table_name = event_key[:forward_slash_index]
    try:
        if table_name not in olap_table_names:
            log.error(f'Table name not recognised in event {event}')
            raise ValueError(f'Table name not recognised in event {event}')

        parquet_to_sql(df, table_name, conn)
    except Exception as e:
        log.error(f'{e}')
        raise e
    finally:
        conn.close()
