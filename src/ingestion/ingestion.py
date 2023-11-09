import os
from datetime import datetime as dt
import json
from ingestion.db_credentials import get_credentials
from ingestion.pg8000_conn import get_conn
from ingestion.write_JSON import write_to_ingestion, write_lookup
from ingestion.get_timestamp import (
    get_last_ingestion_timestamp as get_timestamp,
    update_last_ingestion_timestamp)
from ingestion.rows_to_json import (
    rows_to_json,
    totesys_tables)


def lambda_handler(event, context):
    '''Should connect to a PGSQL database
    collect data from connection
    update an S3 bucket with files created from data'''
    new_timestamp = dt.now().strftime('%Y-%m-%d %H:%M:%S')
    bucket_name = os.environ['S3_DATA_ID']
    credentials = get_credentials('db_credentials_oltp')
    prev_timestamp = get_timestamp()
    conn = get_conn(credentials)
    for table in totesys_tables:
        formatted_ts = f'{prev_timestamp}.000'
        data = rows_to_json(table, formatted_ts, conn)
        s3_key = write_to_ingestion(data, bucket_name)
        write_lookup(json.loads(data), bucket_name, s3_key)
    update_last_ingestion_timestamp(new_timestamp)
    conn.close()
    return {
        'statusCode': 200
    }
