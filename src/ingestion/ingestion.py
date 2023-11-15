import os
from datetime import datetime as dt
from utils.db_credentials import get_credentials
from utils.pg8000_conn import get_conn
from utils.custom_log import totesys_logger
from utils.manifest import write_manifest
from ingestion.write_JSON import write_to_ingestion, write_lookup
from ingestion.get_timestamp import (
    get_last_ingestion_timestamp as get_timestamp,
    update_last_ingestion_timestamp,
)
from ingestion.rows_to_json import rows_to_json, totesys_tables
import json

log = totesys_logger()


def lambda_handler(event, context):
    """Should connect to a PGSQL database
    collect data from connection
    update an S3 bucket with files created from data"""
    manifest = {"files": []}
    new_timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    bucket_name = os.environ["S3_DATA_ID"]
    credentials = get_credentials("db_credentials_oltp")
    prev_timestamp = get_timestamp()
    conn = get_conn(credentials)
    log.info(f"Processing files now: querying from {prev_timestamp} ⌚")
    for table in totesys_tables:
        formatted_ts = f"{prev_timestamp}.000"
        data = rows_to_json(table, formatted_ts, conn)
        s3_key = write_to_ingestion(data, bucket_name)
        log.info(f"s3_key: {s3_key}")
        write_lookup(json.loads(data), bucket_name, s3_key)
        log.info("Processing files now: written lookup")
        if s3_key is not None:
            log.info(f"Added to manifest: {s3_key}")
            manifest["files"].append(s3_key)
    log.info("updating last seen timestamp...")
    update_last_ingestion_timestamp(new_timestamp)
    write_manifest(bucket_name, manifest)
    log.info("Closing db connection...")
    conn.close()
    log.info("Done")
    return {"statusCode": 200}
