import pandas as pd
import os
from json_to_parquet.get_s3_file import json_event, bucket_list, json_S3_key
from json_to_parquet.dim_currency import currency_transform
from json_to_parquet.date_dimension import date_dimension
from json_to_parquet.write_pq_to_s3 import write_pq_to_s3
from utils.custom_log import totesys_logger
from utils.manifest import write_manifest
from json_to_parquet.transformations import (
    transform_address,
    transform_design,
    transform_sales_order,
    transform_payment,
    transform_purchase_order,
    transform_payment_type,
    transform_transaction,
)
from json_to_parquet.dim_counterparty import dim_counterparty
from json_to_parquet.dim_staff import dim_staff

out_table_lookup = {
    "address": "dim_location",
    "counterparty": "dim_counterparty",
    "currency": "dim_currency",
    "department": "dim_staff",
    "design": "dim_design",
    "payment": "fact_payment",
    "payment_type": "dim_payment_type",
    "purchase_order": "fact_purchase_order",
    "sales_order": "fact_sales_order",
    "staff": "dim_staff",
    "transaction": "dim_transaction",
}


log = totesys_logger()


def fake_fn(data) -> pd.DataFrame:
    log.info(f'Processing {data["table_name"]} not implemented. Quitting')
    return pd.DataFrame()


def lambda_handler(event, _):
    log.info(event)
    input_manifest = json_event(event)
    log.info(f"Processing manifest: {input_manifest}")

    out_bucket = os.environ["PARQUET_S3_DATA_ID"]
    date_dim_key = "dim_date/dim_date.parquet"

    output_manifest = {"files": []}

    parquet_keys = bucket_list(out_bucket)
    if date_dim_key not in parquet_keys:
        log.info("Date parquet not found, creating...")
        df = date_dimension()
        output_file = write_pq_to_s3(out_bucket, date_dim_key, df)
        output_manifest["files"].append(output_file)
        log.info("Done")

    for input_file in input_manifest["files"]:
        output_file = process_file(input_file)

        if output_file:
            output_manifest["files"].append(output_file)

    log.info(f"{len(input_manifest['files'])}files processed.")

    write_manifest(out_bucket, output_manifest)


def process_file(in_key):
    function_dict = {
        "address": transform_address,
        "counterparty": dim_counterparty,
        "currency": currency_transform,
        "department": fake_fn,
        "design": transform_design,
        "payment": transform_payment,
        "payment_type": transform_payment_type,
        "purchase_order": transform_purchase_order,
        "sales_order": transform_sales_order,
        "staff": dim_staff,
        "transaction": transform_transaction,
    }
    out_bucket: str = os.environ["PARQUET_S3_DATA_ID"]
    in_bucket = os.environ["S3_DATA_ID"]
    json_body = json_S3_key(in_bucket, in_key)

    try:
        table_name = json_body["table_name"]
    except KeyError:
        log.error(f"Table_name does not exits in {in_key}")
        raise

    out_key = in_key.replace(table_name, out_table_lookup[table_name])
    log.info(f"Processing {table_name} to {out_bucket}/{out_key}")

    transformed_df: pd.DataFrame = function_dict[table_name](json_body)
    if not isinstance(transformed_df, pd.DataFrame):
        logstring = f"""
transformed_df is not a DataFrame whilst processing {table_name}"""
        log.error(logstring)
        raise ValueError(logstring)
    if len(transformed_df):
        log.info(f"Writing output to {out_bucket}/{out_key}")
        return write_pq_to_s3(out_bucket, out_key, transformed_df)
