import pandas as pd
import os
from json_to_parquet.get_s3_file import json_event, bucket_list
from json_to_parquet.dim_currency import currency_transform
from json_to_parquet.date_dimension import date_dimension
from json_to_parquet.write_pq_to_s3 import write_pq_to_s3
from json_to_parquet.custom_log import logger
from json_to_parquet.transformations import (
    transform_address, transform_counterparty, transform_design,
    transform_sales_order, transform_staff)

log = logger(__name__)


def fake_fn():
    pass


def lambda_handler(event, _):
    out_bucket: str = os.environ['PARQUET_S3_DATA_ID']
    in_bucket: str = event['Records'][0]['s3']['bucket']['name']
    in_key = event['Records'][0]['s3']['object']['key']
    json_body = json_event(event)
    parquet_keys = bucket_list(out_bucket)
    table_name = json_body['table_name']

    if table_name == 'counterparty':
        get_address_jsons(json_body)

    function_dict = {
        'address': transform_address,
        'counterparty': transform_counterparty,
        'currency': currency_transform,
        'design': transform_design,
        'payment': fake_fn,
        'payment_type': fake_fn,
        'purchase_order': fake_fn,
        'sales_order': transform_sales_order,
        'staff': transform_staff,
        'transaction': fake_fn
    }

    out_table_lookup = {
        'address': 'dim_location',
        'counterparty': 'dim_counterparty',
        'currency': 'dim_currency',
        'department': 'dim_staff',
        'design': 'dim_design',
        'payment': 'fact_payment',
        'payment_type': 'dim_payment_type',
        'purchase_order': 'fact_purchase_order',
        'sales_order': 'fact_sales_order',
        'staff': 'dim_staff',
        'transaction': 'dim_transaction'
    }

    out_key = in_key.replace(table_name, out_table_lookup[table_name])

    date_dim_key = 'date/dim_date.parquet'
    if date_dim_key not in parquet_keys:
        df = date_dimension()
        write_pq_to_s3(out_bucket, date_dim_key, df)

    # example = table_name/2000-01-01/01 01 01.json

    transformed_df: pd.DataFrame = function_dict[table_name](json_body)
    if len(transformed_df):
        write_pq_to_s3(out_bucket, out_key, transformed_df)
