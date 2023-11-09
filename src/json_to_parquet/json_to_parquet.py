import pandas as pd
import os
from json_to_parquet.get_s3_file import json_event, bucket_list
from json_to_parquet.dim_currency import currency_transform
from json_to_parquet.date_dimension import date_dimension
from json_to_parquet.write_pq_to_s3 import write_pq_to_s3


def fake_fn():
    pass


def lambda_handler(event, _):
    out_bucket: str = os.environ['PARQUET_S3_DATA_ID']
    json_body = json_event(event)
    triggering_key = event['Records'][0]['s3']['object']['key']
    parquet_keys = bucket_list(out_bucket)

    function_dict = {
        'address': fake_fn,
        'counterparty': fake_fn,
        'currency': currency_transform,
        'department': fake_fn,
        'design': fake_fn,
        'payment': fake_fn,
        'payment_type': fake_fn,
        'purchase_order': fake_fn,
        'sales_order': fake_fn,
        'staff': fake_fn,
        'transaction': fake_fn
    }

    date_dim_key = 'date/dim_date.parquet'
    if date_dim_key not in parquet_keys:
        df = date_dimension()
        write_pq_to_s3(out_bucket, date_dim_key, df)

    table_name = json_body['table_name']
    transformed_df: pd.DataFrame = function_dict[table_name](json_body)
    write_pq_to_s3(out_bucket, triggering_key, transformed_df)
