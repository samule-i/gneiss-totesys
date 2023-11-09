from json_to_parquet.get_s3_file import json_event, bucket_list


def fake_fn():
    pass


def write_to_s3(_, __):
    pass


def date_dimension():
    pass


def lambda_handler(event, context):
    json_body = json_event(event)
    keys = bucket_list()

    function_dict = {
        'address': fake_fn,
        'counterparty': fake_fn,
        'currency': fake_fn,
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
    if date_dim_key not in keys:
        df = date_dimension()
        write_to_s3(date_dim_key, df)

    table_name = json_body['table_name']
    transformed_data = function_dict[table_name]()
    write_to_s3('key', transformed_data)
