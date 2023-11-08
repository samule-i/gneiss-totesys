from json_to_parquet.get_s3_file import json_event()

def fake_fn():
    pass

def lambda_handler(event, context):
    json_body = json_event(event)

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
        'sample_tim': fake_fn,
        'staff': fake_fn,
        'transaction': fake_fn
    }

    table_name = json_body['table_name']
    transformed_data = function_dict[table_name]()