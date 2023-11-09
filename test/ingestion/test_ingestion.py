from ingestion.ingestion import lambda_handler
from unittest.mock import patch
from moto import mock_secretsmanager, mock_s3
import boto3
import os
import json

fake_time = {"last_timestamp": "1970-01-01 00:00:00"}

fake_credentials = {
    'hostname': 'localhost',
    'username': 'test_user',
    'database': 'test_database',
    'password': 'test_pw',
    'port': 5432}

fake_json_rows = {
    "table_name": "sales_order",
    "column_names": ["sales_order_id", "created_at",
                     "last_updated", "design_id",
                     "staff_id", "counterparty_id",
                     "units_sold", "unit_price",
                     "currency_id", "agreed_delivery_date",
                     "agreed_payment_date", "agreed_delivery_location_id"],
    "record_count": 3,
    "data": [
        [5030, "2023-11-01 14:22:10.329", "2023-11-01 14:22:10.329",
         186, 11, 17, 51651, 3.25, 2, "2023-11-06", "2023-11-05", 27],
        [5029, "2023-11-01 14:12:10.124", "2023-11-01 14:12:10.124",
         39, 13, 7, 57395, 3.49, 1, "2023-11-02", "2023-11-04", 26],
        [5028, "2023-11-01 13:33:10.231", "2023-11-01 13:33:10.231",
         229, 20, 13, 34701, 3.97, 1, "2023-11-04", "2023-11-03", 28]
    ]
}

number_of_tables = 11

# @pytest.
# @mock_s3
# @mock_secretsmanager
# @patch('ingestion.ingestion.rows_to_json')
# @patch('ingestion.ingestion.get_conn')
# def test_ingestion_calls_rows_to_json_for_each_table(mock_conn, mock_rows):
#     sm = boto3.client("secretsmanager", region_name="eu-west-2")
#     secret = fake_credentials
#     sm.create_secret(Name="db_credentials_oltp",
#                      SecretString=json.dumps(secret))

#     s3 = boto3.client("s3")
#     location = {'LocationConstraint': 'eu-west-2'}
#     s3.create_bucket(
#         Bucket=os.environ['S3_DATA_ID'],
#         CreateBucketConfiguration=location
#     )

#     s3.put_object(Bucket=os.environ['S3_DATA_ID'],
#                   Key='timestamps.json',
#                   Body=json.dumps(fake_time)
#                   )
#     mock_rows.return_value = json.dumps({"table_name": ""})
#     lambda_handler('', '')
#     mock_rows.assert_called()
#     print(dir(mock_rows))
#     assert mock_rows.call_count == 11
