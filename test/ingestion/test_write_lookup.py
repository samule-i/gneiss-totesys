import json
from ingestion.write_JSON import write_lookup
from moto import mock_s3
import boto3


@mock_s3
def test_the_data_has_been_uploaded_to_S3_using_ls():
    client = boto3.client('s3')
    bucket = 'test-bucket'
    table = 'testing-table'
    client.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
    json_body = {"table_name": table,
                 "column_names": ["a", "b"],
                 "record_count": 1,
                 "data": [
                     [10, 20],
                     [30, 40]
                 ]}
    write_lookup(json_body, bucket, 'test/key')
    download = client.get_object(
        Bucket=bucket,
        Key=f'.id_lookup/{table}.json.notrigger'
    )
    contents = download['Body'].read().decode()
    expect = {"table_name": "testing-table",
              "indexes": {"10": "test/key", "30": "test/key"}}
    assert contents == json.dumps(expect)
