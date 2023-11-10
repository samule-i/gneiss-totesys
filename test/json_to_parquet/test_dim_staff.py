from json_to_parquet.dim_staff import dim_staff
from moto import mock_s3
import boto3
import os


@mock_s3
def test_values_return_correctly():
    bucket = 'abcde'
    os.environ['S3_DATA_ID'] = bucket
    with open('test/sample_jsons/sample_department.json', 'r') as file:
        department = file.read()
    with open('test/sample_jsons/sample_staff.json', 'r') as file:
        staff = file.read()

    client = boto3.client('s3')
    client = boto3.client('s3')
    client.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    client.put_object(
        Bucket=bucket,
        Key='department/file.json',
        Body=department
    )
    for i in range(10):
        client.put_object(
            Bucket=bucket,
            Key=f'.id_lookup/department/{i}',
            Body='department/file.json'
        )

    df = dim_staff(staff)
    result = df.loc[df['staff_id'] == 1]
    result = result['department_name'].item()
    assert result == 'Purchasing'

    result = df.loc[df['location'] == 'E']
    print(df.to_json())
    result = result['first_name'].iloc[0]
    print(result)
    assert result == 'f2'


@mock_s3
def test_drops_unused_column():
    bucket = 'abcde'
    os.environ['S3_DATA_ID'] = bucket
    with open('test/sample_jsons/sample_department.json', 'r') as file:
        department = file.read()
    with open('test/sample_jsons/sample_staff.json', 'r') as file:
        staff = file.read()

    client = boto3.client('s3')
    client = boto3.client('s3')
    client.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    client.put_object(
        Bucket=bucket,
        Key='department/file.json',
        Body=department
    )
    for i in range(10):
        client.put_object(
            Bucket=bucket,
            Key=f'.id_lookup/department/{i}',
            Body='department/file.json'
        )
    df = dim_staff(staff)
    result = df.get('manager')
    assert result is None
