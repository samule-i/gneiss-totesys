import boto3
import re


def delete_bucket_contents():
    s3_client = boto3.client('s3')
    aws_buckets = s3_client.list_buckets()['Buckets'] # { Buckets: [{ Name:"", Creatio... }...]}
    totesys_buckets = get_totesys_only(aws_buckets)
    for bucket in totesys_buckets:
        keys = [{'Key': key} for key in list_keys(bucket, s3_client)]
        print('\nCONTINUE WITH DELETION?')
        ans = input()
        if ans in ['y', 'yes']:
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={
                    'Objects': keys,
                    'Quiet': False
                })
            print('Finished')
        else:
            print('Cancelled')
            return


def get_totesys_only(bucket_list):
    patt = r'^(ingress|transform|parquet)-.*'
    totesys_buckets = [
        bckt['Name'] for bckt in bucket_list if re.match(patt, bckt['Name'])
        ]
    print('\ntotesys buckets:')
    print(totesys_buckets)
    return totesys_buckets


def list_keys(bucket_name, client):
    response = client.list_objects_v2(Bucket=bucket_name)
    print('\nkey list:')
    key_list = [item['Key'] for item in response['Contents']]
    print(key_list)
    return key_list


if __name__ == '__main__':
    delete_bucket_contents()
