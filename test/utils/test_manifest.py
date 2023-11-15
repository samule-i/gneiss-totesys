from utils.manifest import write_manifest
from unittest.mock import patch, Mock
import json

@patch("ingestion.write_JSON.boto3.client")
def test_write_manifest_save_json_file_to_ingestion_bucket(
    patched_boto_client,
):
    test_manifest = {"files": ["file1", "file2", "file3"]}
    fake_s3 = Mock()
    patched_boto_client.return_value = fake_s3

    write_manifest("bucket", test_manifest)

    fake_s3.put_object.assert_called_once_with(
        Bucket="bucket", Key="manifest.json", Body=json.dumps(test_manifest)
    )


@patch("ingestion.write_JSON.boto3.client")
def test_write_manifest_does_nothing_if_no_files_in_input(
    patched_boto_client,
):
    test_manifest = {"files": []}
    fake_s3 = Mock()
    patched_boto_client.return_value = fake_s3

    write_manifest("bucket", test_manifest)

    fake_s3.put_object.assert_not_called()
