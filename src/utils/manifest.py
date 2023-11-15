import boto3
import json
from utils.custom_log import totesys_logger

log = totesys_logger()

def write_manifest(bucket: str, manifest: dict) -> None:
    """Write a manifest file containing everything that has been ingested
       during this run.

    Args:
        bucket (str): Bucket name to store the manifest.
        manifest (dict): Dictionary with a "files" key; a list of ingested
                         files.
    """
    if len(manifest["files"]):
        log.info(f"Writing manifest: {manifest}")

        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.put_object(
            Bucket=bucket, Key="manifest.json", Body=json.dumps(manifest)
        )
    else:
        log.info("Manifest skipped - no files written.")
