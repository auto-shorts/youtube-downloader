from loguru import logger
import boto3
from botocore.exceptions import ClientError
import os
from pathlib import Path


def upload_file(
    file_path: str, bucket: str = "auto-shorts", object_name: str | None = None
):
    """
    ### Function taken from aws docs ###

    Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_path)

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        response = s3_client.upload_file(file_path, bucket, object_name)
    except ClientError as e:
        logger.error(e)
        return False
    return True
