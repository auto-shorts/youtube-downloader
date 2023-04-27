import os
from typing import Protocol

import boto3
from botocore.exceptions import ClientError
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from loguru import logger


class DataUploaderInterface(Protocol):
    def upload_file(self, file_path: str, object_name: str) -> bool:
        ...


class AwsS3DataUploader:
    def __init__(self, bucket: str = "auto-shorts"):
        self.bucket = bucket

    def upload_file(
        self,
        file_path: str,
        object_name: str,
    ) -> bool:
        """
        Function taken from aws docs. Upload a file to an S3 bucket
        Parameters
            file_path: File to upload
            bucket: Bucket to upload to
            object_name: S3 object name.
        Returns
            True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name

        # Upload the file
        s3_client = boto3.client("s3")
        try:
            response = s3_client.upload_file(file_path, self.bucket, object_name)
        except ClientError as e:
            logger.error(e)
            return False
        return True


class GoogleDocsDataUploader:
    """
    Not tested!
    Stopped working on it because student's account doesn't
    allow getting GCP API credentials.
    """

    def __init__(self, credentials, parent_folder_id: str):
        self.service = build("drive", "v3", credentials=credentials)
        self.parent_folder_id = parent_folder_id

    @staticmethod
    def _get_split_path(path: str) -> list[str]:
        return path.split("/")

    def create_nested_folders(self, folder_list: list[str]) -> str:
        for folder in folder_list:
            q = (
                "'"
                + self.parent_folder_id
                + "' in parents and name='"
                + folder
                + "' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            )
            files = self.service.files().list(q=q).execute().get("files")
            if not files:
                file_metadata = {
                    "name": folder,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [self.parent_folder_id],
                }
                parent_folder_id = (
                    self.service.files()
                    .create(body=file_metadata, fields="id")
                    .execute()
                    .get("id")
                )
            else:
                parent_folder_id = files[0].get("id")

        return parent_folder_id

    def _upload_single_file(
        self, file_path: str, folder_id: str, file_save_name: str | None
    ) -> str:
        if not file_save_name:
            file_save_name = os.path.basename(file_path)

        file_metadata = {
            "name": file_save_name,
            "parents": [folder_id],
        }
        media = MediaFileUpload(file_path, resumable=True)
        file = (
            self.service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        return file.get("id")

    def upload_file(
        self,
        file_path: str,
        object_name: str | None = None,
    ) -> bool:
        dir_path, file_name = os.path.split(object_name)
        folder_list = self._get_split_path(dir_path)
        parent_folder_id = self.create_nested_folders(folder_list)
        _ = self._upload_single_file(
            file_path=file_path, folder_id=parent_folder_id, file_save_name=file_name
        )
        return True  # TODO add error handling - need to find that error first XD
