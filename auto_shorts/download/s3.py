import os
from pathlib import Path

import boto3
import pandas as pd

from auto_shorts.upload.db.utils import postgres_engine

root_path = Path(__file__).parents[2]


def download_s3_folder(s3_folder: str, local_dir: Path, bucket: str):
    """
    Download the contents of a folder directory
    Args:
        bucket: the name of the s3 bucket
        s3_folder: the folder path in the s3 bucket
        local_dir: a relative or absolute directory path in the local file system
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)

    for obj in bucket.objects.filter(Prefix=s3_folder):
        target = (
            obj.key
            if local_dir is None
            else os.path.join(local_dir, os.path.relpath(obj.key, s3_folder))
        )
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == "/":
            continue
        bucket.download_file(obj.key, target)


def download_data_with_query(
    query: str, save_path: Path = root_path, bucket: str = "auto-shorts"
) -> None:
    with postgres_engine.connect() as connection:
        df = pd.read_sql(query, con=connection)

    if "s3_path" not in df.columns:
        raise ValueError(
            "Wrong query! Result need to contain 's3_path' column"
        )

    if len(df) == 0:
        raise ValueError("Wrong query! Not data found.")

    for s3_folder in df["s3_path"]:
        download_s3_folder(
            s3_folder=s3_folder, local_dir=save_path / s3_folder, bucket=bucket
        )


def download_files(
    s3_paths: list[str],
    bucket_name: str = "auto-shorts",
    save_path: Path = root_path / "data",
) -> None:
    s3 = boto3.client("s3")
    for s3_path in s3_paths:
        local_path = save_path / str(s3_path.split("/")[-1] + ".mp4")
        s3.download_file(bucket_name, s3_path + "/video.mp4", local_path)


def get_extracted_video_ids(bucket_name: str = "auto-shorts") -> list[str]:
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    # paths are in format data/videos/{category_id}/{channel_id}/{video_id}/{feature_name}.npy
    return [
        obj.key.split("/")[-2]
        for obj in bucket.objects.filter(Prefix="data/videos/")
        if obj.key.endswith(".npy")
    ]
