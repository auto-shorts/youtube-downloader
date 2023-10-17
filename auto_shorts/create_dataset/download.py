import asyncio
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import boto3
import numpy as np
import yaml
from botocore.exceptions import ClientError
from loguru import logger

from auto_shorts.create_dataset.models.download import (
    DownloadMetadata,
    FunctionInput,
)
from auto_shorts.create_dataset.select_s3_path import download_s3_paths

POSSIBLE_FILE_NAMES = ["video_data.json", "video.mp4"]
BASE_DATASET_PATH = Path(__file__).parents[2] / "data" / "downloaded"


def download_file(
    s3_object: str,
    downloaded_file_name: str | None = None,
    bucket: str = "auto-shorts",
) -> bool:
    """
    Download a file from an S3 bucket

    Parameters
    ----------
    s3_object : str
        The name of the S3 object to download
    downloaded_file_name : str or None, default None
        The name of the downloaded file. If None, the S3 object name is used.
    bucket : str, default 'wec-2023'
        The name of the S3 bucket to download from

    Returns
    -------
    bool
        True if the file was downloaded successfully, False otherwise
    """
    if not downloaded_file_name:
        downloaded_file_name = s3_object
    logger.debug(f"Downloading {s3_object} from {bucket}")
    s3_client = boto3.client("s3")

    try:
        _ = s3_client.download_file(bucket, s3_object, downloaded_file_name)
        logger.info(f"{s3_object} downloaded successfully")
    except ClientError as e:
        logger.error(e)
        return False
    return True


def download_files_for_video(
    s3_folder_object: str,
    download_folder: Path,
    bucket: str = "auto-shorts",
) -> None:
    video_id = s3_folder_object.split("/")[-1]
    os.makedirs(download_folder / video_id, exist_ok=True)
    for file_name in POSSIBLE_FILE_NAMES:
        download_file(
            s3_object=f"{s3_folder_object}/{file_name}",
            downloaded_file_name=str(download_folder / video_id / file_name),
            bucket=bucket,
        )


async def async_run_functions(function_inputs: list[FunctionInput]) -> None:
    _ = await asyncio.gather(
        *[
            asyncio.to_thread(
                func_input.function, *func_input.args, **func_input.kwargs
            )
            for func_input in function_inputs
        ]
    )


def save_dict_to_yaml(dictionary: dict, save_path: Path) -> None:
    with open(save_path, "w") as f:
        yaml.dump(dictionary, f)


def split_array(array: list[Any], block_size: int) -> list[list[Any]]:
    return np.array_split(array, block_size)


async def download(
    query: str,
    save_path: Path,
    dataset_name: str,
    block_size: int = 5,
) -> None:
    s3_folder_paths = download_s3_paths(query=query)
    metadata = DownloadMetadata(
        s3_paths=s3_folder_paths,
        download_date=str(datetime.now()),
        download_query=query,
    )
    save_path = save_path / dataset_name
    os.makedirs(save_path, exist_ok=True)
    save_dict_to_yaml(
        dictionary=metadata.dict(), save_path=save_path / "metadata.yaml"
    )
    function_inputs = [
        FunctionInput(
            function=download_files_for_video,
            args=[],
            kwargs={
                "s3_folder_object": s3_folder_path,
                "download_folder": save_path,
            },
        )
        for s3_folder_path in s3_folder_paths
    ]
    func_input_chunks = split_array(
        array=function_inputs, block_size=block_size
    )

    for func_input_chunk in func_input_chunks:
        await async_run_functions(function_inputs=func_input_chunk)


if __name__ == "__main__":
    query_test = f"""
        SELECT
            s3_path
        FROM
            autoshorts.videos
        WHERE
            description LIKE '%EKIPY%';
    """
    save_path_test = Path(__file__).parents[2] / "data" / "downloaded"
    asyncio.run(
        download(
            query=query_test,
            save_path=save_path_test,
            dataset_name="test_dataset",
        )
    )
