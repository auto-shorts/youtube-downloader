import asyncio
import json
import os
import shutil
from pathlib import Path
from typing import Protocol

import numpy as np
from loguru import logger
from pytube import YouTube

from auto_shorts.upload_to_s3 import upload_file
from auto_shorts.video_download.download_info import (
    ChannelInfoDownloader,
    ChannelInfoDownloaderInterface,
    VideoData,
    VideoDataWithStats,
    VideoDataParserInterface,
    VideoDataParser
)
from auto_shorts.video_download.most_watched_moments import (
    MostReplayedNotPresentException,
    MostWatchedMomentsDownloader,
)

base_data_path = Path(__file__).parents[2] / "data"


class VideoDataWithMoments(VideoDataWithStats):
    most_watched_moments: list[dict]


class DownloaderInterface(Protocol):
    def download(
        self,
        video_data: VideoData,
        save_path: Path,
        bucket: str,
        to_s3: bool,
        save_local: bool,
    ):
        """Enforce download method"""

    async def download_async(
        self,
        video_data: VideoData,
        save_path: Path,
        bucket: str,
        to_s3: bool,
        save_local: bool,
    ):
        """Enforce async download method"""


class YoutubeVideoDownloader:
    """
    A class used to download YouTube videos and save them to a specified location.

    Methods
    -------
    download(video_data: VideoData, save_path: Path = base_data_path,
             bucket: str = "auto-shorts", to_s3: bool = False, save_local: bool = True) -> None:
        Download the specified video and save it to a specified location.

    download_moments(video_data: VideoData) -> VideoDataWithMoments:
        Download the most watched moments of the video.

    _download_to_mp4(save_path: Path, vide_data_full: VideoDataWithMoments, filename: str) -> None:
        Download the video in mp4 format and save it to a specified location.

    """

    @staticmethod
    def download_moments(video_data: VideoData) -> VideoDataWithMoments:
        """
        Download the most watched moments of the video.

        Parameters
        ----------
        video_data : VideoData
            A `VideoData` object containing the video's metadata.

        Returns
        -------
        VideoDataWithMoments
            A `VideoDataWithMoments` object containing the video's metadata and the most watched moments.
        """
        moment_downloader = MostWatchedMomentsDownloader(video_id=video_data.id)
        most_watched_moments = moment_downloader.get_most_watched_moments().to_dict(
            orient="records"
        )
        return VideoDataWithMoments(
            **video_data.dict(), most_watched_moments=most_watched_moments
        )

    @staticmethod
    def _download_to_mp4(
        save_path: Path,
        vide_data_full: VideoDataWithMoments,
        filename: str,
    ) -> None:
        """
        Download the video in mp4 format and save it to a specified location.

        Parameters
        ----------
        save_path : Path
            The path where the video will be saved.

        video_data_full : VideoDataWithMoments
            A `VideoDataWithMoments` object containing the video's metadata and the most watched moments.

        filename : str
            The name of the file in which the video will be saved.

        Returns
        -------
        None
        """
        try:
            (
                YouTube(f"https://www.youtube.com/watch?v={vide_data_full.id}")
                .streams.filter(file_extension="mp4")
                .first()
                .download(str(save_path), filename=filename)
            )
        except KeyError as e:
            logger.error(f"Data needed to download not found. Key error: {e}")

    def download(
        self,
        video_data: VideoData,
        save_path: Path = base_data_path,
        bucket: str = "auto-shorts",
        to_s3: bool = False,
        save_local: bool = True,
    ) -> None:
        """
        Download video data and save it to the specified directory.
        If `to_s3` flag is set to True, the downloaded files will also be uploaded to S3 bucket.
        If `save_local` flag is set to False, the local files will be deleted.

        Parameters:
        -----------
        video_data : VideoData
            VideoData object containing the necessary information to download the video
        save_path : str
            The directory where the downloaded files should be saved (default: base_data_path)
        bucket : str
            The name of the S3 bucket to which the downloaded files should be uploaded (default: "auto-shorts")
        to_s3 : bool
            Flag indicating whether the downloaded files should be uploaded to S3 (default: False)
        save_local : bool
            Flag indicating whether the downloaded files should be saved locally (default: True)

        Returns:
        --------
        None
        """
        if not to_s3 and not save_local:
            raise ValueError(
                "Wrong params config! One of 'to_s3' and 'save_local' must be True!"
            )

        try:
            video_data_full = self.download_moments(video_data=video_data)

        except MostReplayedNotPresentException as e:
            logger.error(e)
            return

        os.makedirs(save_path / video_data_full.id, exist_ok=True)
        data_save_path = save_path / video_data_full.id
        with open(data_save_path / "video_data.json", "w") as file:
            json.dump(
                video_data_full.dict(),
                file,
                indent=4,
            )

        self._download_to_mp4(
            save_path=data_save_path,
            vide_data_full=video_data_full,
            filename="video.mp4",
        )

        if to_s3:
            base_s3_file_path = (
                f"data/videos/{video_data_full.channel_id}/{video_data_full.id}"
            )
            upload_file(
                file_path=f"{data_save_path}/video_data.json",
                bucket=bucket,
                object_name=f"{base_s3_file_path}/video_data.json",
            )
            upload_file(
                file_path=f"{data_save_path}/video.mp4",
                bucket=bucket,
                object_name=f"{base_s3_file_path}/video.mp4",
            )

        if not save_local:
            shutil.rmtree(data_save_path)

    async def download_async(
        self,
        video_data: VideoData,
        save_path: Path = base_data_path,
        bucket: str = "auto-shorts",
        to_s3: bool = False,
        save_local: bool = True,
    ) -> None:
        self.download(
            video_data=video_data,
            save_path=save_path,
            bucket=bucket,
            to_s3=to_s3,
            save_local=save_local,
        )


class MultipleVideoDownloader:
    def __init__(
        self,
        downloader: DownloaderInterface,
        channel_info_downloader: ChannelInfoDownloaderInterface,
    ) -> None:
        self.downloader = downloader
        self.channel_info_downloader = channel_info_downloader

    def get_video_data(
        self, video_id: str, video_number_limit: int
    ) -> list[VideoDataWithStats]:
        return self.channel_info_downloader.get_videos_from_channel(
            video_id=video_id, video_number_limit=video_number_limit
        )

    def download_videos_from_channel(
        self,
        video_id: str,
        video_number_limit: int = 1000,
        save_path: Path = base_data_path,
        bucket: str = "auto-shorts",
        to_s3: bool = False,
        save_local: bool = True,
    ) -> None:
        videos_data = self.get_video_data(
            video_id=video_id, video_number_limit=video_number_limit
        )
        for video_data in videos_data:
            logger.info(f"Downloading video: {video_data.id}")
            self.downloader.download(
                video_data=video_data,
                save_path=save_path,
                bucket=bucket,
                to_s3=to_s3,
                save_local=save_local,
            )

    async def download_videos_from_channel_async(
        self,
        video_id: str,
        video_number_limit: int = 1000,
        save_path: Path = base_data_path,
        bucket: str = "auto-shorts",
        to_s3: bool = False,
        save_local: bool = True,
        async_videos_block_size: int = 5,
    ):
        videos_data = self.get_video_data(
            video_id=video_id, video_number_limit=video_number_limit
        )
        video_chunks = np.array_split(videos_data, async_videos_block_size)

        for video_chunk in video_chunks:
            _ = await asyncio.gather(
                *[
                    self.downloader.download_async(
                        video_data=video_data,
                        save_path=save_path,
                        bucket=bucket,
                        to_s3=to_s3,
                        save_local=save_local,
                    )
                    for video_data in video_chunk
                ]
            )


if __name__ == "__main__":
    downloader_test = YoutubeVideoDownloader()
    channel_info_downloader_test = ChannelInfoDownloader()
    m_downloader = MultipleVideoDownloader(
        downloader=downloader_test, channel_info_downloader=channel_info_downloader_test
    )
    asyncio.run(
        m_downloader.download_videos_from_channel_async(
            video_id="1fUpkq7urDU", video_number_limit=10, to_s3=False, save_local=True
        )
    )
