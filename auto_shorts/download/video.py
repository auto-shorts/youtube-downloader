import asyncio
import json
import os
import shutil
from pathlib import Path
from typing import Protocol

import numpy as np
from loguru import logger
from pydantic import BaseModel
from pytube import YouTube
from youtube_transcript_api import TranscriptsDisabled

from auto_shorts.download.channel import (
    ChannelInfoDownloader,
    ChannelInfoDownloaderInterface,
)
from auto_shorts.download.most_watched_moments import (
    MostReplayedNotPresentException,
    MostWatchedMomentsDownloader,
    MostWatchedMomentsDownloaderBase,
)
from auto_shorts.download.transcription import (
    TranscriptionData,
    YoutubeTranscription,
    YoutubeTranscriptionInterface,
)
from auto_shorts.download.video_info import VideoData, VideoDataWithStats
from auto_shorts.preprocess.parse_response import (
    VideoDataList,
    VideoDataParser,
    VideoDataParserInterface,
)
from auto_shorts.upload.video_data_upload import (
    AwsS3DataUploader,
    DataUploaderInterface,
)

base_data_path = Path(__file__).parents[2] / "data"


class VideoDataFull(VideoDataWithStats):
    most_watched_moments: list[dict]
    transcription: TranscriptionData | None


class DownloadConfig(BaseModel):
    save_path: Path = base_data_path
    bucket: str = "auto-shorts"
    to_s3: bool = False
    save_local: bool = True


class DownloadParams(DownloadConfig):
    video_data: VideoData
    resolution: str = "480p"


class DownloaderInterface(Protocol):
    def download(self, download_params: DownloadParams):
        """Enforce download method."""

    async def download_async(self, download_params: DownloadParams):
        """Enforce async download method."""


class YoutubeVideoDownloader:
    """A class used to download YouTube videos and save them to a specified
    location.

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

    def __init__(
        self,
        data_uploader: DataUploaderInterface = AwsS3DataUploader(),
        moment_downloader: MostWatchedMomentsDownloaderBase = MostWatchedMomentsDownloader(),
        transcription_downloader: YoutubeTranscriptionInterface = YoutubeTranscription(),
    ):
        self.data_uploader = data_uploader
        self.moment_downloader = moment_downloader
        self.transcription_downloader = transcription_downloader

    def download_moments(self, video_id: str) -> list[dict] | None:
        try:
            most_watched_moments = (
                self.moment_downloader.get_most_watched_moments(
                    video_id=video_id
                ).to_dict(orient="records")
            )

        except MostReplayedNotPresentException as e:
            logger.error(e)
            most_watched_moments = None

        return most_watched_moments

    def download_transcription(
        self, video_id: str
    ) -> TranscriptionData | None:
        try:
            transcription = self.transcription_downloader.get_transcription(
                video_id=video_id
            )

        except TranscriptsDisabled:
            logger.error("Subtitles are disabled for this video")
            transcription = None

        return transcription

    @staticmethod
    def _download_to_mp4(
        save_path: Path,
        vide_data_full: VideoDataFull,
        filename: str,
        resolution: str,
    ) -> None:
        """Download the video in mp4 format and save it to a specified
        location.

        Parameters
        ----------
        save_path : Path
            The path where the video will be saved.

        video_data_full : VideoDataFull
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
                .streams.filter(file_extension="mp4", res=resolution)
                .first()
                .download(str(save_path), filename=filename)
            )
        except KeyError as e:
            logger.error(f"Data needed to download not found. Key error: {e}")

    async def _download_to_mp4_async(
        self,
        save_path: Path,
        vide_data_full: VideoDataFull,
        filename: str,
        resolution: str,
    ) -> None:
        self._download_to_mp4(
            vide_data_full=vide_data_full,
            filename=filename,
            resolution=resolution,
            save_path=save_path,
        )

    def download(self, download_params: DownloadParams) -> None:
        """Download video data and save it to the specified directory. If
        `to_s3` flag is set to True, the downloaded files will also be uploaded
        to S3 bucket. If `save_local` flag is set to False, the local files
        will be deleted.

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
        logger.info(f"Downloading video: {download_params.video_data.id}")
        if not download_params.to_s3 and not download_params.save_local:
            raise ValueError(
                "Wrong params config! One of 'to_s3' and 'save_local' must be True!"
            )

        most_watched_moments = self.download_moments(
            video_id=download_params.video_data.id
        )
        if not most_watched_moments:
            # we don't need movies without most watched moments for now
            return

        transcription = self.download_transcription(
            download_params.video_data.id
        )

        video_data_full = VideoDataFull(
            **download_params.video_data.dict(),
            most_watched_moments=most_watched_moments,
            transcription=transcription,
        )
        os.makedirs(
            download_params.save_path / video_data_full.id, exist_ok=True
        )
        data_save_path = download_params.save_path / video_data_full.id
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
            resolution=download_params.resolution,
        )

        if download_params.to_s3:
            base_s3_file_path = f"data/videos/{video_data_full.channel_id}/{video_data_full.id}"
            self.data_uploader.upload_file(
                file_path=f"{data_save_path}/video_data.json",
                object_name=f"{base_s3_file_path}/video_data.json",
            )
            self.data_uploader.upload_file(
                file_path=f"{data_save_path}/video.mp4",
                object_name=f"{base_s3_file_path}/video.mp4",
            )

        if not download_params.save_local:
            shutil.rmtree(data_save_path)

    async def download_async(self, download_params: DownloadParams) -> None:
        self.download(
            download_params=download_params,
        )


class VideoFromChannelDownloader:
    """A class for downloading multiple videos from a channel. It is designed
    to work with different types of downloaders and video data parsers.

    Parameters:
        downloader (DownloaderInterface): The object responsible for downloading the video content.
        channel_info_downloader (ChannelInfoDownloaderInterface): The object responsible for collecting video data.
        video_data_parser (VideoDataParserInterface): The object used to select videos based on the date range.

    Methods:
        get_video_data(video_id: str, video_info_limit: int) -> list[VideoData]:
            Collects video data for a given video ID and returns a list of VideoData objects.

        select_videos_by_date(videos_data: VideoDataList, date_from: str, date_to: str) -> VideoDataList:
            Selects videos from the provided VideoDataList that fall within the specified date range.

        download_videos_from_channel(video_id: str, download_config: DownloadConfig = DownloadConfig(),
                                      video_number_limit: int = 1000, video_info_limit: int = 1000,
                                      date_from: str = None, date_to: str = None) -> None:
            Downloads videos from a channel based on the provided parameters.

        download_videos_from_channel_async(video_id: str, download_config: DownloadConfig = DownloadConfig(),
                                            video_number_limit: int = 1000, video_info_limit: int = 1000,
                                            async_videos_block_size: int = 5, date_from: str = None,
                                            date_to: str = None) -> None:
            Downloads videos asynchronously from a channel based on the provided parameters.
    """

    def __init__(
        self,
        downloader: DownloaderInterface,
        channel_info_downloader: ChannelInfoDownloaderInterface,
        video_data_parser: VideoDataParserInterface,
    ) -> None:
        self.downloader = downloader
        self.channel_info_downloader = channel_info_downloader
        self.video_data_parser = video_data_parser

    def get_video_data(
        self, video_id: str, video_info_limit: int
    ) -> list[VideoData]:
        """Collects video data for a given video ID and returns a list of
        VideoData objects.

        Parameters:
            video_id (str): The ID of the video whose data should be collected.
            video_info_limit (int): The maximum number of video metadata objects to collect.
            It determines how many playlist items pages will be requested.

        Returns:
            list[VideoData]: A list of VideoData objects containing the video metadata.
        """

        logger.info("Downloading videos idx from channel")
        return self.channel_info_downloader.get_videos_from_channel(
            video_id=video_id, video_info_limit=video_info_limit
        )

    def select_videos_by_date(
        self, videos_data: VideoDataList, date_from: str, date_to: str
    ) -> VideoDataList:
        """Selects videos from the provided VideoDataList that fall within the
        specified date range.

        Parameters:
            videos_data (VideoDataList): A list of VideoData objects to filter.
            date_from (str): The start date of the date range to filter by.
            date_to (str): The end date of the date range to filter by.

        Returns:
            VideoDataList: A list of VideoData objects filtered by the date range.
        """
        return self.video_data_parser.select_videos_by_date(
            video_data_list=videos_data, date_from=date_from, date_to=date_to
        )

    def download(
        self,
        video_id: str,
        download_config: DownloadConfig = DownloadConfig(),
        video_number_limit: int = 1000,
        video_info_limit: int = 1000,
        date_from: str = None,
        date_to: str = None,
    ) -> None:
        """Download videos from a YouTube channel.

        Parameters:
            video_id: A string representing the channel ID.
            download_config: An instance of the DownloadConfig class. It contains the download
            parameters such as the save_path, bucket, to_s3, save_local
            video_number_limit: An integer representing the maximum number of videos to download.
            video_info_limit: An integer representing the maximum number of videos to retrieve information for.
            date_from: A string representing the start date for selecting videos in the format "YYYY-MM-DD".
            date_to: A string representing the end date for selecting videos in the format "YYYY-MM-DD".
        """
        videos_data = self.get_video_data(
            video_id=video_id, video_info_limit=video_info_limit
        )
        logger.info(f"Collected {len(videos_data)} videos idx")
        videos_data = self.select_videos_by_date(
            videos_data=videos_data, date_from=date_from, date_to=date_to
        )
        videos_data = (
            videos_data[:video_number_limit]
            if video_number_limit < len(videos_data)
            else videos_data
        )
        logger.info(f"Downloading {len(videos_data)} after comparing dates")
        for video_data in videos_data:
            download_params = DownloadParams(
                video_data=video_data, **download_config.dict()
            )
            self.downloader.download(download_params=download_params)

    async def download_async(
        self,
        video_id: str,
        download_config: DownloadConfig = DownloadConfig(),
        video_number_limit: int = 1000,
        video_info_limit: int = 1000,
        async_videos_block_size: int = 5,
        date_from: str = None,
        date_to: str = None,
    ):
        """Download videos asynchronously from a YouTube channel.

        Parameters:
            video_id: A string representing the channel ID.
            download_config: An instance of the DownloadConfig class. It contains the download
            parameters such as the save_path, bucket, to_s3, save_local
            video_number_limit: An integer representing the maximum number of videos to download.
            video_info_limit: An integer representing the maximum number of videos to retrieve information for.
            async_videos_block_size: number of the videos that are downloaded at the same time.
            date_from: A string representing the start date for selecting videos in the format "YYYY-MM-DD".
            date_to: A string representing the end date for selecting videos in the format "YYYY-MM-DD".
        """
        videos_data = self.get_video_data(
            video_id=video_id, video_info_limit=video_info_limit
        )
        videos_data = self.select_videos_by_date(
            videos_data=videos_data, date_from=date_from, date_to=date_to
        )
        videos_data = videos_data[:video_number_limit]
        video_chunks = np.array_split(videos_data, async_videos_block_size)

        for video_chunk in video_chunks:
            _ = await asyncio.gather(
                *[
                    self.downloader.download_async(
                        DownloadParams(
                            video_data=video_data, **download_config.dict()
                        ),
                    )
                    for video_data in video_chunk
                ]
            )


if __name__ == "__main__":
    uploader_test = AwsS3DataUploader()
    downloader_test = YoutubeVideoDownloader(data_uploader=uploader_test)
    channel_info_downloader_test = ChannelInfoDownloader()
    video_parser_test = VideoDataParser()
    m_downloader = VideoFromChannelDownloader(
        downloader=downloader_test,
        channel_info_downloader=channel_info_downloader_test,
        video_data_parser=video_parser_test,
    )

    download_params_test = dict(
        video_id="1fUpkq7urDU",
        video_number_limit=20,
        video_info_limit=50,
        download_config=DownloadConfig(to_s3=True, save_local=True),
        # date_from="2022-10-01",
        # date_to="2022-12-01",
    )

    # @timeit
    def download_sync(params: dict):
        m_downloader.download(**params)

    def download_async(params: dict):
        asyncio.run(
            m_downloader.download_async(**params, async_videos_block_size=10)
        )

    download_sync(download_params_test)
    # download_async(download_params_test)
