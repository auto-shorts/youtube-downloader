import asyncio
import json
import os
import shutil
from pathlib import Path
from typing import Protocol

import numpy as np
from loguru import logger
from pytube import YouTube
from youtube_transcript_api import TranscriptsDisabled

from auto_shorts.download.channel import (
    ChannelInfoDownloader,
    ChannelInfoDownloaderInterface,
)
from auto_shorts.download.models.transcription import TranscriptionData
from auto_shorts.download.models.video import (
    DownloadConfig,
    DownloadParams,
    TranscriptionAndMoments,
)
from auto_shorts.download.models.video_info import VideoData
from auto_shorts.download.most_watched_moments import (
    MostReplayedNotPresentException,
    MostWatchedMomentsDownloader,
    MostWatchedMomentsDownloaderBase,
)
from auto_shorts.download.transcription import (
    YoutubeTranscription,
    YoutubeTranscriptionInterface,
)
from auto_shorts.preprocess.parse_response import (
    VideoDataList,
    VideoDataParser,
    VideoDataParserInterface,
)
from auto_shorts.upload.db import upload_video_info_to_db
from auto_shorts.upload.db.upload import (
    is_channel_present,
    upload_channel_info,
    is_video_present
)
from auto_shorts.upload.s3.video_data_upload import (
    AwsS3DataUploader,
    DataUploaderInterface,
)


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

    _download_to_mp4(save_path: Path, vide_data_full: VideoDataWithMoments, filename: str) -> bool:
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
        video_id: str,
        filename: str,
        resolution: str,
    ) -> bool:
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
                YouTube(f"https://www.youtube.com/watch?v={video_id}", use_oauth=True, allow_oauth_cache=True)
                .streams.filter(file_extension="mp4", res=resolution)
                .first()
                .download(str(save_path), filename=filename)
            )
            return True

        except KeyError as e:
            logger.error(f"Data needed to download not found. Key error: {e}")
            return False

    async def _download_to_mp4_async(
        self,
        save_path: Path,
        video_id: str,
        filename: str,
        resolution: str,
    ) -> bool:
        return self._download_to_mp4(
            video_id=video_id,
            filename=filename,
            resolution=resolution,
            save_path=save_path,
        )

    def download(self, download_params: DownloadParams) -> bool:
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

        if is_video_present(video_id=download_params.video_data.id):
            logger.info(f"Video already on s3 - skipping: {download_params.video_data.id}")
            return True

        most_watched_moments = self.download_moments(
            video_id=download_params.video_data.id
        )
        if not most_watched_moments:
            return False

        transcription = self.download_transcription(
            download_params.video_data.id
        )

        video_data_full = TranscriptionAndMoments(
            most_watched_moments=most_watched_moments,
            transcription=transcription,
        )
        data_save_path = (
            download_params.save_path
            / download_params.video_data.category_id
            / download_params.video_data.channel_id
            / download_params.video_data.id
        )
        os.makedirs(
            data_save_path,
            exist_ok=True,
        )
        with open(data_save_path / "video_data.json", "w") as file:
            json.dump(
                video_data_full.dict(),
                file,
                indent=4,
            )

        downloaded = self._download_to_mp4(
            save_path=data_save_path,
            video_id=download_params.video_data.id,
            filename="video.mp4",
            resolution=download_params.resolution,
        )
        if not downloaded:
            return False

        if download_params.to_s3:
            base_s3_file_path = (
                f"data/videos/{download_params.video_data.category_id}"
                f"/{download_params.video_data.channel_id}/"
                f"{download_params.video_data.id}"
            )
            self.data_uploader.upload_file(
                file_path=f"{data_save_path}/video_data.json",
                object_name=f"{base_s3_file_path}/video_data.json",
            )
            self.data_uploader.upload_file(
                file_path=f"{data_save_path}/video.mp4",
                object_name=f"{base_s3_file_path}/video.mp4",
            )
            _ = upload_video_info_to_db(
                video_data=download_params.video_data,
                s3_path=base_s3_file_path,
            )

        if not download_params.save_local:
            shutil.rmtree(data_save_path)

        logger.info(f"Video downloaded - {download_params.video_data.id}")
        return True

    async def download_async(self, download_params: DownloadParams) -> bool:
        return self.download(
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
        logger.info("Downloading videos idx from channel")
        return self.channel_info_downloader.get_videos_from_channel(
            video_id=video_id, video_info_limit=video_info_limit
        )

    def select_videos_by_date(
        self, videos_data: VideoDataList, date_from: str, date_to: str
    ) -> VideoDataList:
        return self.video_data_parser.select_videos_by_date(
            video_data_list=videos_data, date_from=date_from, date_to=date_to
        )

    def upload_channel_info_if_not_present(
        self, videos_data: list[VideoData]
    ) -> None:
        channel_id = videos_data[
            0
        ].channel_id  # every channel_id should be the same

        if not is_channel_present(channel_id=channel_id):
            channel_info = self.channel_info_downloader.get_info(
                channel_id=channel_id
            )
            upload_channel_info(channel_info)

    def prepare_video_data(
        self,
        video_id: str,
        video_number_limit: int,
        video_info_limit: int,
        date_from: str,
        date_to: str,
    ) -> list[VideoData] | None:
        videos_data = self.get_video_data(
            video_id=video_id, video_info_limit=video_info_limit
        )
        if len(videos_data) == 0:
            logger.info("No videos found!")
            return

        logger.info(f"Collected {len(videos_data)} videos idx")
        videos_data = self.select_videos_by_date(
            videos_data=videos_data, date_from=date_from, date_to=date_to
        )

        if len(videos_data) == 0:
            logger.info("No videos found after comparing dates!")
            return

        videos_data = (
            videos_data[:video_number_limit]
            if video_number_limit < len(videos_data)
            else videos_data
        )
        logger.info(f"Downloading {len(videos_data)} videos")
        return videos_data

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
        videos_data = self.prepare_video_data(
            video_id=video_id,
            video_number_limit=video_number_limit,
            video_info_limit=video_info_limit,
            date_from=date_from,
            date_to=date_to,
        )
        self.upload_channel_info_if_not_present(videos_data)
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
        videos_data = self.prepare_video_data(
            video_id=video_id,
            video_number_limit=video_number_limit,
            video_info_limit=video_info_limit,
            date_from=date_from,
            date_to=date_to,
        )
        self.upload_channel_info_if_not_present(videos_data)
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
        video_id="1WEAJ-DFkHE",
        video_number_limit=30,
        video_info_limit=100,
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
