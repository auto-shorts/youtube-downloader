import asyncio
import json
import os
import shutil
from pathlib import Path
from typing import Protocol

import numpy as np
import requests
from loguru import logger
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
from auto_shorts.download.models.video_info import (
    VideoData,
    VideoDataWithStats,
)
from auto_shorts.download.most_watched_moments import (
    MostReplayedNotPresentException,
    MostWatchedMomentsDownloader,
    MostWatchedMomentsDownloaderBase,
)
from auto_shorts.download.mp4 import (
    Mp4DownloaderInterface,
    MutualVideoAudioDownloader,
)
from auto_shorts.download.transcription import (
    YoutubeTranscription,
    YoutubeTranscriptionInterface,
)
from auto_shorts.download.video_info import (
    VideoInfoDownloader,
    VideoInfoDownloaderInterface,
)
from auto_shorts.preprocess.parse_response import (
    VideoDataList,
    VideoDataParserInterface,
)
from auto_shorts.upload.bucket import AwsS3DataUploader, DataUploaderInterface
from auto_shorts.upload.db import (
    is_channel_present,
    is_video_present,
    upload_channel_info,
    upload_video_info_to_db,
)
from auto_shorts.utils import safe_get


class WholeVideoDataDownloaderInterface(Protocol):
    def download(self, download_params: DownloadParams) -> bool:
        """Enforce download method."""


class WholeVideoDataDownloader:
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
        mp4_downloader: Mp4DownloaderInterface = MutualVideoAudioDownloader(),
    ):
        self.data_uploader = data_uploader
        self.moment_downloader = moment_downloader
        self.transcription_downloader = transcription_downloader
        self.mp4_downloader = mp4_downloader

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

    @staticmethod
    def is_video_shorts(video_id: str) -> bool:
        url = f"https://yt.lemnoslife.com/videos?part=short&id={video_id}"
        raw_results = requests.get(url).json()
        if safe_get(raw_results["items"][0], "short", "available"):
            return True
        return False

    def download_transcription(
        self, video_id: str
    ) -> TranscriptionData | None:
        try:
            transcription = self.transcription_downloader.get_transcription(
                video_id=video_id
            )

        except TranscriptsDisabled:
            logger.error(
                "Subtitles are disabled for this WholeVideoDataDownloadervideo"
            )
            transcription = None

        return transcription

    def download_mp4(
        self,
        save_path: Path,
        video_id: str,
        resolution: str,
    ) -> bool:
        return self.mp4_downloader.download_to_mp4(
            save_path=save_path,
            video_id=video_id,
            resolution=resolution,
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
        if self.is_video_shorts(video_id=download_params.video_data.id):
            logger.error(
                f"Video is YouTube shorts: {download_params.video_data.id}"
            )
            return False

        if is_video_present(video_id=download_params.video_data.id):
            logger.info(
                f"Video already on s3 - skipping: {download_params.video_data.id}"
            )
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

        downloaded = self.download_mp4(
            save_path=data_save_path,
            video_id=download_params.video_data.id,
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


class MultipleVideoDownloader:
    def __init__(
        self,
        downloader: WholeVideoDataDownloaderInterface = WholeVideoDataDownloader(),
        video_info_downloader: VideoInfoDownloaderInterface = VideoInfoDownloader(),
        channel_info_downloader: ChannelInfoDownloaderInterface = ChannelInfoDownloader(),
    ) -> None:
        self.downloader = downloader
        self.video_info_downloader = video_info_downloader
        self.channel_info_downloader = channel_info_downloader
        
    def get_video_data(
        self, video_ids: list[str], idx_per_request: int = 10
    ) -> list[VideoDataWithStats]:
        logger.info("Downloading videos idx from channel")
        idx_chunks = np.array_split(video_ids, idx_per_request)
        video_data = []
        for idx_chunk in idx_chunks:
            videos_as_string = ",".join(idx_chunk)
            video_data.extend(
                self.video_info_downloader.download_video_data(
                    video_id=videos_as_string
                )
            )

        return video_data

    def upload_channel_info_if_not_present(
        self, videos_data: list[VideoData]
    ) -> None:
        channel_ids = set([video_data.channel_id for video_data in videos_data])
        for channel_id in channel_ids:
            if not is_channel_present(channel_id=channel_id):
                channel_info = self.channel_info_downloader.get_info(
                    channel_id=channel_id
                )
                upload_channel_info(channel_info)
            
    def download(
        self,
        video_ids: list[str],
        download_config: DownloadConfig = DownloadConfig(),
    ) -> None:
        videos_data = self.get_video_data(video_ids)
        self.upload_channel_info_if_not_present(videos_data)
        for video_data in videos_data:
            download_params = DownloadParams(
                video_data=video_data, **download_config.dict()
            )
            self.downloader.download(download_params=download_params)

    async def download_async(
        self,
        video_ids: list[str],
        download_config: DownloadConfig = DownloadConfig(),
        async_videos_block_size: int = 5,
    ) -> None:
        videos_data = self.get_video_data(video_ids)
        self.upload_channel_info_if_not_present(videos_data)
        
        video_chunks = np.array_split(videos_data, async_videos_block_size)

        for video_chunk in video_chunks:
            _ = await asyncio.gather(
                *[
                    asyncio.to_thread(
                        self.downloader.download,
                        DownloadParams(
                            video_data=video_data, **download_config.dict()
                        ),
                    )
                    for video_data in video_chunk
                ]
            )


class VideoFromChannelDownloader:
    """A class for downloading multiple videos from a channel. It is designed
    to work with different types of downloaders and video data parsers.

    Parameters:
        downloader (WholeVideoDataDownloaderInterface): The object responsible for downloading the video content.
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
        downloader: WholeVideoDataDownloaderInterface,
        channel_info_downloader: ChannelInfoDownloaderInterface,
        video_data_parser: VideoDataParserInterface,
    ) -> None:
        self.downloader = downloader
        self.channel_info_downloader = channel_info_downloader
        self.video_data_parser = video_data_parser

    def get_video_data(
        self, video_id: str, video_info_limit: int
    ) -> list[VideoDataWithStats]:
        logger.info("Downloading videos idx from channel")
        return self.channel_info_downloader.get_full_video_data_from_channel(
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
    ) -> list[VideoDataWithStats] | None:
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
                    asyncio.to_thread(
                        self.downloader.download,
                        DownloadParams(
                            video_data=video_data, **download_config.dict()
                        ),
                    )
                    for video_data in video_chunk
                ]
            )


if __name__ == "__main__":
    def get_video_indices(filepath):
        # Read the file content
        with open(filepath, 'r') as file:
            content = file.readlines()

        # Extract video ids
        video_ids = [url.split("=")[-1].strip() for url in content]

        return video_ids
    
    video_idx = get_video_indices("/Users/jakubwujec/projects/auto-shorts/data/video_ids_to_download.txt")
    multiple_video_downloader = MultipleVideoDownloader()
    download_config = DownloadConfig(to_s3=True, save_local=False)
    asyncio.run(multiple_video_downloader.download_async(video_idx, download_config=download_config))
    
    
    
    # info_downloader = VideoInfoDownloader()
    # video_data_with_stats = info_downloader.download_video_data("8Q2RGD5f0Sc")[
    #     0
    # ]
    # params = DownloadParams(video_data=video_data_with_stats)
    # downloader_test = WholeVideoDataDownloader()
    # downloader_test.download(params)
    
    
    # channel_info_downloader_test = ChannelInfoDownloader()
    # video_parser_test = VideoDataParser()
    # m_downloader = VideoFromChannelDownloader(
    #     downloader=downloader_test,
    #     channel_info_downloader=channel_info_downloader_test,
    #     video_data_parser=video_parser_test,
    # )
    #
    # download_params_test = dict(
    #     video_id="4pZbM3zOMwQ",
    #     video_number_limit=5,
    #     video_info_limit=100,
    #     download_config=DownloadConfig(to_s3=False, save_local=True),
    #     # date_from="2022-10-01",
    #     # date_to="2022-12-01",
    # )
    #
    # # @timeit
    # def download_sync(params: dict):
    #     m_downloader.download(**params)
    #
    # def download_async(params: dict):
    #     asyncio.run(
    #         m_downloader.download_async(**params, async_videos_block_size=10)
    #     )
    #
    # # download_sync(download_params_test)
    # download_async(download_params_test)
