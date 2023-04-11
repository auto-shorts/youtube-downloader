import json
import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path

from pytube import YouTube

from auto_shorts.upload_to_s3 import upload_file
from auto_shorts.video_download.download_info import (
    ChannelInfoDownloader,
    VideoData,
    VideoDataWithStats,
)
from loguru import logger
from auto_shorts.video_download.most_watched_moments import (
    MostReplayedNotPresentException, MostWatchedMomentsDownloader,
)

base_data_path = Path(__file__).parents[2] / "data"


class VideoDataWithMoments(VideoDataWithStats):
    most_watched_moments: list[dict]


class DownloaderBase(ABC):
    @abstractmethod
    def download(
            self,
            video_data: VideoDataWithStats,
            save_path: Path,
            bucket: str,
            to_s3: bool,
            save_local: bool,
    ):
        """Enforce download method"""


class YoutubeVideoDownloader(DownloaderBase):
    @staticmethod
    def download_moments(video_data: VideoData) -> VideoDataWithMoments:
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
        (
            YouTube(f"https://www.youtube.com/watch?v={vide_data_full.id}")
            .streams.filter(file_extension="mp4")
            .first()
            .download(str(save_path), filename=filename)
        )

    def download(
            self,
            video_data: VideoData,
            save_path: Path = base_data_path,
            bucket: str = "auto-shorts",
            to_s3: bool = False,
            save_local: bool = True,
    ) -> None:

        if not to_s3 and not save_local:
            raise ValueError(
                "Wrong params config! One of 'to_s3' and 'save_local' must be True!"
            )

        video_data_full = self.download_moments(video_data=video_data)
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


class MultipleVideoDownloader:
    def __init__(self) -> None:
        self.downloader = YoutubeVideoDownloader()
        self.channel_info_downloader = ChannelInfoDownloader()

    def download_videos_from_channel(
            self,
            video_id: str,
            video_number_limit: int = 1000,
            save_path: Path = base_data_path,
            bucket: str = "auto-shorts",
            to_s3: bool = False,
            save_local: bool = True,
    ):
        videos_data = self.channel_info_downloader.get_videos_from_channel(
            video_id=video_id,
            video_number_limit=video_number_limit
        )
        for video_data in videos_data:
            try:
                logger.info(f"Downloading video: {video_data.id}")
                self.downloader.download(
                    video_data=video_data,
                    save_path=save_path,
                    bucket=bucket,
                    to_s3=to_s3,
                    save_local=save_local,
                )
            except MostReplayedNotPresentException as e:
                logger.error(e)
                continue


if __name__ == "__main__":
    m_downloader = MultipleVideoDownloader()
    m_downloader.download_videos_from_channel(video_id="1fUpkq7urDU", video_number_limit=100, to_s3=True,
                                              save_local=True)
