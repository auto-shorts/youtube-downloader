import json
import os
from pathlib import Path
from pytube import YouTube
from auto_shorts.video_download.download_info import VideoData, VideoInfoDownloader
from auto_shorts.video_download.most_watched_moments import (
    MostWatchedMomentsDownloader,
)
from auto_shorts.upload_to_s3 import upload_file

base_data_path = Path(__file__).parents[2] / "data"


class VideoDataWithMoments(VideoData):
    most_watched_moments: list[dict]


class YoutubeVideoDownloader:
    def download_moments(self, video_data: VideoData) -> VideoDataWithMoments:
        moment_downloader = MostWatchedMomentsDownloader(
            video_id=video_data.id
        )
        most_watched_moments = (
            moment_downloader.get_most_watched_moments().to_dict(
                orient="record"
            )
        )
        return VideoDataWithMoments(
            **video_data.dict(), most_watched_moments=most_watched_moments
        )

    def _download_to_mp4(
        self,
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
    ) -> None:
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
            base_s3_file_path = f"data/videos/{video_data_full.id}"
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


class MultipleVideoDownloader:
    def download_all_videos_from_channel(): ...
    
    
if __name__ == "__main__":
    connector = VideoInfoDownloader()
    video_data = connector.download_video_data(video_id="VdMEP9ScpUg")
    downloader = YoutubeVideoDownloader()
    downloader.download(video_data=video_data[0], to_s3=True)
