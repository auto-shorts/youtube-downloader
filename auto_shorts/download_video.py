import json
import os
from pathlib import Path
from pytube import YouTube
from auto_shorts.list_popular_videos import VideoData, VideoInfoDownloader
from auto_shorts.most_watched_moments import MostWatchedMomentsDownloader


base_data_path = Path(__file__).parents[1] / "data"


class VideoDataWithMoments(VideoData):
    most_watched_moments: list[dict]


class YoutubeVideoDownloader:
    def __init__(
        self,
        video_data: VideoData,
    ) -> None:
        self.vide_data_full: VideoDataWithMoments = self.prepare_video_data(
            video_data=video_data
        )

    def prepare_video_data(
        self, video_data: VideoData
    ) -> VideoDataWithMoments:
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

    def download_to_mp4(self, save_path: Path, filename: str) -> None:
        (
            YouTube(f"https://www.youtube.com/watch?v={self.vide_data_full.id}")
            .streams.filter(file_extension="mp4")
            .first()
            .download(str(save_path), filename=filename)
        )

    def save(self, save_path: Path = base_data_path) -> None:
        os.makedirs(save_path / self.vide_data_full.id, exist_ok=True)
        data_save_path = save_path / self.vide_data_full.id
        with open(data_save_path / "video_data.json", "w") as file:
            json.dump(
                self.vide_data_full.dict(),
                file,
                indent=4,
            )
        self.download_to_mp4(save_path=data_save_path, filename="video.mp4")


if __name__ == "__main__":
    connector = VideoInfoDownloader()
    video_data = connector.download_video_data(video_id="VdMEP9ScpUg")
    downloader = YoutubeVideoDownloader(video_data=video_data)
    downloader.save()
