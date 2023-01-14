from pathlib import Path
from pytube import YouTube


class YoutubeVideoDownloader:
    def __init__(self, video_id: str) -> None:
        self.video_id = video_id

    def download_to_mp4(self, save_path: Path) -> None:
        (
            YouTube(f"https://www.youtube.com/watch?v={self.video_id}")
            .streams.filter(file_extension="mp4")
            .first()
            .download(str(save_path))
        )
