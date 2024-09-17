import os
from pathlib import Path
from typing import Protocol

from loguru import logger
from moviepy.editor import AudioFileClip, VideoFileClip
from pytubefix import YouTube


class Mp4DownloaderInterface(Protocol):
    def download_to_mp4(
        self, save_path: Path, video_id: str, **kwargs
    ) -> bool:
        """Enforce download to mp4 method."""


class SeparatelyVideoAudioDownloader:
    """TODO REFACTOR OR REMOVE - slow and not working properly."""

    @staticmethod
    def _download_video(save_path: Path, video_id: str, **kwargs) -> bool:
        try:
            (
                YouTube(
                    f"https://www.youtube.com/watch?v={video_id}",
                    use_oauth=True,
                    allow_oauth_cache=True,
                )
                .streams.filter(file_extension="mp4", res=kwargs["resolution"])
                .first()
                .download(str(save_path), filename="raw_video.mp4")
            )
            return True

        except KeyError as e:
            logger.error(f"Video needed to download not found. Key error: {e}")
            return False

    @staticmethod
    def _download_audio(save_path: Path, video_id: str) -> bool:
        try:
            (
                YouTube(
                    f"https://www.youtube.com/watch?v={video_id}",
                    use_oauth=True,
                    allow_oauth_cache=True,
                )
                .streams.filter(only_audio=True)
                .first()
                .download(str(save_path), filename="audio.mp4")
            )
            return True

        except KeyError as e:
            logger.error(f"Audio needed to download not found. Key error: {e}")
            return False

    @staticmethod
    def _merge_video_audio(
        save_path: Path,
    ) -> None:
        video_clip = VideoFileClip(str(save_path / "raw_video.mp4"))
        audio_clip = AudioFileClip(str(save_path / "audio.mp4"))
        final_clip = video_clip.set_audio(audio_clip)
        final_clip.write_videofile(str(save_path / "video.mp4"))

        os.remove(str(save_path / "raw_video.mp4"))
        os.remove(str(save_path / "audio.mp4"))

    def download_to_mp4(
        self,
        save_path: Path,
        video_id: str,
        resolution: str,
    ) -> bool:
        video_downloaded = self._download_video(
            save_path=save_path,
            video_id=video_id,
            resolution=resolution,
        )
        audio_downloaded = self._download_audio(
            save_path=save_path, video_id=video_id
        )
        if not (video_downloaded and audio_downloaded):
            return False

        self._merge_video_audio(save_path=save_path)
        return True


class MutualVideoAudioDownloader:
    @staticmethod
    def _download(save_path: Path, video_id: str) -> None:
        (
            YouTube(
                f"https://www.youtube.com/watch?v={video_id}",
                use_oauth=True,
                allow_oauth_cache=True,
            )
            .streams.filter(file_extension="mp4")
            .first()
            .download(str(save_path), filename="video.mp4")
        )

    def download_to_mp4(
        self, save_path: Path, video_id: str, **kwargs
    ) -> bool:
        try:
            self._download(save_path=save_path, video_id=video_id)
            return True

        except KeyError as e:
            logger.error(f"Video needed to download not found. Key error: {e}")
            return False
