from pathlib import Path

from pydantic import BaseModel

from auto_shorts.download.models.transcription import TranscriptionData
from auto_shorts.download.models.video_info import VideoDataWithStats

base_data_path = Path(__file__).parents[3] / "data"


class TranscriptionAndMoments(BaseModel):
    most_watched_moments: list[dict]
    transcription: TranscriptionData | None


class DownloadConfig(BaseModel):
    save_path: Path = base_data_path
    bucket: str = "auto-shorts"
    to_s3: bool = False
    save_local: bool = True


class DownloadParams(DownloadConfig):
    video_data: VideoDataWithStats
    resolution: str = "480p"


class DownloadedVideoResults(BaseModel):
    video_id: str
    successful_download: bool
