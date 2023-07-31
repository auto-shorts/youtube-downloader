from typing import Iterable

from pydantic import BaseModel, validator


class VideoStatistics(BaseModel):
    comments: int | None
    likes: int | None
    views: int | None


class VideoData(BaseModel):
    id: str
    channel_id: str | None
    channel_title: str | None
    audio_language: str | None
    licensed: bool | None
    description: str | None
    published_at: str | None
    category_id: str
    tags: list[str] | None
    title: str | None

    @validator("tags", pre=True, always=True)
    def ensure_tags_is_iterable(cls, v):
        if v is None:
            return []
        elif not isinstance(v, Iterable):
            return [v]
        return v


class VideoDataWithStats(VideoData):
    statistics: VideoStatistics | None


class PlaylistVideoData(BaseModel):
    video_data: list[VideoData]
    next_page_token: str | None
