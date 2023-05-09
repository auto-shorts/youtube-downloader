from pydantic import BaseModel


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


class VideoDataWithStats(VideoData):
    statistics: VideoStatistics | None


class PlaylistVideoData(BaseModel):
    video_data: list[VideoData]
    next_page_token: str | None
