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
    category_id: str | None
    tags: list[str] | None
    title: str | None


class VideoDataWithStats(VideoData):
    statistics: VideoStatistics | None


class VideoCategory(BaseModel):
    """Class representing a video category on YouTube.

    Attributes
    ----------
    region_code : str
        The two-letter ISO 3166-1 alpha-2 country code for the region to which the video category belongs.
    category_id : str
        The ID of the video category.
    assignable : bool
        Whether the video category can be used as a target for new videos.
    category_title : str
        The title of the video category.
    """

    region_code: str
    category_id: int
    category_title: str
    assignable: bool


class PlaylistVideoData(BaseModel):
    video_data: list[VideoData]
    next_page_token: str | None
