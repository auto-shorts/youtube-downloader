import json
import os
from pathlib import Path
from typing import Any
import googleapiclient.discovery
import googleapiclient.errors
from auto_shorts.config import GCP_API_KEY
import pprint
from pydantic import BaseModel
from abc import ABC, abstractmethod
from auto_shorts.utils import to_async
import pprint

BASE_RESULT_KEYS = [
    "contentDetails",
    "id",
    "liveStreamingDetails",
    "localizations",
    "player",
    "recordingDetails",
    "snippet",
    "statistics",
    "status",
    "topicDetails",
]
BASE_CHANNEL_RESULT_KEYS = [
    "contentDetails",
    "snippet",
]


class VideoStatistics(BaseModel):
    comments: int | None
    likes: int | None
    views: int | None


class VideoData(BaseModel):
    id: str
    channel_id: str | None
    channel_title: str | None
    licensed: bool | None
    audio_language: str | None
    decripton: str | None
    published_at: str | None
    category_id: str | None
    tags: list[str] | None
    title: str | None
    statistics: VideoStatistics | None


class VideoCategory(BaseModel):
    region_code: str
    category_id: int
    category_title: str
    assignable: bool


class PlaylistPageData(BaseModel):
    video_idx: list[str] | None
    next_page_token: str | None


def safe_get(dct: dict, *keys) -> Any:
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct


class PreprocessorBase(ABC):
    @abstractmethod
    def preprocess(self) -> VideoData:
        """Main preprocessing function"""


class ApiResponsePreprocessorBase(PreprocessorBase):
    @abstractmethod
    def __init__(self, video_data: dict) -> None:
        """Ensure video_data keyword exists"""


class ApiResponsePreprocessor(ApiResponsePreprocessorBase):
    def __init__(self, video_data: dict) -> None:
        self.video_data = video_data

    def preprocess(self) -> VideoData:
        statistics_data = safe_get(self.video_data, "statistics")
        statistics = VideoStatistics(
            comments=safe_get(statistics_data, "commentCount"),
            likes=safe_get(statistics_data, "likeCount"),
            views=safe_get(statistics_data, "viewCount"),
        )
        return VideoData(
            id=safe_get(
                self.video_data,
                "id",
            ),
            channel_id=safe_get(self.video_data, "snippet", "channelId"),
            channel_title=safe_get(self.video_data, "snippet", "channelTitle"),
            licensed=safe_get(
                self.video_data, "contentDetails", "licensedContent"
            ),
            audio_language=safe_get(
                self.video_data, "snippet", "defaultAudioLanguage"
            ),
            decripton=safe_get(self.video_data, "snippet", "description"),
            published_at=safe_get(self.video_data, "snippet", "publishedAt"),
            category_id=safe_get(self.video_data, "snippet", "categoryId"),
            tags=safe_get(self.video_data, "snippet", "tags"),
            title=safe_get(self.video_data, "snippet", "title"),
            statistics=statistics,
        )


class PlaylistInfoPreprocessorBase(PreprocessorBase):
    @abstractmethod
    def __init__(self, channel_data: dict) -> None:
        """Ensure channel_data keyword exists"""


class PlaylistInfoPreprocessor(PlaylistInfoPreprocessorBase):
    def __init__(self, playlist_response: dict) -> None:
        self.playlist_response = playlist_response

    def preprocess(self) -> PlaylistPageData:
        items = safe_get(self.playlist_response, "items")
        video_idx = [
            safe_get(video_data, "contentDetails", "videoId")
            for video_data in items
        ]
        next_page_token = safe_get(self.playlist_response, "nextPageToken")
        return PlaylistPageData(
            video_idx=video_idx, next_page_token=next_page_token
        )


class InfoDownloaderBase:
    def __init__(
        self,
        api_key: str = GCP_API_KEY,
        api_service_name: str = "youtube",
        api_version: str = "v3",
    ) -> None:
        self.youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=api_key
        )


class CategoryInfoDownloader(InfoDownloaderBase):
    def video_categories_by_region(
        self, region_code: str, save_path: Path | None = None
    ) -> dict:
        request = self.youtube.videoCategories().list(
            part="snippet", regionCode=region_code
        )
        response = request.execute()
        categories = []

        for item in response["items"]:
            snippet = item["snippet"]
            category = VideoCategory(
                region_code=region_code,
                category_id=item["id"],
                assignable=snippet["assignable"],
                category_title=snippet["title"],
            )
            categories.append(category)

        if save_path:
            os.makedirs(save_path, exist_ok=True)
            with open(save_path / f"{region_code}.json", "w") as file:
                json.dump(
                    [category.dict() for category in categories],
                    file,
                    indent=4,
                )

        return categories


class VideoInfoDownloader(InfoDownloaderBase):
    def __init__(
        self,
        preprocessor_class: ApiResponsePreprocessorBase = ApiResponsePreprocessor,
        result_keys: list = BASE_RESULT_KEYS,
    ) -> None:
        super().__init__()
        self.preprocessor_class = preprocessor_class
        self.result_keys = result_keys

    def most_popular_videos(
        self,
        region_code: str | None = None,
        video_category_id: int | None = None,
        max_results: int | None = 100,
    ) -> list[VideoData]:

        request = self.youtube.videos().list(
            part=",".join(self.result_keys),
            chart="mostPopular",
            regionCode=region_code,
            videoCategoryId=video_category_id,
            maxResults=max_results,
        )
        all_videos = request.execute()["items"]
        return [
            self.preprocessor_class(video_data=video_data).preprocess()
            for video_data in all_videos
        ]

    def id_from_response(self, response: dict) -> list[str]:
        return [
            self.preprocessor_class.safe_get(video_data, "id", "videoId")
            for video_data in response["items"]
        ]

    def download_video_data(self, video_id: str) -> list[VideoData]:
        """
        Video id might be one id or string with
        multiple id separated by coma.
        """
        request = self.youtube.videos().list(
            part=",".join(self.result_keys), id=video_id
        )
        response = request.execute()["items"]
        video_data_preprocessed = []

        for video_data in response:
            preprocessor = self.preprocessor_class(video_data=video_data)
            video_data_preprocessed.append(preprocessor.preprocess())

        return video_data_preprocessed

    def video_id_by_page_token(self, page_token: str) -> tuple[list[str], str]:
        request = self.youtube.search().list(
            part="snippet", pageToken=page_token
        )
        response = request.execute()
        try:
            next_page_token = response["nextPageToken"]
        except KeyError:
            pprint.pprint(response)
        return self.id_from_response(response), next_page_token

    def video_id_by_search_query(
        self, q: str, max_results: int = 100, order: str = "viewCount"
    ) -> list[str]:
        request = self.youtube.search().list(
            q=q,
            part="snippet",
            maxResults=max_results,
            order=order,
            type="video"
            # videoLicense="creativeCommon", To check later!
        )
        response = request.execute()
        video_id = self.id_from_response(response)
        next_page_token = response["nextPageToken"]

        while len(video_id) < max_results:
            tmp_video_id, next_page_token = self.video_id_by_page_token(
                next_page_token
            )
            video_id.extend(tmp_video_id)

        return video_id

    def video_data_by_search_query(
        self, q: str, max_results: int = 100, order: str = "viewCount"
    ) -> list[VideoData]:
        video_id = self.video_id_by_search_query(
            q=q, max_results=max_results, order=order
        )
        return self.download_video_data(video_id=",".join(video_id))


class ChannelInfoDownloader(InfoDownloaderBase):
    def __init__(
        self,
        playlist_preprocessor: PlaylistInfoPreprocessor = PlaylistInfoPreprocessor,
        result_keys: list = BASE_CHANNEL_RESULT_KEYS,
    ) -> None:
        super().__init__()
        self.playlist_preprocessor = playlist_preprocessor
        self.result_keys = result_keys

    def _get_user_playlist_id_from_video(self, video_id: str) -> str:
        video_downloader = VideoInfoDownloader()
        channel_id = video_downloader.download_video_data(video_id=video_id)[
            0
        ].channel_id
        return f"UU{channel_id[2:]}"

    def _next_page_download(
        self, next_page_token: str, playlist_id: str
    ) -> PlaylistPageData:
        request = self.youtube.playlistItems().list(
            part=",".join(self.result_keys),
            pageToken=next_page_token,
            playlistId=playlist_id,
        )
        response = request.execute()
        return self.playlist_preprocessor(response).preprocess()

    def download_from_one_video(
        self, video_id: str, video_number_limit: int = 1000
    ):
        playlist_id = self._get_user_playlist_id_from_video(video_id)
        request = self.youtube.playlistItems().list(
            part=",".join(self.result_keys), playlistId=playlist_id
        )
        response = request.execute()
        playlist_data = self.playlist_preprocessor(response).preprocess()
        video_idx: list[str] = []
        video_idx.extend(playlist_data.video_idx)
        next_page_token = playlist_data.next_page_token

        while (
            next_page_token is not None and len(video_idx) < video_number_limit
        ):
            playlist_data = self._next_page_download(
                next_page_token=next_page_token, playlist_id=playlist_id
            )
            video_idx.extend(playlist_data.video_idx)

        return video_idx


if __name__ == "__main__":
    channel = ChannelInfoDownloader()
    pprint.pprint(
        channel.download_from_one_video(
            video_id="1fUpkq7urDU", video_number_limit=10
        )
    )
