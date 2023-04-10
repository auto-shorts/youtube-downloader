import json
import os
import pprint
from pathlib import Path
from typing import Any

import googleapiclient.discovery
import googleapiclient.errors
from pydantic import BaseModel

from auto_shorts.config import GCP_API_KEY

BASE_RESULT_KEYS = (
    "contentDetails",
    "snippet",
    "statistics",
)

BASE_PLAYLIST_RESULT_KEYS = (
    "contentDetails",
    "snippet",
)


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
    region_code: str
    category_id: int
    category_title: str
    assignable: bool


class PlaylistVideoData(BaseModel):
    video_data: list[VideoData]
    next_page_token: str | None


def safe_get(dct: dict, *keys) -> Any:
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct


def preprocess_video_response(video_data) -> VideoData:
    return VideoData(
        id=safe_get(
            video_data,
            "id",
        ),
        channel_id=safe_get(video_data, "snippet", "channelId"),
        channel_title=safe_get(video_data, "snippet", "channelTitle"),
        licensed=safe_get(video_data, "contentDetails", "licensedContent"),
        audio_language=safe_get(video_data, "snippet", "defaultAudioLanguage"),
        decripton=safe_get(video_data, "snippet", "description"),
        published_at=safe_get(video_data, "snippet", "publishedAt"),
        category_id=safe_get(video_data, "snippet", "categoryId"),
        tags=safe_get(video_data, "snippet", "tags"),
        title=safe_get(video_data, "snippet", "title"),
    )


def preprocess_video_response_with_stats(video_data) -> VideoDataWithStats:
    base_response = preprocess_video_response(video_data)
    statistics_data = safe_get(video_data, "statistics")
    statistics = VideoStatistics(
        comments=safe_get(statistics_data, "commentCount"),
        likes=safe_get(statistics_data, "likeCount"),
        views=safe_get(statistics_data, "viewCount"),
    )
    return VideoDataWithStats(**base_response.dict(), statistics=statistics)


def preprocess_playlist_item_response(video_data) -> VideoData:
    video_data_dict = preprocess_video_response(video_data).dict()
    idx = safe_get(video_data, "contentDetails", "videoId")
    video_data_dict["id"] = idx
    return VideoData(**video_data_dict)


def preprocess_playlist(playlist_response) -> PlaylistVideoData:
    items = safe_get(playlist_response, "items")
    video_data = [preprocess_playlist_item_response(video_data) for video_data in items]
    next_page_token = safe_get(playlist_response, "nextPageToken")
    return PlaylistVideoData(video_data=video_data, next_page_token=next_page_token)


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
    ) -> list[VideoCategory]:
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
            result_keys: list = BASE_RESULT_KEYS,
    ) -> None:
        super().__init__()
        self.result_keys = result_keys

    def most_popular_videos(
            self,
            region_code: str | None = None,
            video_category_id: int | None = None,
            max_results: int | None = 100,
    ) -> list[VideoDataWithStats]:

        request = self.youtube.videos().list(
            part=",".join(self.result_keys),
            chart="mostPopular",
            regionCode=region_code,
            videoCategoryId=video_category_id,
            maxResults=max_results,
        )
        all_videos = request.execute()["items"]
        return [
            preprocess_video_response_with_stats(video_data)
            for video_data in all_videos
        ]

    @staticmethod
    def id_from_response(response: dict) -> list[str]:
        return [
            safe_get(video_data, "id", "videoId") for video_data in response["items"]
        ]

    def download_video_data(self, video_id: str) -> list[VideoDataWithStats]:
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
            video_data_preprocessed.append(
                preprocess_video_response_with_stats(video_data)
            )

        return video_data_preprocessed

    def video_id_by_page_token(self, page_token: str) -> tuple[list[str], str]:
        request = self.youtube.search().list(part="snippet", pageToken=page_token)
        response = request.execute()
        try:
            next_page_token = response["nextPageToken"]
            return self.id_from_response(response), next_page_token
        except KeyError:
            pprint.pprint(response)

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
            tmp_video_id, next_page_token = self.video_id_by_page_token(next_page_token)
            video_id.extend(tmp_video_id)

        return video_id

    def video_data_by_search_query(
            self, q: str, max_results: int = 100, order: str = "viewCount"
    ) -> list[VideoDataWithStats]:
        video_id = self.video_id_by_search_query(
            q=q, max_results=max_results, order=order
        )
        return self.download_video_data(video_id=",".join(video_id))


class ChannelInfoDownloader(InfoDownloaderBase):
    def __init__(
            self,
            result_keys: tuple = BASE_PLAYLIST_RESULT_KEYS,
    ) -> None:
        super().__init__()
        self.result_keys = result_keys

    @staticmethod
    def _get_user_playlist_id_from_video(video_id: str) -> str:
        video_downloader = VideoInfoDownloader()
        channel_id = video_downloader.download_video_data(video_id=video_id)[
            0
        ].channel_id
        return f"UU{channel_id[2:]}"

    def _next_page_download(
            self, next_page_token: str, playlist_id: str
    ) -> PlaylistVideoData:
        request = self.youtube.playlistItems().list(
            part=",".join(self.result_keys),
            pageToken=next_page_token,
            playlistId=playlist_id,
        )
        response = request.execute()
        return preprocess_playlist(response)

    def get_videos_from_channel(
            self, video_id: str, video_number_limit: int = 1000
    ) -> list[VideoData]:
        playlist_id = self._get_user_playlist_id_from_video(video_id)
        request = self.youtube.playlistItems().list(
            part=",".join(self.result_keys), playlistId=playlist_id
        )
        response = request.execute()
        playlist_data = preprocess_playlist(response)
        video_data: list[VideoData] = [*playlist_data.video_data]
        next_page_token = playlist_data.next_page_token

        while next_page_token is not None and len(video_data) < video_number_limit:
            playlist_data = self._next_page_download(
                next_page_token=next_page_token, playlist_id=playlist_id
            )
            video_data.extend(playlist_data.video_data)

        return video_data


if __name__ == "__main__":
    channel = ChannelInfoDownloader()
    pprint.pprint(
        channel._get_user_playlist_id_from_video(video_id="1fUpkq7urDU")
    )
