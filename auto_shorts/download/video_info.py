import pprint
from typing import Protocol

import googleapiclient.discovery
import googleapiclient.errors
from loguru import logger
from youtube_transcript_api import YouTubeTranscriptApi

from auto_shorts.config import GCP_API_KEY
from auto_shorts.download.models.video_info import (
    PlaylistVideoData,
    VideoData,
    VideoDataWithStats,
    VideoStatistics,
)
from auto_shorts.utils import safe_get

BASE_RESULT_KEYS = (
    "contentDetails",
    "snippet",
    "statistics",
)

BASE_PLAYLIST_RESULT_KEYS = (
    "contentDetails",
    "snippet",
)


def preprocess_video_response(video_data) -> VideoData:
    category_id = safe_get(video_data, "snippet", "categoryId")
    data_raw = dict(
        id=safe_get(
            video_data,
            "id",
        ),
        channel_id=safe_get(video_data, "snippet", "channelId"),
        channel_title=safe_get(video_data, "snippet", "channelTitle"),
        licensed=safe_get(video_data, "contentDetails", "licensedContent"),
        audio_language=safe_get(video_data, "snippet", "defaultAudioLanguage"),
        description=safe_get(video_data, "snippet", "description"),
        published_at=safe_get(video_data, "snippet", "publishedAt"),
        category_id=category_id if category_id else 999,  # meaning no category
        tags=safe_get(video_data, "snippet", "tags"),
        title=safe_get(video_data, "snippet", "title"),
    )
    # TODO Find bug with % when downloading from mrbeast channel
    return VideoData(
        **{
            k: (
                v.replace("'", "").replace("%", "").replace("%", "")
                if isinstance(v, str)
                else v
            )
            for k, v in data_raw.items()
        }
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
    video_data = [
        preprocess_playlist_item_response(video_data) for video_data in items
    ]
    next_page_token = safe_get(playlist_response, "nextPageToken")
    return PlaylistVideoData(
        video_data=video_data, next_page_token=next_page_token
    )


class InfoDownloaderBase:
    """Base class for downloading data from the YouTube API."""

    def __init__(
        self,
        api_key: str = GCP_API_KEY,
        api_service_name: str = "youtube",
        api_version: str = "v3",
    ) -> None:
        """Initializes a new instance of the InfoDownloaderBase class.

        Args:
            api_key (str): Your GCP API key.
            api_service_name (str): The name of the API service to use (default: "youtube").
            api_version (str): The version of the API to use (default: "v3").
        """
        self.youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=api_key
        )


class VideoInfoDownloaderInterface(Protocol):
    def download_video_data(self, video_id: str) -> list[VideoDataWithStats]:
        ...


class VideoInfoDownloader(InfoDownloaderBase):
    """Downloads video information from YouTube using YouTube API v3.

    Args:
        result_keys (list): List of video information keys to download.
            Defaults to BASE_RESULT_KEYS.

    Methods:
        most_popular_videos(region_code, video_category_id, max_results):
            Returns the most popular videos for a given region or category.

        id_from_response(response):
            Extracts video ids from a response dictionary.

        download_video_data(video_id):
            Downloads video data for one or more video ids.

        video_id_by_page_token(page_token):
            Extracts video ids and a next page token from a page token.

        video_id_by_search_query(q, max_results, order):
            Returns video ids for a search query.

        video_data_by_search_query(q, max_results, order):
            Returns video data for a search query.
    """

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
        """Returns the most popular videos for a given region or category.

        Args:
            region_code (str): ISO 3166-1 alpha-2 code of the region. Defaults
                to None.
            video_category_id (int): ID of the video category. Defaults to None.
            max_results (int): Maximum number of results to return. Defaults to
                100.

        Returns:
            A list of VideoDataWithStats objects.
        """
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
        """Extracts video ids from a response dictionary.

        Args:
            response (dict): The response dictionary.

        Returns:
            A list of video ids.
        """
        return [
            safe_get(video_data, "id", "videoId")
            for video_data in response["items"]
        ]

    def download_video_data(self, video_id: str) -> list[VideoDataWithStats]:
        """Downloads video data for one or more video ids.

        Args:
            video_id (str): One or more video ids separated by comma.

        Returns:
            A list of VideoDataWithStats objects.
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
        """Extracts video ids and a next page token from API using previous
        page token.

        Args:
            page_token (str): A page token.

        Returns:
            A tuple of a list of video ids and the next page token.
        """
        request = self.youtube.search().list(
            part="snippet", pageToken=page_token
        )
        response = request.execute()
        try:
            next_page_token = response["nextPageToken"]
            return self.id_from_response(response), next_page_token
        except KeyError as e:
            logger.error(e)

    def video_id_by_search_query(
        self, q: str, max_results: int = 100, order: str = "viewCount"
    ) -> list[str]:
        """Returns video ids for a search query.

        Args:
            q (str): The search query.
            max_results (int): Maximum number of results to return. Defaults to
                100.
            order (str): The order of the results. Defaults to "viewCount".

        Returns:
            A list of video ids.
        """
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
    ) -> list[VideoDataWithStats]:
        """Returns video data for a search query.

        Args:
            q (str): The search query.
            max_results (int): Maximum number of results to return. Defaults to
                100.
            order (str): The order of the results. Defaults to "viewCount".

        Returns:
            A list of VideoDataWithStats objects.
        """
        video_id = self.video_id_by_search_query(
            q=q, max_results=max_results, order=order
        )
        return self.download_video_data(video_id=",".join(video_id))


if __name__ == "__main__":
    info_downloader_test = VideoInfoDownloader()
    video_id_test = "1WEAJ-DFkHE"
    pprint.pprint(YouTubeTranscriptApi.get_transcript(video_id_test))
    # pprint.pprint(info_downloader_test.download_video_data("1WEAJ-DFkHE"))
