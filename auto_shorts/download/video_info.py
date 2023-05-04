import json
import os
import pprint
from pathlib import Path
from typing import Protocol

import googleapiclient.discovery
import googleapiclient.errors
from loguru import logger
from pydantic import BaseModel
from youtube_transcript_api import YouTubeTranscriptApi

from auto_shorts.config import GCP_API_KEY
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
        description=safe_get(video_data, "snippet", "description"),
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


class CategoryInfoDownloader(InfoDownloaderBase):
    """Class for downloading information about video categories from YouTube
    API.

    Methods:
        video_categories_by_region(region_code save_path)
            Fetches video categories for the given region code from the YouTube API.
    """

    def video_categories_by_region(
        self, region_code: str, save_path: Path | None = None
    ) -> list[VideoCategory]:
        """Fetches video categories for the given region code from the YouTube
        API.

        Parameters
        ----------
        region_code : str
            The two-letter ISO 3166-1 alpha-2 country code for the region whose video categories are to be fetched.
        save_path : Optional[Path], optional
            The path where the fetched video categories data should be saved as a JSON file. Default value is None,
            which means the data won't be saved.

        Returns
        -------
        categories : List[VideoCategory]
            A list of VideoCategory objects, where each object contains information about a single video category.

        Raises
        ------
        HttpError
            If there's an error in fetching the video categories data from the YouTube API.
        """
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
        pprint.pprint(response)
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


class ChannelInfoDownloaderInterface(Protocol):
    def get_videos_from_channel(
        self,
        video_id: str,
        video_info_limit: int,
        max_results_per_page: int = 20,
    ) -> list[VideoData]:
        ...


class ChannelInfoDownloader(InfoDownloaderBase):
    """A class for downloading information about videos from a YouTube channel.

    Args:
        result_keys (tuple): A tuple of keys to be included in the request for the
            channel's video information.

    Methods:
        _get_user_playlist_id_from_video(video_id: str) -> str:
            Returns the ID of the playlist containing the videos for the specified
            video ID.

        _next_page_download(next_page_token: str, playlist_id: str) -> PlaylistVideoData:
            Downloads the next page of videos from the specified playlist and returns
            the preprocessed data.

        get_videos_from_channel(video_id: str, video_number_limit: int = 1000)
            -> list[VideoData]:
            Returns a list of VideoData objects containing the video information from
            the specified channel up to the specified limit.
    """

    def __init__(
        self,
        result_keys: tuple = BASE_PLAYLIST_RESULT_KEYS,
    ) -> None:
        super().__init__()
        self.result_keys = result_keys

    @staticmethod
    def _get_user_playlist_id_from_video(video_id: str) -> str:
        """Returns the ID of the playlist containing the videos for the
        specified video ID.

        Args:
            video_id (str): The ID of the video.

        Returns:
            str: The ID of the playlist containing the videos for the specified video ID.
        """
        video_downloader = VideoInfoDownloader()
        channel_id = video_downloader.download_video_data(video_id=video_id)[
            0
        ].channel_id
        return f"UU{channel_id[2:]}"

    def _next_page_download(
        self,
        next_page_token: str,
        playlist_id: str,
    ) -> PlaylistVideoData:
        """Downloads the next page of videos from the specified playlist and
        returns the preprocessed data.

        Args:
            next_page_token (str): The token representing the next page of videos.
            playlist_id (str): The ID of the playlist to download videos from.

        Returns:
            PlaylistVideoData: An object containing the preprocessed data for the
            downloaded videos.
        """
        request = self.youtube.playlistItems().list(
            part=",".join(self.result_keys),
            pageToken=next_page_token,
            playlistId=playlist_id,
        )
        response = request.execute()
        return preprocess_playlist(response)

    def get_videos_from_channel(
        self,
        video_id: str,
        video_info_limit: int,
        max_results_per_page: int = 20,
    ) -> list[VideoData]:
        """Returns a list of VideoData objects containing the video information
        from the specified channel up to the specified limit.

        Args:
            video_id (str): The ID of a video in the channel.
            max_results_per_page (int): Number of videos returned in each playlist page.
            video_info_limit (int): Limit of requested videos from channel

        Returns:
            list[VideoData]: A list of VideoData objects containing the video
            information from the specified channel up to the specified limit.
        """
        playlist_id = self._get_user_playlist_id_from_video(video_id)
        request = self.youtube.playlistItems().list(
            part=",".join(self.result_keys),
            playlistId=playlist_id,
            maxResults=max_results_per_page,
        )
        response = request.execute()
        playlist_data = preprocess_playlist(response)
        video_data: list[VideoData] = [*playlist_data.video_data]
        next_page_token = playlist_data.next_page_token

        while next_page_token and len(video_data) < video_info_limit:
            playlist_data = self._next_page_download(
                next_page_token=next_page_token,
                playlist_id=playlist_id,
            )
            video_data.extend(playlist_data.video_data)

        return video_data


if __name__ == "__main__":
    info_downloader_test = VideoInfoDownloader()
    video_id_test = "1WEAJ-DFkHE"
    pprint.pprint(YouTubeTranscriptApi.get_transcript(video_id_test))
    # pprint.pprint(info_downloader_test.download_video_data("1WEAJ-DFkHE"))
