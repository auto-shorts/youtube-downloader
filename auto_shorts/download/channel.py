from pprint import pprint
from typing import Protocol

from auto_shorts.db.upload import upload_channel_info
from auto_shorts.download.models.channel import ChannelInfo
from auto_shorts.download.models.video_info import PlaylistVideoData, VideoData
from auto_shorts.download.video_info import (
    BASE_PLAYLIST_RESULT_KEYS,
    InfoDownloaderBase,
    VideoInfoDownloader,
    preprocess_playlist,
)
from auto_shorts.utils import safe_get


class ChannelInfoDownloaderInterface(Protocol):
    def get_videos_from_channel(
        self,
        video_id: str,
        video_info_limit: int,
        max_results_per_page: int = 20,
    ) -> list[VideoData]:
        ...

    def get_info(self, channel_id: str) -> ChannelInfo:
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

    def get_info(self, channel_id: str) -> ChannelInfo:
        request = self.youtube.channels().list(
            part="snippet,contentDetails,statistics", id=channel_id
        )
        response = request.execute()
        channel_data = safe_get(response, "items")[0]
        snippet = safe_get(channel_data, "snippet")
        statistics = safe_get(channel_data, "statistics")
        return ChannelInfo(
            channel_id=channel_id,
            title=snippet["title"],
            description=snippet["description"],
            custom_url=snippet["customUrl"],
            views=statistics["viewCount"],
            subscribers=statistics["subscriberCount"],
        )

    def push_info_to_db(self, channel_id: str) -> None:
        channel_info = self.get_info(channel_id)
        upload_channel_info(channel_info)


if __name__ == "__main__":
    downloader_test = ChannelInfoDownloader()
    downloader_test.push_info_to_db("UCjXfkj5iapKHJrhYfAF9ZGg")
