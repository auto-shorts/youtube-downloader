import os
import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from config import GCP_API_KEY
import pprint


class YoutubeDataDownloader:
    def __init__(
        self,
        api_key: str = GCP_API_KEY,
        api_service_name: str = "youtube",
        api_version: str = "v3",
    ) -> None:
        self.youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=api_key
        )

    def video_categories_by_region(self, region_code: str) -> dict:
        request = self.youtube.videoCategories().list(
            part="snippet", regionCode=region_code
        )
        response = request.execute()
        categories = {}

        for item in response["items"]:
            snippet = item["snippet"]
            categories[item["id"]] = {
                "title": snippet["title"],
                "assignable": snippet["assignable"],
                "etag": item["etag"],
            }

        return categories

    def most_popular_videos(
        self, region_code: str | None = None, video_category: int | None = None
    ) -> dict:
        result_keys = [
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
        request = self.youtube.videos().list(
            part=",".join(result_keys),
            chart="mostPopular",
            regionCode=region_code,
            videoCategoryId=video_category,
        )
        return request.execute()["items"]


if __name__ == "__main__":
    connector = YoutubeDataDownloader()
    pprint.pprint(connector.most_popular_videos()[0])
