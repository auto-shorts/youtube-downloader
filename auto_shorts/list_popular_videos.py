import os
import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from config import GCP_API_KEY
import pprint

class YoutubeConnector:
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


# def main():
#     # Disable OAuthlib's HTTPS verification when running locally.
#     # *DO NOT* leave this option enabled in production.
#     os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

#     api_service_name = "youtube"
#     api_version = "v3"

#     # Get credentials and create an API client
#     youtube = googleapiclient.discovery.build(
#         api_service_name, api_version, developerKey=GCP_API_KEY
#     )

#     request = youtube.videos().list(
#         part="contentDetails, id, liveStreamingDetails, localizations, player, recordingDetails, snippet, statistics, status, topicDetails",
#         chart="mostPopular",
#     )
#     response = request.execute()

#     print(response)


if __name__ == "__main__":
    connector = YoutubeConnector()
    pprint.pprint(connector.video_categories_by_region("us"))
