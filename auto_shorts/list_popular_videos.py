import json
import os
from pathlib import Path
from typing import Any
import googleapiclient.discovery
import googleapiclient.errors
from config import GCP_API_KEY
import pprint
from pydantic import BaseModel
from abc import ABC, abstractmethod

base_result_keys = [
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


class ApiResponsePreprocessorBase(ABC):
    @abstractmethod
    def __init__(self, video_data: dict) -> None:
        """Ensure video_data keyword exists"""

    @abstractmethod
    def preprocess_video(self) -> VideoData:
        """Main preprocessing function"""


class ApiResponsePreprocessor(ApiResponsePreprocessorBase):
    def __init__(self, video_data: dict) -> None:
        self.video_data = video_data

    @staticmethod
    def safe_get(dct: dict, *keys) -> Any:
        for key in keys:
            try:
                dct = dct[key]
            except KeyError:
                return None
        return dct

    def preprocess_video(self) -> VideoData:
        statistics_data = self.safe_get(self.video_data, "statistics")
        statistics = VideoStatistics(
            comments=self.safe_get(statistics_data, "commentCount"),
            likes=self.safe_get(statistics_data, "likeCount"),
            views=self.safe_get(statistics_data, "viewCount"),
        )
        return VideoData(
            id=self.safe_get(
                self.video_data,
                "id",
            ),
            channel_id=self.safe_get(self.video_data, "snippet", "channelId"),
            channel_title=self.safe_get(
                self.video_data, "snippet", "channelTitle"
            ),
            licensed=self.safe_get(
                self.video_data, "contentDetails", "licensedContent"
            ),
            audio_language=self.safe_get(
                self.video_data, "snippet", "defaultAudioLanguage"
            ),
            decripton=self.safe_get(self.video_data, "snippet", "description"),
            published_at=self.safe_get(
                self.video_data, "snippet", "publishedAt"
            ),
            category_id=self.safe_get(
                self.video_data, "snippet", "categoryId"
            ),
            tags=self.safe_get(self.video_data, "snippet", "tags"),
            title=self.safe_get(self.video_data, "snippet", "title"),
            statistics=statistics,
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
        result_keys: list = base_result_keys,
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
            self.preprocessor_class(video_data=video_data).preprocess_video()
            for video_data in all_videos
        ]

    def id_from_response(self, response: dict) -> list[str]:
        return [
            self.preprocessor_class.safe_get(video_data, "id", "videoId")
            for video_data in response["items"]
        ]

    def download_video_data(self, video_id: str) -> VideoData:
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
            video_data_preprocessed.append(preprocessor.preprocess_video())

        return video_data_preprocessed

    def video_id_by_page_token(self, page_token: str) -> tuple[list[str], str]:
        request = self.youtube.search().list(
            part="snippet", pageToken=page_token
        )
        response = request.execute()
        next_page_token = response["nextPageToken"]

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
            tmp_video_id, next_page_token = self.video_id_by_page_token(next_page_token)
            video_id.extend(tmp_video_id)
            
        return video_id

    def video_data_by_search_query(
        self, q: str, max_results: int = 100, order: str = "viewCount"
    ) -> list[VideoData]:
        video_id = self.video_id_by_search_query(
            q=q, max_results=max_results, order=order
        )
        print(video_id)
        return self.download_video_data(video_id=",".join(video_id))


if __name__ == "__main__":
    info_downloader = VideoInfoDownloader()
    q = "blinders"
    results = info_downloader.video_data_by_search_query(q=q, max_results=10)
    #pprint.pprint(results)
    # print(f"Keys: {results.keys()}")
    print(len(results))
    # info_downloader = CategoryInfoDownloader()
    # save_path = Path(__file__).parents[1] / "categories"
    # pprint.pprint(info_downloader.video_categories_by_region(region_code="PL", save_path=save_path))
