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


class VideoPreprocessorBase(ABC):
    @abstractmethod
    def __init__(self, video_data: dict) -> None:
        """Ensure video_data keyword exists"""

    @abstractmethod
    def preprocess_video(self) -> VideoData:
        """Main preprocessing function"""


class VideoPreprocessor(VideoPreprocessorBase):
    def __init__(self, video_data: dict) -> None:
        self.video_data = video_data

    @staticmethod
    def safeget(dct: dict, *keys) -> Any:
        for key in keys:
            try:
                dct = dct[key]
            except KeyError:
                return None
        return dct

    def preprocess_video(self) -> VideoData:
        statistics_data = self.safeget(self.video_data, "statistics")
        statistics = VideoStatistics(
            comments=self.safeget(statistics_data, "commentCount"),
            likes=self.safeget(statistics_data, "likeCount"),
            views=self.safeget(statistics_data, "viewCount"),
        )
        return VideoData(
            id=self.safeget(
                self.video_data,
                "id",
            ),
            channel_id=self.safeget(self.video_data, "snippet", "channelId"),
            channel_title=self.safeget(
                self.video_data, "snippet", "channelTitle"
            ),
            licensed=self.safeget(
                self.video_data, "contentDetails", "licensedContent"
            ),
            audio_language=self.safeget(
                self.video_data, "snippet", "defaultAudioLanguage"
            ),
            decripton=self.safeget(self.video_data, "snippet", "description"),
            published_at=self.safeget(
                self.video_data, "snippet", "publishedAt"
            ),
            category_id=self.safeget(self.video_data, "snippet", "categoryId"),
            tags=self.safeget(self.video_data, "snippet", "tags"),
            title=self.safeget(self.video_data, "snippet", "title"),
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
        preprocessor_class: VideoPreprocessorBase = VideoPreprocessor,
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

    def download_video_data(self, video_id: str) -> VideoData:
        request = self.youtube.videos().list(
            part=",".join(self.result_keys), id=video_id
        )
        video_data = request.execute()["items"][0]
        preprocessor = self.preprocessor_class(video_data=video_data)
        return preprocessor.preprocess_video()


if __name__ == "__main__":
    info_downloader = CategoryInfoDownloader()
    save_path = Path(__file__).parents[1] / "categories"
    pprint.pprint(info_downloader.video_categories_by_region(region_code="PL", save_path=save_path))
