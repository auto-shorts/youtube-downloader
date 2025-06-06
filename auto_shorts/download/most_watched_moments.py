import pprint
from abc import ABC, abstractmethod

import pandas as pd
import requests


class MostReplayedNotPresentException(Exception):
    """Exception thrown when video doesn't have most replayed moments available"""

    def __init__(
        self,
        message="Video doesn't have most replayed moments available",
        video_id: str = None,
    ):
        self.message = message
        if video_id:
            self.message = f"{message}. Video id: {video_id}"
        super().__init__(self.message)


class MostWatchedMomentsDownloaderBase(ABC):
    @abstractmethod
    def get_most_watched_moments(self, video_id: str):
        """Main function"""


class MostWatchedMomentsDownloader(MostWatchedMomentsDownloaderBase):
    @staticmethod
    def _get_data_from_api(video_id) -> dict:
        url = (
            f"https://yt.lemnoslife.com/videos?part=mostReplayed&id={video_id}"
        )
        return requests.get(url).json()

    @staticmethod
    def _preprocess_results(raw_results: dict) -> pd.DataFrame:
        cleaned_timeframes = []

        marker_duration_millis = raw_results["items"][0]["mostReplayed"][
            "markers"
        ][1]["startMillis"]
        for timeframe in raw_results["items"][0]["mostReplayed"][
            "markers"
        ]:  # check it in the future
            # print(timeframe)
            one_timeframe_cleaned = {
                "time_start_ms": timeframe["startMillis"],
                "time_end_ms": (
                    timeframe["startMillis"]
                    + marker_duration_millis
                ),
                "peroid_duration_ms": marker_duration_millis,
                "intensity_score": timeframe[
                    "intensityScoreNormalized"
                ],
            }
            cleaned_timeframes.append(one_timeframe_cleaned)

        return pd.DataFrame(cleaned_timeframes)

    def get_most_watched_moments(self, video_id: str) -> pd.DataFrame:
        raw_results = self._get_data_from_api(video_id=video_id)
        if "error" in raw_results.keys():
            raise MostReplayedNotPresentException(video_id=video_id)

        if (
            raw_results["items"][0]["mostReplayed"] is None
        ):  # check if 0 is okay
            raise MostReplayedNotPresentException(video_id=video_id)

        return self._preprocess_results(raw_results=raw_results)

    def contain_most_watched(self, video_id: str) -> bool:
        try:
            self.get_most_watched_moments(video_id=video_id)
            return True
        except MostReplayedNotPresentException:
            return False


if __name__ == "__main__":
    moments_downloader = MostWatchedMomentsDownloader()
    print(
        moments_downloader.get_most_watched_moments(
            video_id="3Xj9pJECk2o"
        ).sort_values(by="intensity_score", ascending=False)
    )
