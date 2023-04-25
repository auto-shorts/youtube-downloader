import requests
import pandas as pd
from abc import ABC, abstractmethod


class MostReplayedNotPresentException(Exception):
    """Exception thrown when video doesn't have most replayed moments avaible"""

    def __init__(
        self,
        message="Video doesn't have most replayed moments avaible",
        video_id: str = None,
    ):
        self.message = message
        if video_id:
            self.message = f"{message}. Video id: {video_id}"
        super().__init__(self.message)


class MostWatchedMomentsDownloaderBase(ABC):
    @abstractmethod
    def __init__(self, video_id: str) -> None:
        """Ensure video_id param exists"""

    @abstractmethod
    def get_most_watched_moments(self):
        """Main function"""


class MostWatchedMomentsDownloader(MostWatchedMomentsDownloaderBase):
    def __init__(self, video_id: str) -> None:
        self.video_id = video_id

    def _get_data_from_api(self) -> dict:
        url = f"https://yt.lemnoslife.com/videos?part=mostReplayed&id={self.video_id}"
        return requests.get(url).json()

    @staticmethod
    def _preprocess_results(raw_results: dict) -> pd.DataFrame:
        cleaned_timeframes = []

        for timeframe in raw_results["items"][0]["mostReplayed"][
            "heatMarkers"
        ]:  # check it in the future
            # print(timeframe)
            timeframe_data = timeframe["heatMarkerRenderer"]

            one_timeframe_cleaned = {
                "time_start_ms": timeframe_data["timeRangeStartMillis"],
                "time_end_ms": (
                    timeframe_data["timeRangeStartMillis"]
                    + timeframe_data["markerDurationMillis"]
                ),
                "peroid_duration_ms": timeframe_data["markerDurationMillis"],
                "intensity_score": timeframe_data["heatMarkerIntensityScoreNormalized"],
            }
            cleaned_timeframes.append(one_timeframe_cleaned)

        return pd.DataFrame(cleaned_timeframes)

    def get_most_watched_moments(self) -> pd.DataFrame:
        raw_results = self._get_data_from_api()
        if "error" in raw_results.keys():
            raise MostReplayedNotPresentException(video_id=self.video_id)

        if raw_results["items"][0]["mostReplayed"] is None:  # check if 0 is okay
            raise MostReplayedNotPresentException(video_id=self.video_id)

        return self._preprocess_results(raw_results=raw_results)

    def contain_most_watched(self) -> bool:
        try:
            self.get_most_watched_moments()
            return True
        except MostReplayedNotPresentException:
            return False


if __name__ == "__main__":
    moments_downloader = MostWatchedMomentsDownloader(video_id="3Xj9pJECk2o")
    print(
        moments_downloader.get_most_watched_moments().sort_values(
            by="intensity_score", ascending=False
        )
    )
