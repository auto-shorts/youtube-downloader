from datetime import datetime
from typing import Protocol

import pytz

from auto_shorts.download.video_info import VideoData, VideoDataWithStats
from auto_shorts.utils import datetime_from_iso_str

VideoDataList = list[VideoData | VideoDataWithStats]


class VideoDataParserInterface(Protocol):
    def select_videos_by_date(
        self, video_data_list: VideoDataList, date_from: str, date_to
    ) -> list[VideoData | VideoDataWithStats]:
        ...


class VideoDataParser:
    @staticmethod
    def prepare_date_from_user(date: str) -> datetime:
        return datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=pytz.utc)

    @staticmethod
    def prepare_video_date(date: str) -> datetime:
        return datetime_from_iso_str(date)

    @staticmethod
    def check_date(
        video_data_date: datetime,
        date_from: datetime | None,
        date_to: datetime | None,
    ) -> bool:
        if not video_data_date:
            return False

        if date_from and video_data_date < date_from:
            return False

        if date_to and video_data_date > date_to:
            return False

        return True

    def select_videos_by_date(
        self,
        video_data_list: VideoDataList,
        date_from: str | None,
        date_to: str | None,
    ) -> VideoDataList:
        date_from = (
            self.prepare_date_from_user(date_from) if date_from else None
        )
        date_to = self.prepare_date_from_user(date_to) if date_to else None

        return [
            video_data
            for video_data in video_data_list
            if self.check_date(
                video_data_date=self.prepare_video_date(
                    video_data.published_at
                ),
                date_from=date_from,
                date_to=date_to,
            )
        ]
