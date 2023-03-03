from abc import ABC, abstractmethod
from auto_shorts.list_popular_videos import VideoData, VideoInfoDownloader
from auto_shorts.most_watched_moments import MostWatchedMomentsDownloader

class VideoToDownloadChooseBase(ABC):
    @abstractmethod
    def __init__(self, videos: list[VideoData]) -> None:
        """Ensure that list of videos is provided"""

    @abstractmethod
    def get_videos_with_moments(self) -> list[VideoData]:
        """Main functionality"""


class VideoToDownloadChooser(VideoToDownloadChooseBase):
    def __init__(self, videos: list[VideoData]) -> None:
        self.videos = videos
        
    def get_videos_with_moments(self) -> list[VideoData]:
        videos_to_download = []
        
        for video in self.videos:
            moments_downloader = MostWatchedMomentsDownloader(video_id=video.id)
            if moments_downloader.contain_most_watched():
                videos_to_download.append(video)
        
        return videos_to_download
    

if __name__ == "__main__":
    downloader = VideoInfoDownloader()
    most_watched_videos = downloader.most_popular_videos(max_results=10000)
    video_chooser = VideoToDownloadChooser(videos=most_watched_videos)
    print(video_chooser.get_videos_with_moments())