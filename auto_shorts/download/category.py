import json
import os
from pathlib import Path

from auto_shorts.download.models.category import VideoCategory
from auto_shorts.download.video_info import InfoDownloaderBase
from auto_shorts.upload.db import upload_categories


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


if __name__ == "__main__":
    downloader_category = CategoryInfoDownloader()
    categories_info = downloader_category.video_categories_by_region("pl")
    upload_categories(categories_info)
