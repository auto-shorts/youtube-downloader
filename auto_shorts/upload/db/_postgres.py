import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import CursorResult

from auto_shorts.download.models.category import VideoCategory
from auto_shorts.download.models.channel import ChannelInfo
from auto_shorts.download.models.video_info import (
    VideoData,
    VideoDataWithStats,
)
from auto_shorts.upload.db.utils import postgres_engine


def upload_channel_info(channel_info: ChannelInfo) -> CursorResult:
    query = text(
        """
        INSERT INTO autoshorts.channels (
            channel_id, 
            title, 
            description, 
            custom_url, 
            views, 
            subscribers, 
            updated_at, 
            created_at
        )
        VALUES
            (
                :channel_id,
                :title,
                :description,
                :custom_url,
                :views,
                :subscribers,
                null,
                NOW()
            ) 
        ON DUPLICATE KEY UPDATE
            channel_id=:channel_id,
            title=:title,
            description=:description,
            custom_url=:custom_url,
            views=:views,
            subscribers=:subscribers,
            updated_at = NOW();
    """
    )

    parameters = {
        "channel_id": channel_info.channel_id,
        "title": channel_info.title,
        "description": channel_info.description,
        "custom_url": channel_info.custom_url,
        "views": channel_info.views,
        "subscribers": channel_info.subscribers,
    }

    with postgres_engine.connect() as connection:
        response = connection.execute(query, parameters)
        connection.commit()

    return response


def upload_categories(
    video_categories: list[VideoCategory],
    table_name: str = "categories",
    schema: str = "autoshorts",
) -> None:
    data = pd.DataFrame(
        [video_category.dict() for video_category in video_categories]
    )
    with postgres_engine as connection:
        data.to_sql(
            name=table_name,
            con=connection,
            if_exists="append",
            index=False,
            schema=schema,
        )
        connection.commit()


def is_channel_present(channel_id: str) -> bool:
    query = text(
        """
        SELECT
            channel_id
        FROM
            autoshorts.channels
        WHERE
            channel_id = :channel_id; 
        """
    )

    with postgres_engine.connect() as connection:
        response = connection.execute(
            query, {"channel_id": channel_id}
        ).fetchall()

    return len(response) > 0


def is_video_present(video_id: str) -> bool:
    query = text(
        """
        SELECT
            id
        FROM
            autoshorts.videos
        WHERE
            id = :video_id;   
        """
    )

    with postgres_engine.connect() as connection:
        response = connection.execute(query, {"video_id": video_id}).fetchall()

    return len(response) > 0


def upload_video_info_to_db(
    video_data: VideoDataWithStats, s3_path: str
) -> CursorResult:
    tags_joined = ",".join(video_data.tags) if video_data.tags else None

    query = text(
        """
        INSERT INTO autoshorts.videos (
            id, 
            audio_language,
            licensed, 
            description, 
            published_at, 
            tags, 
            title, 
            category_id,
            channel_id,
            s3_path,
            comments,
            likes,
            views,
            created_at
        )
        VALUES
            (
                :vid_id,
                :audio_lang,
                :licensed_val,
                :desc,
                STR_TO_DATE(:pub_at, '%Y-%m-%dT%H:%i:%sZ'),
                :tags_val,
                :title_val,
                :cat_id,
                :chan_id,
                :s3,
                :comments_val,
                :likes_val,
                :views_val,
                NOW()
            ) 
        ON DUPLICATE KEY UPDATE
        id = VALUES(id);
    """
    )

    parameters = {
        "vid_id": video_data.id,
        "audio_lang": video_data.audio_language,
        "licensed_val": video_data.licensed,
        "desc": video_data.description,
        "pub_at": video_data.published_at,
        "tags_val": tags_joined,
        "title_val": video_data.title,
        "cat_id": video_data.category_id,
        "chan_id": video_data.channel_id,
        "s3": s3_path,
        "comments_val": video_data.statistics.comments,
        "likes_val": video_data.statistics.likes,
        "views_val": video_data.statistics.views,
    }

    with postgres_engine.connect() as connection:
        response = connection.execute(query, parameters)
        connection.commit()

    return response


if __name__ == "__main__":
    print(is_video_present("8kggT2dasej-lo"))
