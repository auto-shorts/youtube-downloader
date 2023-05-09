import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import CursorResult

from auto_shorts.download.models.category import VideoCategory
from auto_shorts.download.models.channel import ChannelInfo
from auto_shorts.download.models.video_info import VideoData
from auto_shorts.upload.db.utils import postgres_engine


def upload_channel_info(channel_info: ChannelInfo) -> CursorResult:
    query = f"""
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
                '{channel_info.channel_id}',
                '{channel_info.title}',
                '{channel_info.description}',
                '{channel_info.custom_url}',
                {channel_info.views},
                {channel_info.subscribers},
                null,
                NOW()
            ) 
        ON CONFLICT (channel_id) 
        DO
        UPDATE
        SET channel_id='{channel_info.channel_id}',
            title='{channel_info.title}',
            description='{channel_info.description}',
            custom_url='{channel_info.custom_url}',
            views={channel_info.views},
            subscribers={channel_info.subscribers},
            updated_at = NOW();
    """

    with postgres_engine.connect() as connection:
        response = connection.execute(text(query))
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
    with postgres_engine.connect() as connection:
        data.to_sql(
            name=table_name,
            con=connection,
            if_exists="append",
            index=False,
            schema=schema,
        )
        connection.commit()


def is_channel_present(channel_id: str) -> bool:
    query = f"""
        SELECT
            channel_id
        FROM
            autoshorts.channels
        WHERE
            channel_id = '{channel_id}'   
    """
    with postgres_engine.connect() as connection:
        response = connection.execute(text(query)).fetchall()

    if len(response) == 0:
        return False

    return True


def is_video_present(video_id: str) -> bool:
    query = f"""
        SELECT
            channel_id
        FROM
            autoshorts.videos
        WHERE
            id = '{video_id}'   
    """
    with postgres_engine.connect() as connection:
        response = connection.execute(text(query)).fetchall()

    if len(response) == 0:
        return False

    return True


def upload_video_info_to_db(
    video_data: VideoData, s3_path: str
) -> CursorResult:
    licensed = (
        video_data.licensed if video_data.licensed is not None else "null"
    )
    query = f"""
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
            created_at
        )
        VALUES
            (
                '{video_data.id}',
                '{video_data.audio_language}',
                {licensed},
                '{video_data.description}',
                '{video_data.published_at}',
                '{video_data.tags}',
                '{video_data.title}',
                '{video_data.category_id}',
                '{video_data.channel_id}',
                '{s3_path}',
                NOW()
            ) 
        ON CONFLICT (id) 
        DO
        NOTHING;
    """

    with postgres_engine.connect() as connection:
        response = connection.execute(text(query))
        connection.commit()

    return response


if __name__ == "__main__":
    print(is_video_present("8kggT2dasej-lo"))
