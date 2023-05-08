from auto_shorts.db.utils import postgres_engine
from auto_shorts.download.models.channel import ChannelInfo
from sqlalchemy import text


def get_all_tables():
    query = "SELECT * FROM pg_catalog.pg_tables;"
    with postgres_engine.connect() as connection:
        result = connection.execute(text(query))
        for r in result:
            print(r)


def upload_channel_info(channel_info: ChannelInfo) -> None:
    query = f"""
        INSERT INTO channels (
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
        ON CONFLICT (url) 
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
        result = connection.execute(text(query))


if __name__ == "__main__":
    get_all_tables()