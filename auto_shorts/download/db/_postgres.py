from sqlalchemy import text
from sqlalchemy.engine import CursorResult

from auto_shorts.upload.db.utils import postgres_engine


def get_video_ids_and_buckets_not_in_list(
    video_id_list: list[str],
) -> CursorResult:
    if not video_id_list:
        # jak lista jest pusta to juz nie jest xdd
        video_id_list = ["xdd"]
    ids_str = ", ".join(f"'{vid_id}'" for vid_id in video_id_list)
    query = f"""
        SELECT
            s3_path
        FROM
            autoshorts.videos
        WHERE
            id NOT IN ({ids_str})
    """
    with postgres_engine.connect() as connection:
        response = connection.execute(text(query)).fetchall()

    return response
