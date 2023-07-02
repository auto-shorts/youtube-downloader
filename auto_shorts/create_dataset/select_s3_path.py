import pandas as pd
from sqlalchemy import text

from auto_shorts.upload.db.utils import postgres_engine


def _check_downloaded_data(df: pd.DataFrame) -> None:
    if "s3_path" not in df.columns:
        raise ValueError(
            "Wrong query! Result need to contain 's3_path' column"
        )

    if len(df) == 0:
        raise ValueError("Wrong query! Not data found.")


def download_s3_paths(query: str) -> list:
    query = text(query)
    with postgres_engine.connect() as connection:
        df = pd.read_sql(query, con=connection)

    _check_downloaded_data(df)
    return df.s3_path.values.tolist()
