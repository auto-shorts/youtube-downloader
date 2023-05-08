from sqlalchemy import create_engine

from auto_shorts.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_USERNAME,
)

POSTGRES_URL = f"postgresql+psycopg2://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"
postgres_engine = create_engine(POSTGRES_URL)
