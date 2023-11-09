from sqlalchemy import URL, create_engine

from auto_shorts.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_USERNAME,
)

postgres_engine = create_engine(
    URL.create(
        host=POSTGRES_HOST,
        drivername="mysql+mysqlconnector",
        username=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
        port=3306,
    ),
    pool_pre_ping=True,
)
