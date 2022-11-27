import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from lib import constants

logger = logging.getLogger(__name__)


def get_db_connection():
    """If running locally, access via IP

    If deployed, then use the unix socket approach described here:
    https://cloud.google.com/sql/docs/mysql/connect-run#connect_to
    """
    if unix_socket_path := os.environ.get("INSTANCE_UNIX_SOCKET"):
        address = URL.create(
            drivername="postgresql+psycopg2",
            username=constants.DB_USERNAME,
            password=constants.DB_PASSWORD,
            database="postgres",
            query={"unix_sock": "{}/.s.PGSQL.5432".format(unix_socket_path)}
            # https://stackoverflow.com/questions/54967660/connect-to-a-database-over-a-unix-socket-using-sqlalchemy
        )
    else:
        address = f"postgresql+psycopg2://{constants.DB_USERNAME}:{constants.DB_PASSWORD}@{constants.DB_IP}:5432"
    return create_engine(url=address)


def write_data(df: pd.DataFrame):
    engine = get_db_connection()

    if "local_datetime" in df.columns:
        df = df.rename(columns={"local_datetime": "time"})

    logger.info(f"Adding curtailment to database ({len(df)}")

    with engine.connect() as conn:
        df.to_sql("curtailment", conn, if_exists="append", index=False)


def read_data(start_time="2022-01-01", end_time="2023-01-01"):
    engine = get_db_connection()

    raw_query = f"select * from curtailment " f"where time BETWEEN '{start_time}' AND '{end_time}'" f"order by time"

    with engine.connect() as conn:
        df_curtailment = pd.read_sql(
            raw_query,
            conn,
            parse_dates=["timeFrom", "timeTo"],
        )

    return df_curtailment


def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, index_col=0)
    df = df.rename(
        columns={
            "Time": "time",
            "Level_FPN": "level_fpn",
            "Level_BOAL": "level_boal",
            "Level_After_BOAL": "level_after_boal",
            "delta": "delta_mw",
        }
    )
    df["time"] = df["local_datetime"]
    return df[["time", "level_fpn", "level_boal", "level_after_boal", "delta_mw"]]
