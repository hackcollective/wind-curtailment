import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from lib import constants

logger = logging.getLogger(__name__)


def get_db_connection():
    """If running locally, access via IP

    If deployed, then use the unix socket approach described here:
    https://cloud.google.com/sql/docs/mysql/connect-run#connect_to
    """

    if cloud_sql_instance := os.environ.get("CLOUD_SQL_INSTANCE"):
        address = f"postgresql+psycopg2://{constants.DB_USERNAME}:{constants.DB_PASSWORD}@{constants.DB_NAME}?host={constants.HOST}:{cloud_sql_instance}"
    else:
        address = f"postgresql+psycopg2://{constants.DB_USERNAME}:{constants.DB_PASSWORD}@{constants.DB_IP}:5432"
    return create_engine(url=address)


def write_curtailment_data(df: pd.DataFrame):

    if len(df) == 0:
        logger.debug("There was not data to write to the database")
    else:
        engine = get_db_connection()
        if "local_datetime" in df.columns:
            df = df.rename(columns={"local_datetime": "time"})

        logger.info(f"Adding curtailment to database ({len(df)}")

        with engine.connect() as conn:
            df.to_sql("curtailment", conn, if_exists="append", index=False)


def write_sbp_data(df: pd.DataFrame):
    if len(df) == 0:
        logger.debug("There was not data to write to the database")
    else:
        engine = get_db_connection()
        logger.info(f"Adding sbp to database ({len(df)}")

        with engine.connect() as conn:
            df.to_sql("sbp", conn, if_exists="append", index=False)


def read_data(start_time="2022-01-01", end_time="2023-01-01"):
    engine = get_db_connection()

    # TODO merge in the SBP data here, to avoid doing a migration
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

    columns = ["time", "level_fpn", "level_boal", "level_after_boal", "delta_mw", "cost_gbp"]

    if len(df) == 0:
        logger.debug("No data to load")
        return pd.DataFrame(columns=columns)

    df = df.rename(
        columns={
            "Time": "time",
            "Level_FPN": "level_fpn",
            "Level_BOAL": "level_boal",
            "Level_After_BOAL": "level_after_boal",
            "delta": "delta_mw",
        }
    )
    return df[columns]
