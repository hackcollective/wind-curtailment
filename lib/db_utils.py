import logging
import sqlite3
from typing import Tuple

import pandas as pd
from sqlalchemy import create_engine

from lib.constants import SQL_DIR, BASE_DIR

logger = logging.getLogger(__name__)


def drop_and_initialize_tables(path_to_db):
    """Init the tables of our DB, setting primary keys.
    Need to do this up front with SQLite, cannot ALTER to add primary keys later.
    """
    logger.info(f"drop and initialize BOA tables {path_to_db} ")
    connection = sqlite3.connect(path_to_db)

    with open(SQL_DIR / "init_boal_db.sql") as f:
        query = f.read()

    with connection:
        connection.executescript(query)
        connection.commit()


def drop_and_initialize_bod_table(path_to_db):
    """Init the tables of our DB, setting primary keys.
    Need to do this up front with SQLite, cannot ALTER to add primary keys later.
    """

    logger.info(f"drop and initialize BOD tables {path_to_db} ")
    connection = sqlite3.connect(path_to_db)

    with open(SQL_DIR / "add_bod.sql") as f:
        query = f.read()

    with connection:
        connection.executescript(query)
        connection.commit()


class DbRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{self.db_path}", echo=False)

    def _get_query(self, table_name: str, start_time: str, end_time: str):
        """
        Cannot set table name as an SQL param:https://stackoverflow.com/questions/46736633/syntax-error-with-python3-and-sqlite3-when-using-parameters
        """
        start_time = pd.to_datetime(start_time) - pd.Timedelta(seconds=1)
        start_time = str(start_time)

        end_time = pd.to_datetime(end_time) + pd.Timedelta(seconds=1)
        end_time = str(end_time)

        return (
            f"select * from {table_name} "
            f" where local_datetime < '{end_time}' "
            f" and local_datetime >= '{start_time}' "
        )

    def get_data_for_time_range(self, start_time: str, end_time: str) -> Tuple[pd.DataFrame, pd.DataFrame]:

        logger.debug(f"{start_time=}")
        logger.debug(f"{end_time=}")

        # load 30 minutes more
        start_time = pd.to_datetime(start_time) - pd.Timedelta(minutes=30)

        with self.engine.connect() as conn:
            logger.debug(f"Getting FPNs from {start_time} to {end_time}")
            df_fpn = pd.read_sql(
                self._get_query("fpn", start_time, end_time),
                conn,
                index_col="unit",
                parse_dates=["timeFrom", "timeTo"],
            )
            logger.debug(f"Getting BOAs from {start_time} to {end_time}")
            df_boal = pd.read_sql(
                self._get_query("boal", start_time, end_time),
                conn,
                index_col="unit",
                parse_dates=["timeFrom", "timeTo"],
            )
            logger.debug(f"Getting BODs from {start_time} to {end_time}")
            df_bod = pd.read_sql(
                self._get_query("bod", start_time, end_time),
                conn,
                index_col="bmUnitID",
                parse_dates=["timeFrom", "timeTo"],
            )

            logger.info(f"Found {len(df_fpn)} FPNs")
            logger.info(f"Found {len(df_boal)} BOAs")
            logger.info(f"Found {len(df_bod)} BODs")

        return df_fpn, df_boal, df_bod


if __name__ == "__main__":
    db = DbRepository(BASE_DIR / "scripts/phys_data.db")
    df_fpn, df_boal = db.get_data_for_time_range(start_time="2022-03-19", end_time="2022-03-20")
