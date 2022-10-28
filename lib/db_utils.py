import sqlite3
from typing import Tuple

import pandas as pd
from sqlalchemy import create_engine

from lib.constants import SQL_DIR, BASE_DIR


def drop_and_initialize_tables(path_to_db):
    """Init the tables of our DB, setting primary keys.
    Need to do this up front with SQLite, cannot ALTER to add primary keys later.
    """

    connection = sqlite3.connect(path_to_db)

    with open(SQL_DIR / "init.sql") as f:
        query = f.read()

    with connection:
        connection.executescript(query)
        connection.commit()


def drop_and_initialize_bod_table(path_to_db):
    """Init the tables of our DB, setting primary keys.
    Need to do this up front with SQLite, cannot ALTER to add primary keys later.
    """

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

    def get_data_for_time_range(
        self, start_time: str, end_time: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # Cannot set table name as an SQL param:https://stackoverflow.com/questions/46736633/syntax-error-with-python3-and-sqlite3-when-using-parameters
        raw_query = lambda x: f"select * from {x} where timeFrom BETWEEN ? AND ?"

        with self.engine.connect() as conn:
            df_fpn = pd.read_sql(
                raw_query("fpn"),
                conn,
                params=(start_time, end_time),
                index_col="unit",
                parse_dates=["timeFrom", "timeTo"],
            )
            df_boal = pd.read_sql(
                raw_query("boal"),
                conn,
                params=(start_time, end_time),
                index_col="unit",
                parse_dates=["timeFrom", "timeTo"],
            )

        return df_fpn, df_boal


if __name__ == "__main__":
    db = DbRepository(BASE_DIR / "scripts/phys_data.db")
    df_fpn, df_boal = db.get_data_for_time_range(
        start_time="2022-03-19", end_time="2022-03-20"
    )
