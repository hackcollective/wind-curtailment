from sqlalchemy import create_engine

from lib.data.fetch_boa_data import run_boa
from lib.db_utils import drop_and_initialize_tables, drop_and_initialize_bod_table

import pandas as pd


def test_fetch_fpn_data():

    # make new SQL database
    db_url = f"test.db"
    engine = create_engine(f"sqlite:///{db_url}", echo=False)

    # initialize database
    drop_and_initialize_tables(db_url)

    start = pd.Timestamp("2024-04-11 04:00")
    end = pd.Timestamp("2024-04-11 05:00")
    run_boa(
        start_date=start,
        end_date=end,
        units=None,
        database_engine=engine,
        chunk_size_in_days=1 / 24,
        cache=False
    )

    with engine.connect() as conn:
        df_boa = pd.read_sql(
            "select * from boal",
            conn,
            parse_dates=["timeFrom", "timeTo"],
        )

        df_fpn = pd.read_sql(
            "select * from fpn",
            conn,
            parse_dates=["timeFrom", "timeTo"],
        )

    assert len(df_boa) == 265
    assert len(df_fpn) == 2985
