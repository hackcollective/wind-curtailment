from sqlalchemy import create_engine

from lib.data.fetch_bod_data import run_bod
from lib.db_utils import drop_and_initialize_tables, drop_and_initialize_bod_table

import pandas as pd


def test_fetch_bod_data():

    # make new SQL database
    db_url = f"test.db"
    engine = create_engine(f"sqlite:///{db_url}", echo=False)

    # initialize database
    drop_and_initialize_bod_table(db_url)

    start = pd.Timestamp("2024-04-11 04:00")
    end = pd.Timestamp("2024-04-11 05:00")
    run_bod(
        start_date=start,
        end_date=end,
        units=None,
        database_engine=engine,
        chunk_size_in_days=1 / 24,
        cache=False,
    )

    with engine.connect() as conn:
        df_bod = pd.read_sql(
            "select * from bod",
            conn,
            parse_dates=["timeFrom", "timeTo"],
        )

    assert len(df_bod) == 1404
    # assert (df_bod['local_datetime'] <= end).all()
    # assert (df_bod['local_datetime'] >= start).all()
