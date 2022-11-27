import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine

from lib.curtailment import analyze_curtailment
from lib.data.fetch_boa_data import run_boa
from lib.data.fetch_bod_data import run_bod
from lib.db_utils import drop_and_initialize_tables, drop_and_initialize_bod_table, DbRepository
from lib.gcp_db_utils import load_data, write_data
from scripts.fetch_data_and_calculate_curtailment import df_bm_units

logger = logging.getLogger(__name__)


def fetch_and_load_data(start: Optional[str] = None, end: Optional[str] = None):

    # get yesterdays date
    if (start is None) or (end is None):
        now = datetime.now(tz=timezone.utc)
        today = now.date()
        yesterday = today - timedelta(days=1)

    start = pd.Timestamp(yesterday)
    end = pd.Timestamp(today)

    wind_units = df_bm_units[df_bm_units["FUEL TYPE"] == "WIND"]["SETT_BMU_ID"].unique()

    # make new SQL database
    db_url = f"phys_data_{yesterday}_{today}.db"

    # initialize database
    drop_and_initialize_tables(db_url)
    drop_and_initialize_bod_table(db_url)

    engine = create_engine(f"sqlite:///{db_url}", echo=False)

    logger.info("Fetching data from ELEXON")

    # get BOAs and BODs
    run_boa(
        start_date=start,
        end_date=end,
        units=wind_units,
        chunk_size_in_days=1,
        database_engine=engine,
    )
    run_bod(
        start_date=start,
        end_date=end,
        units=wind_units,
        chunk_size_in_days=1,
        database_engine=engine,
    )

    logger.info("Running analysis")
    db = DbRepository(db_url)
    df = analyze_curtailment(db, str(start), str(end))

    df.to_csv(f"./data/outputs/results-{start}-{end}.csv")

    # load csv and save to database
    df = load_data(f"./data/outputs/results-{start}-{end}.csv")

    logger.info("Pushing to postgres")
    write_data(df=df)
