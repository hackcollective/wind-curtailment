import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine

from lib.constants import df_bm_units
from lib.curtailment import analyze_curtailment
from lib.data.fetch_boa_data import run_boa
from lib.data.fetch_bod_data import run_bod
from lib.db_utils import drop_and_initialize_tables, drop_and_initialize_bod_table, DbRepository
from lib.gcp_db_utils import load_data, write_data

logger = logging.getLogger(__name__)


def fetch_and_load_data(
    start: Optional[str] = None,
    end: Optional[str] = None,
    chunk_size_minutes: int = 60*24,
    multiprocess: bool = True,
):
    """
    Entrypoint for the scheduled data refresh. Fetches data from Elexon and pushes
    to the postgres instance.

    Writes a CSV as intermediate step
    """

    # get yesterdays date
    if (start is None) or (end is None):
        now = datetime.now(tz=timezone.utc)
        end = now.date()
        start = end - timedelta(days=1)

    start = pd.Timestamp(start)
    end = pd.Timestamp(end)

    wind_units = df_bm_units[df_bm_units["FUEL TYPE"] == "WIND"]["SETT_BMU_ID"].unique()

    # make new SQL database
    db_url = f"phys_data_{start}_{end}.db"

    # initialize database
    drop_and_initialize_tables(db_url)
    drop_and_initialize_bod_table(db_url)

    engine = create_engine(f"sqlite:///{db_url}", echo=False)

    logger.info("Fetching data from ELEXON")

    start_chunk = start
    end_chunk = start_chunk + pd.Timedelta(f"{chunk_size_minutes}T")
    # loop over 30 minutes chunks of data
    while end_chunk <= end:

        end_chunk = start_chunk + pd.Timedelta(f"{chunk_size_minutes}T")

        # get BOAs and BODs
        run_boa(
            start_date=start_chunk,
            end_date=end_chunk,
            units=wind_units,
            chunk_size_in_days=chunk_size_minutes / 24 / 60,
            database_engine=engine,
            cache=False,
            multiprocess=multiprocess,
        )
        run_bod(
            start_date=start_chunk,
            end_date=end_chunk,
            units=wind_units,
            chunk_size_in_days=chunk_size_minutes / 24 / 60,
            database_engine=engine,
            cache=False,
            multiprocess=multiprocess
        )

        logger.info("Running analysis")
        db = DbRepository(db_url)
        df = analyze_curtailment(db, str(start_chunk), str(end_chunk))

        df.to_csv(f"./data/outputs/results-{start_chunk}-{end_chunk}.csv")

        # load csv and save to database
        df = load_data(f"./data/outputs/results-{start_chunk}-{end_chunk}.csv")

        logger.info(f"Pushing to postgres, {len(df)} rows")
        try:
            write_data(df=df)
        except Exception as e:
            logger.warning("Writing the df failed")
            raise e

        # bump up the start_chunk by 30 minutes
        start_chunk = start_chunk + pd.Timedelta(f"{chunk_size_minutes}T")
