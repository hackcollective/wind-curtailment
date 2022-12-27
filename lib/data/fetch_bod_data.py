import logging
import time

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from lib.constants import SAVE_DIR, DATA_DIR
from lib.data.utils import (
    fetch_bod_data,
    add_bm_unit_type,
)

df_bm_units = pd.read_excel(DATA_DIR / "BMUFuelType.xls", header=0)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_RETRIES = 1


def run_bod(
    start_date,
    end_date,
    units,
    chunk_size_in_days=7,
    database_engine=None,
    cache=True,
    multiprocess=True,
    pull_data_once=False,
):
    """
    Collects data from the ElexonAPI, saved as a local feather file, does some preprocessing and then places in
    an SQLite DB.

    Only collects data for specified units, to keep things fast. Uses multiprocessing to grab all units in parallel.
    """

    if database_engine is None:
        database_engine = create_engine("sqlite:///phys_data.db", echo=False)

    interval = pd.Timedelta(days=chunk_size_in_days)

    chunk_start = start_date
    chunk_end = start_date + interval

    while chunk_end <= end_date:
        logger.info(f"{chunk_start} to {chunk_end}")
        t1 = time.time()
        fetch_and_load_one_chunk(
            start_date=str(chunk_start),
            end_date=str(chunk_end),
            unit_ids=units,
            database_engine=database_engine,
            cache=cache,
            multiprocess=multiprocess,
            pull_data_once=pull_data_once,
        )
        t2 = time.time()
        logger.info(f"{(t2 - t1) / 60} minutes for {interval}")
        chunk_start = chunk_end
        chunk_end += interval


def write_bod_to_db(df_fpn, database_engine) -> bool:
    """Write the BOD df to DB"""

    logger.info("Writing BODs to db")

    try:
        with database_engine.connect() as connection:
            logger.debug(f"Writing {len(df_fpn)} to database")
            df_fpn.to_sql("bod", connection, if_exists="append", index_label="bmUnitID")
            logger.debug(f"Writing to database: Done")
        return True
    except OperationalError as e:
        logger.warning(e)
        return False


def fetch_and_load_one_chunk(
    start_date,
    end_date,
    unit_ids,
    database_engine,
    cache=True,
    multiprocess=True,
    pull_data_once=False,
):
    """Fetch and load FPN and BOAL data for `start_date` to `end_date` for units in `unit_ids"""
    # TODO clean up the preprocessing of data here
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    logger.info(f"{start_date}-{end_date}")

    df = fetch_bod_data(
        start_date=start_date,
        end_date=end_date,
        save_dir=SAVE_DIR,
        cache=cache,
        unit_ids=unit_ids,
        multiprocess=multiprocess,
        pull_data_once=pull_data_once,
    )

    df["timeFrom"], df["timeTo"] = df["timeFrom"].apply(pd.to_datetime), df["timeTo"].apply(
        pd.to_datetime
    )

    df = add_bm_unit_type(df, df_bm_units=df_bm_units, index_name="bmUnitID")

    df = df[df["Fuel Type"] == "WIND"]
    df.drop(columns=["Fuel Type"], inplace=True)

    # Duplicates can occur from multiple SP's reporting the same BOAL
    # df_boal = df_boal.drop_duplicates(subset=["timeFrom", "timeTo", "Accept ID", "levelFrom", "levelTo"])

    # DB Locking collisions between processes necessitate a retry loop
    bod_success = write_bod_to_db(df, database_engine)
    retries = 0
    while not bod_success and retries < MAX_RETRIES:
        logger.info("Retrying FPN after sleep")
        time.sleep(np.random.randint(1, 20))
        bod_success = write_bod_to_db(df, database_engine)
