import logging
import sqlite3
import time

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, IntegrityError

from lib.constants import SAVE_DIR, DATA_DIR
from lib.data import (
    fetch_bod_data,
    add_bm_unit_type,
    parse_boal_from_physical_data,
    parse_fpn_from_physical_data,
)
from lib.db_utils import drop_and_initialize_bod_table

engine = create_engine("sqlite:///phys_data.db", echo=False)
df_bm_units = pd.read_excel(DATA_DIR / "BMUFuelType.xls", header=0)

logging.basicConfig(level=logging.INFO)

MAX_RETRIES = 1


def run(start_date, end_date, units, chunk_size_in_days=7):
    """
    Collects data from the ElexonAPI, saved as a local feather file, does some preprocessing and then places in
    an SQLite DB.

    Only collects data for specified units, to keep things fast. Uses multiprocessing to grab all units in parallel.
    """

    interval = pd.Timedelta(days=chunk_size_in_days)

    chunk_start = start_date
    chunk_end = start_date + interval

    while chunk_end < end_date:
        logger.info(f"{chunk_start} to {chunk_end}")
        t1 = time.time()
        fetch_and_load_one_chunk(start_date=str(chunk_start), end_date=str(chunk_end), unit_ids=units)
        t2 = time.time()
        logger.info(f"{(t2 - t1) / 60} minutes for {interval}")
        chunk_start = chunk_end
        chunk_end += interval


def write_bod_to_db(df_fpn) -> bool:
    """Write the BOD df to DB"""
    try:
        with engine.connect() as connection:
            df_fpn.to_sql("bod", connection, if_exists="append", index_label="bmUnitID")
        return True
    except OperationalError:
        return False


def fetch_and_load_one_chunk(start_date, end_date, unit_ids):
    """Fetch and load FPN and BOAL data for `start_date` to `end_date` for units in `unit_ids"""
    # TODO clean up the preprocessing of data here
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    logger.info(f"{start_date}-{end_date}")

    df = fetch_bod_data(
        start_date=start_date,
        end_date=end_date,
        save_dir=SAVE_DIR,
        cache=True,
        unit_ids=unit_ids,
    )

    df["timeFrom"], df["timeTo"] = df["timeFrom"].apply(pd.to_datetime), df["timeTo"].apply(pd.to_datetime)

    df = add_bm_unit_type(df, df_bm_units=df_bm_units)

    df = df[df["Fuel Type"] == "WIND"]

    # Duplicates can occur from multiple SP's reporting the same BOAL
    # df_boal = df_boal.drop_duplicates(subset=["timeFrom", "timeTo", "Accept ID", "levelFrom", "levelTo"])

    # DB Locking collisions between processes necessitate a retry loop
    fpn_success = write_bod_to_db(df)
    retries = 0
    while not fpn_success and retries < MAX_RETRIES:
        logger.info("Retrying FPN after sleep")
        time.sleep(np.random.randint(1, 20))
        fpn_success = write_bod_to_db(df)


if __name__ == "__main__":

    logger = logging.getLogger(__file__)

    drop_and_initialize_bod_table("phys_data.db")
    wind_units = df_bm_units[df_bm_units["FUEL TYPE"] == "WIND"]["SETT_BMU_ID"].unique()

    start = pd.Timestamp("2022-01-01 00:00")
    end = pd.Timestamp("2022-01-02 00:00")

    run(start_date=start, end_date=end, units=wind_units)
