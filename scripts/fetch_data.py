# Getting the data via the Elexon API is slow
# Here we use multiprocessing and store the output in an SQLite DB for easy retrieval

import logging
import sqlite3
import time
import numpy as np

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, IntegrityError

from lib.constants import SAVE_DIR, DATA_DIR
from lib.data import (
    fetch_physical_data,
    add_bm_unit_type,
    parse_boal_from_physical_data,
    parse_fpn_from_physical_data,
)
import pandas as pd

from lib.db_utils import initialize_tables

engine = create_engine("sqlite:///phys_data.db", echo=False)

df_bm_units = pd.read_excel(DATA_DIR / "BMUFuelType.xls", header=0)

logging.basicConfig(level=logging.INFO)

MAX_RETRIES = 1


def write_fpn_to_db(df_fpn) -> bool:
    try:
        with engine.connect() as connection:
            df_fpn.to_sql("fpn", connection, if_exists="append", index_label="unit")
        return True
    except OperationalError:
        return False


def write_boal_to_db(df_boal) -> bool:
    try:
        with engine.connect() as connection:
            # Potential issue here with duplicate BOALs nuking the whole write. This can happen because
            # BOALs are extended across SPs
            try:
                df_boal.to_sql("boal", connection, if_exists="append", index_label="unit")
            except (sqlite3.IntegrityError, IntegrityError) as e:
                logging.warning(e)
                # Try and write them one at at time
                for i in range(len(df_boal)):
                    try:
                        df_boal.iloc[i].to_sql("boal", con=connection, if_exists="append", index_label="unit")
                    except IntegrityError as e:
                        logging.warning(e)
                        pass
        return True
    except OperationalError:
        return False


def run_one_sp(start_date, end_date, unit_ids):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    logger.info(f"{start_date}-{end_date}")

    df = fetch_physical_data(start_date=start_date, end_date=end_date, save_dir=SAVE_DIR, cache=True,
                             unit_ids=unit_ids)
    # df = format_physical_data(df)
    df = df.rename(columns={"bmUnitID": "Unit"})
    df["timeFrom"], df["timeTo"] = df["timeFrom"].apply(pd.to_datetime), df["timeTo"].apply(pd.to_datetime)

    df = add_bm_unit_type(df, df_bm_units=df_bm_units)

    df_fpn, df_boal = parse_fpn_from_physical_data(df), parse_boal_from_physical_data(df)

    df_boal = df_boal[df_boal["Fuel Type"] == "WIND"]
    df_fpn = df_fpn[df_fpn["Fuel Type"] == "WIND"]

    # Duplicates can occur from multiple SP's reporting the same BOAL
    df_boal = df_boal.drop_duplicates(subset=['timeFrom', 'timeTo', 'Accept ID', 'levelFrom', 'levelTo'])

    # DB Locking collisions between processes necessitate a retry loop
    fpn_success = write_fpn_to_db(df_fpn)
    retries = 0
    while not fpn_success and retries < MAX_RETRIES:
        logger.info("Retrying FPN after sleep")
        time.sleep(np.random.randint(1, 20))
        fpn_success = write_fpn_to_db(df_fpn)

    # Separated these because pandas autocommits, so FPN could end up being retried unecessarily
    # if subsequent BOAL write has failed!
    boal_success = write_boal_to_db(df_boal)
    retries = 0
    while not boal_success and retries < MAX_RETRIES:
        logger.info("Retrying BOAL after sleep")
        time.sleep(np.random.randint(1, 20))
        boal_success = write_boal_to_db(df_boal)
        retries +=1

if __name__ == "__main__":

    logger = logging.getLogger(__file__)

    initialize_tables("phys_data.db")
    wind_units = df_bm_units[df_bm_units["FUEL TYPE"] == "WIND"]['SETT_BMU_ID'].unique()
    # march = [(f"2022-03-{day+1:0>2d} 00:00", f"2022-03-{day+2:0>2d} 00:00", wind_units) for day in range(30)]
    # march.append((f"2022-03-31 00:00", f"2022-04-01 00:00", wind_units))

    interval = pd.Timedelta(days=7)
    start = pd.Timestamp("2022-01-01 00:00")
    end = start + interval

    while end.month < 8:
        logger.info(f"{start} to {end}")
        t1 = time.time()
        run_one_sp(start_date=str(start), end_date=str(end),
                   unit_ids=wind_units)
        t2 = time.time()
        logger.info(f"{(t2-t1)/60} minutes for {interval}")
        start = end
        end += interval
