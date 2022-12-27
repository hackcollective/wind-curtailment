import concurrent.futures
import logging
import os
import sqlite3
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, IntegrityError

from lib.constants import SAVE_DIR, DATA_DIR
from lib.data.utils import (
    add_bm_unit_type,
    parse_boal_from_physical_data,
    parse_fpn_from_physical_data, logger, client, N_POOL_INSTANCES,
)

df_bm_units = pd.read_excel(DATA_DIR / "BMUFuelType.xls", header=0)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_RETRIES = 1


def run_boa(start_date, end_date, units, chunk_size_in_days=7, database_engine=None, cache=True, multiprocess=True,pull_data_once=False):
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
    logger.info(f"{chunk_start=} to {chunk_end=} ({end_date=})")
    while chunk_end <= end_date:
        logger.info(f"{chunk_start} to {chunk_end}")
        t1 = time.time()
        fetch_and_load_one_chunk(
            start_date=str(chunk_start), end_date=str(chunk_end), unit_ids=units,
            database_engine=database_engine, cache=cache, multiprocess=multiprocess,pull_data_once=pull_data_once
        )
        t2 = time.time()
        logger.info(f"{(t2 - t1) / 60} minutes for {interval}")
        chunk_start = chunk_end
        chunk_end += interval


def write_fpn_to_db(df_fpn,database_engine) -> bool:
    """Write the FPN df to DB"""

    logger.info(f'Writing {len(df_fpn)} to FPN database')

    try:
        with database_engine.connect() as connection:
            df_fpn.to_sql("fpn", connection, if_exists="append", index_label="unit")
        return True
    except OperationalError:
        return False


def write_boal_to_db(df_boal, database_engine) -> bool:
    """Write the BOAL df to DB, falling back to a row-by-row load if the load of the whole df fails.

    This can happen because at boundaries between SPs, the same BOAL can be reported in multiple SP's. For instance,
    if the BOAL is 00:40 -> 01.05, it will be reported in two SPs, and so we can end up trying to load the same
    BOAL twice.
    """

    logger.info(f'Writing {len(df_boal)} to BOA database')

    try:
        with database_engine.connect() as connection:
            # Potential issue here with duplicate BOALs nuking the whole write. This can happen because
            # BOALs are extended across SPs
            try:
                df_boal.to_sql("boal", connection, if_exists="append", index_label="unit")
            except (sqlite3.IntegrityError, IntegrityError) as e:
                logging.warning(e)
                # Try and write them one at at time
                for i in range(len(df_boal)):
                    try:
                        df_boal.iloc[i].to_sql(
                            "boal",
                            con=connection,
                            if_exists="append",
                            index_label="unit",
                        )
                    except IntegrityError as e:
                        logging.warning(e)
                        pass
        return True
    except OperationalError:
        return False


def fetch_and_load_one_chunk(start_date, end_date, unit_ids, database_engine, cache=True, multiprocess=True,pull_data_once=False):
    """Fetch and load FPN and BOAL data for `start_date` to `end_date` for units in `unit_ids"""
    # TODO clean up the preprocessing of data here
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    logger.info(f"{start_date}-{end_date}")

    df = fetch_physical_data(
        start_date=start_date,
        end_date=end_date,
        save_dir=SAVE_DIR,
        cache=cache,
        unit_ids=unit_ids,
        multiprocess=multiprocess,
        pull_data_once=pull_data_once
    )

    df = df.rename(columns={"bmUnitID": "Unit"})
    df["timeFrom"], df["timeTo"] = df["timeFrom"].apply(pd.to_datetime), df["timeTo"].apply(
        pd.to_datetime
    )

    df = add_bm_unit_type(df, df_bm_units=df_bm_units)

    df_fpn, df_boal = parse_fpn_from_physical_data(df), parse_boal_from_physical_data(df)

    logger.debug(f'there are {len(df_fpn)} FPNS')
    logger.debug(f'there are {len(df_boal)} BOAs')

    # Duplicates can occur from multiple SP's reporting the same BOAL
    df_boal = df_boal.drop_duplicates(
        subset=["timeFrom", "timeTo", "Accept ID", "levelFrom", "levelTo"]
    )

    # DB Locking collisions between processes necessitate a retry loop
    fpn_success = write_fpn_to_db(df_fpn,database_engine)
    retries = 0
    while not fpn_success and retries < MAX_RETRIES:
        logger.info("Retrying FPN after sleep")
        time.sleep(np.random.randint(1, 20))
        fpn_success = write_fpn_to_db(df_fpn)

    # Separated these because pandas autocommits, so FPN could end up being retried unecessarily
    # if subsequent BOAL write has failed!
    boal_success = write_boal_to_db(df_boal,database_engine)
    retries = 0
    while not boal_success and retries < MAX_RETRIES:
        logger.info("Retrying BOAL after sleep")
        time.sleep(np.random.randint(1, 20))
        boal_success = write_boal_to_db(df_boal,database_engine)
        retries += 1


def call_physbm_api(start_date, end_date, unit):
    """Thin wrapper to allow kwarg passing with starmap"""
    logger.info(f"Calling BOAS API for {unit}")
    return client.get_PHYBMDATA(start_date=start_date, end_date=end_date, BMUnitId=unit)


def fetch_physical_data(
    start_date, end_date, save_dir: Path, cache=True, unit_ids=None, multiprocess=False, pull_data_once: bool = False
):
    """From a brief visual inspection, this returns data that looks the same as the stuff I downloaded manually"""

    if cache:
        file_name = save_dir / f"{start_date}-{end_date}.fthr"
        if file_name.exists():
            return pd.read_feather(file_name)

    if (unit_ids is not None) and (not pull_data_once):
        if multiprocess:
            unit_dfs = []
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=int(os.getenv("N_POOL_INSTANCES", N_POOL_INSTANCES))
            ) as executor:

                tasks = [executor.submit(call_physbm_api, start_date, end_date, unit) for unit in unit_ids]

                for future in concurrent.futures.as_completed(tasks):
                    data = future.result()
                    unit_dfs.append(data)

        else:
            unit_dfs = []
            for i, unit in enumerate(unit_ids):
                logger.info(f"Calling API PHYBMDATA for {unit} ({i}/{len(unit_ids)}) " f"{start_date=} {end_date=}")
                unit_dfs.append(call_physbm_api(start_date, end_date, unit))

        df = pd.concat(unit_dfs)
    else:
        df = client.get_PHYBMDATA(start_date=start_date, end_date=end_date)
        if unit_ids is not None:
            df = df[df["bmUnitID"].isin(unit_ids)]

    if cache:
        df.reset_index(drop=True).to_feather(file_name)

    return df