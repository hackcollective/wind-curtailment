import concurrent.futures
import logging
import os
import time
from pathlib import Path
import requests

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from lib.constants import SAVE_DIR, df_bm_units
from lib.data.utils import (
    add_bm_unit_type, logger, N_POOL_INSTANCES,
)

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
        bod_success = write_bod_to_db(df,database_engine)


def call_api_bod(start_date, end_date, unit = None):
    """Thin wrapper to allow kwarg passing with starmap"""
    logger.info(f"Calling BOD API for {unit}")

    # "https://data.elexon.co.uk/bmrs/api/v1/datasets/BOD?from=2024-03-01&to=2024-03-01&bmUnit=T_ACHRW-1&format=json"
    datetimes = pd.date_range(start_date, end_date, freq="30min")
    data_df = []
    for datetime in datetimes:
        logger.info(f"Getting BOD from {datetime}")

        datetime_no_timezone = datetime.tz_localize(None)

        url = f"https://data.elexon.co.uk/bmrs/api/v1/datasets/BOD?from={datetime_no_timezone}&to={datetime_no_timezone}"
        if unit is not None:
            url = url+f"&bmUnit={unit}"
        url = url+"&format=json"

        r = requests.get(url)

        data_one_settlement_period_df = pd.DataFrame(r.json()["data"])
        data_df.append(data_one_settlement_period_df)

    data_df = pd.concat(data_df)

    # rename bmUnit to bmUnitID
    data_df.rename(columns={"bmUnit": "bmUnitID"}, inplace=True)

    # drop dataset column
    data_df.drop(columns=["nationalGridBmUnit"], inplace=True)

    # rename LevelFrom to bidOfferLevelFrom
    data_df.rename(columns={"levelFrom": "bidOfferLevelFrom"}, inplace=True)
    data_df.rename(columns={"levelTo": "bidOfferLevelTo"}, inplace=True)
    data_df.rename(columns={"pairId": "bidOfferPairNumber"}, inplace=True)
    data_df.rename(columns={"offer": "offerPrice"}, inplace=True)
    data_df.rename(columns={"bid": "bidPrice"}, inplace=True)
    data_df.rename(columns={"dataset": "recordType"}, inplace=True)

    data_df['local_datetime'] = pd.to_datetime(data_df['timeFrom'])
    # remove anything after end_date
    data_df = data_df[data_df['local_datetime'] < end_date]

    return data_df


def fetch_bod_data(
    start_date, end_date, save_dir: Path, cache=True, unit_ids=None, multiprocess=False, pull_data_once: bool = False
):
    """From a brief visual inspection, this returns data that looks the same as the stuff I downloaded manually"""

    if cache:
        logger.info('Loading BOD data from cache')
        file_name = save_dir / f"BOD_{start_date}-{end_date}.feather"
        if file_name.exists():
            return pd.read_feather(file_name)

    if (unit_ids is not None) and (not pull_data_once):
        if multiprocess:

            unit_dfs = []
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=int(os.getenv("N_POOL_INSTANCES", N_POOL_INSTANCES))
            ) as executor:

                tasks = [executor.submit(call_api_bod, start_date, end_date, unit) for unit in unit_ids]

                for future in concurrent.futures.as_completed(tasks):
                    data = future.result()
                    unit_dfs.append(data)

        else:
            unit_dfs = []
            for i, unit in enumerate(unit_ids):
                logger.info(f"Calling API BOD for {unit} ({i}/{len(unit_ids)}) " f"{start_date=} {end_date=}")
                unit_dfs.append(call_api_bod(start_date, end_date, unit))
        df = pd.concat(unit_dfs)
    else:

        df = call_api_bod(start_date=start_date, end_date=end_date)
        if unit_ids is not None:
            df = df[df["bmUnitID"].isin(unit_ids)]

    if cache:
        df.reset_index(drop=True).to_feather(file_name)

    return df
