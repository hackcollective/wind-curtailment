import concurrent.futures
import logging
import multiprocessing
import os
from pathlib import Path

import pandas as pd
from ElexonDataPortal import api

API_KEY = "xutthojn7xa28q6"

client = api.Client(API_KEY)
MINUTES_TO_HOURS = 1 / 60
N_POOL_INSTANCES = 20

logger = logging.getLogger(__name__)
multiprocessing.log_to_stderr()


def call_sbp_api(start_date: str, end_date: str):
    return client.get_DERSYSDATA(start_date=start_date, end_date=end_date)


def call_api(start_date, end_date, unit):
    """Thin wrapper to allow kwarg passing with starmap"""
    logger.info(f"Calling BOAS API for {unit}")
    return client.get_PHYBMDATA(start_date=start_date, end_date=end_date, BMUnitId=unit)


def call_api_bod(start_date, end_date, unit):
    """Thin wrapper to allow kwarg passing with starmap"""
    logger.info(f"Calling BOD API for {unit}")
    return client.get_BOD(start_date=start_date, end_date=end_date, BMUnitId=unit)


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

                tasks = [executor.submit(call_api, start_date, end_date, unit) for unit in unit_ids]

                for future in concurrent.futures.as_completed(tasks):
                    data = future.result()
                    unit_dfs.append(data)

        else:
            unit_dfs = []
            for i, unit in enumerate(unit_ids):
                logger.info(f"Calling API PHYBMDATA for {unit} ({i}/{len(unit_ids)}) " f"{start_date=} {end_date=}")
                unit_dfs.append(call_api(start_date, end_date, unit))

        df = pd.concat(unit_dfs)
    else:
        df = client.get_PHYBMDATA(start_date=start_date, end_date=end_date)
        if unit_ids is not None:
            df = df[df["bmUnitID"].isin(unit_ids)]

    if cache:
        df.reset_index(drop=True).to_feather(file_name)

    return df


def fetch_bod_data(
    start_date, end_date, save_dir: Path, cache=True, unit_ids=None, multiprocess=False, pull_data_once: bool = False
):
    """From a brief visual inspection, this returns data that looks the same as the stuff I downloaded manually"""

    if cache:
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
        df = client.get_BOD(start_date=start_date, end_date=end_date)
        if unit_ids is not None:
            df = df[df["bmUnitID"].isin(unit_ids)]

    if cache:
        df.reset_index(drop=True).to_feather(file_name)

    return df


def format_physical_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"timeFrom": "From Time", "timeTo": "To Time", "bmUnitID": "Unit"})

    df["From Time"], df["To Time"] = df["From Time"].apply(pd.to_datetime), df["To Time"].apply(pd.to_datetime)
    return df


def add_bm_unit_type(df: pd.DataFrame, df_bm_units: pd.DataFrame, index_name: str = "Unit") -> pd.DataFrame:
    df = (
        df.set_index(index_name)
        .join(df_bm_units.set_index("SETT_BMU_ID")["FUEL TYPE"])
        .rename(columns={"FUEL TYPE": "Fuel Type"})
    )
    df["Fuel Type"].fillna("Battery?", inplace=True)
    return df.dropna(axis=1, how="all")


def parse_fpn_from_physical_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["recordType"] == "PN"]
    df.rename(columns={f"pnLevel{x}": f"level{x}" for x in ["From", "To"]}, inplace=True)
    return df.dropna(axis=1, how="all")


def parse_boal_from_physical_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["recordType"] == "BOALF"]
    df.rename(
        columns={f"bidOfferLevel{x}": f"level{x}" for x in ["From", "To"]}
        | {"bidOfferAcceptanceNumber": "Accept ID", "acceptanceTime": "Accept Time"},
        inplace=True,
    )
    return df.dropna(axis=1, how="all")