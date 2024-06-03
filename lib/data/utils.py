import logging
import multiprocessing

import pandas as pd

MINUTES_TO_HOURS = 1 / 60
N_POOL_INSTANCES = 20

logger = logging.getLogger(__name__)
multiprocessing.log_to_stderr()


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


def add_utc_timezone(datetime):
    """ Add utc timezone to datetime. """
    if datetime.tzinfo is None:
        datetime = datetime.tz_localize('UTC')
    else:
        datetime = datetime.tz_convert('UTC')
    return datetime
