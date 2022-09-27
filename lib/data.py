from multiprocessing.pool import Pool
from pathlib import Path

import pandas as pd
from ElexonDataPortal import api

API_KEY = "xutthojn7xa28q6"

client = api.Client(API_KEY)
MINUTES_TO_HOURS = 1 / 60


def call_api(start_date, end_date, unit):
    """Thin wrapper to allow kwarg passing with starmap"""
    return client.get_PHYBMDATA(start_date=start_date, end_date=end_date, BMUnitId=unit)


def fetch_physical_data(
    start_date, end_date, save_dir: Path, cache=True, unit_ids=None
):
    """From a brief visual inspection, this returns data that looks the same as the stuff I downloaded manually"""

    file_name = save_dir / f"{start_date}-{end_date}.fthr"
    if file_name.exists():
        return pd.read_feather(file_name)

    if unit_ids is not None:
        kwargs = [(start_date, end_date, unit) for unit in unit_ids]
        with Pool(len(unit_ids)) as p:
            unit_dfs = p.starmap(call_api, kwargs)
        df = pd.concat(unit_dfs)
    else:
        df = client.get_PHYBMDATA(start_date=start_date, end_date=end_date)

    if cache:
        df.reset_index(drop=True).to_feather(file_name)

    return df


def format_physical_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={"timeFrom": "From Time", "timeTo": "To Time", "bmUnitID": "Unit"}
    )

    df["From Time"], df["To Time"] = df["From Time"].apply(pd.to_datetime), df[
        "To Time"
    ].apply(pd.to_datetime)
    return df


def add_bm_unit_type(df: pd.DataFrame, df_bm_units: pd.DataFrame) -> pd.DataFrame:
    df = (
        df.set_index("Unit")
        .join(df_bm_units.set_index("SETT_BMU_ID")["FUEL TYPE"])
        .rename(columns={"FUEL TYPE": "Fuel Type"})
    )
    df["Fuel Type"].fillna("Battery?", inplace=True)
    return df.dropna(axis=1, how="all")


def parse_fpn_from_physical_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["recordType"] == "PN"]
    df.rename(
        columns={f"pnLevel{x}": f"level{x}" for x in ["From", "To"]}, inplace=True
    )
    return df.dropna(axis=1, how="all")


def parse_boal_from_physical_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["recordType"] == "BOALF"]
    df.rename(
        columns={f"bidOfferLevel{x}": f"level{x}" for x in ["From", "To"]}
        | {"bidOfferAcceptanceNumber": "Accept ID", "acceptanceTime": "Accept Time"},
        inplace=True,
    )
    return df.dropna(axis=1, how="all")
