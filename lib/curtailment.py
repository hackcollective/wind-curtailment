import logging
from typing import Optional

import pandas as pd

from lib.data.utils import MINUTES_TO_HOURS, add_utc_timezone
from lib.db_utils import DbRepository

logger = logging.getLogger(__name__)

MINUTES_TO_HOURS = 1 / 60


def resolve_applied_bid_offer_level(df_linear: pd.DataFrame):
    """
    We can have multiple levels for a given timepoint, because levels are fixed
    at one point and then overwitten at a later timepoint, before the moment in
    question has arrived.

    We need to resolve them, choosing the latest possible commitment for each timepoint.

    We need to upsample the data first to achieve this.
    """

    if len(df_linear) == 0:
        return df_linear

    out = []

    for accept_id, data in df_linear.groupby("Accept ID"):
        high_freq = data.reset_index().rename(columns={"index": "Unit"}).set_index("Time").resample("T").first()
        out.append(high_freq.interpolate("ffill").fillna(method="ffill"))

    recombined = pd.concat(out)

    # Select the latest commitment for every timepoint
    resolved = recombined.reset_index().groupby("Time").last()

    return resolved


def linearize_physical_data(df: pd.DataFrame):
    """Convert a From/To horizontal format to a long format with values at different timepoitns"""

    df = df.copy()
    from_columns = ["levelFrom", "timeFrom"]
    to_columns = ["levelTo", "timeTo"]

    if type(df) == pd.Series:
        # this sometime happens if there is only one data point
        df = pd.DataFrame(df).T

    base_columns = [x for x in df.columns.copy() if x not in from_columns + to_columns]

    if len(df) == 0:
        return pd.DataFrame(columns=base_columns + ["Level", "Time"])

    df = pd.concat(
        (
            df[base_columns + from_columns].rename(columns={"levelFrom": "Level", "timeFrom": "Time"}),
            df[base_columns + to_columns].rename(columns={"levelTo": "Level", "timeTo": "Time"}),
        )
    )

    df["Level"] = df["Level"].astype(float)
    return df


def calculate_curtailment_in_mwh(df_merged: pd.DataFrame) -> float:
    """
    Calculate the curtailment implied by the difference between FPN levels and BOAL

    """

    # TODO is this right? is delta in MW or MWH
    # idea change delta to 'delta_mw' to be sure
    mw_minutes = df_merged["delta"].sum()

    return mw_minutes * MINUTES_TO_HOURS


def calculate_curtailment_costs_in_gbp(df_merged: pd.DataFrame) -> float:
    """
    Calculate the curtailment implied by the difference between FPN levels and BOAL

    """
    # delta is in mw so to get energy in 30 mins we get
    df_merged["energy_mwh"] = df_merged["delta"] * 0.5

    # total costs in pounds
    costs_gbp = -(df_merged["energy_mwh"] * df_merged["bidPrice"]).sum()

    return costs_gbp


def calculate_notified_generation_in_mwh(df_merged: pd.DataFrame) -> float:
    """
    Calculate the total generation implied by the FPN levels

    """

    mw_minutes = df_merged["Level_FPN"].sum()

    return mw_minutes * MINUTES_TO_HOURS


def analyze_one_unit(
    df_boal_unit: pd.DataFrame,
    df_fpn_unit: pd.DataFrame,
    df_bod_unit: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Product a dataframe of actual (curtailed) vs. proposed generation"""

    if isinstance(df_boal_unit, pd.Series):
        df_boal_unit = pd.DataFrame(df_boal_unit).T

    if type(df_fpn_unit) == pd.Series:
        df_fpn_unit = pd.DataFrame(df_fpn_unit).T

    logger.debug(
        f"Analyzing one unit for {len(df_boal_unit)} BOA, "
        f"{len(df_fpn_unit)} FPN and {len(df_bod_unit)} BOD"
    )

    # Make time linear
    df_boal_linear = linearize_physical_data(df_boal_unit)
    df_boal_linear["Accept Time str"] = df_boal_linear["Accept Time"].astype(str)

    # resolve boa data
    unit_boal_resolved = resolve_applied_bid_offer_level(df_boal_linear)

    unit_fpn_resolved = linearize_physical_data(df_fpn_unit).set_index("Time").resample("T").mean(numeric_only=True).interpolate()
    unit_fpn_resolved["Notification Type"] = "FPN"

    # remove last time valye as we dont want to incluce the first minute in the next 30 mins
    unit_fpn_resolved = unit_fpn_resolved.iloc[:-1]

    # We merge BOAL to FPN, so all FPN data is preserved. We want to include
    # units with an FPN but not BOAL
    df_merged = unit_fpn_resolved.join(unit_boal_resolved["Level"], lsuffix="_FPN", rsuffix="_BOAL")

    # If there is no BOALF, then the level after the BOAL is the same as the FPN!
    df_merged["Level_After_BOAL"] = df_merged["Level_BOAL"].fillna(df_merged["Level_FPN"])
    df_merged["delta"] = df_merged["Level_FPN"] - df_merged["Level_After_BOAL"]

    # unsure if we should take '1' or '-1'. they seemd to have the same 'bidPrice'
    if df_bod_unit is not None:
        df_bod_unit = df_bod_unit.copy()
        df_bod_unit.reset_index(inplace=True)
        df_bod_unit["bidOfferPairNumber"] = df_bod_unit["bidOfferPairNumber"].astype(float)
        mask = df_bod_unit["bidOfferPairNumber"] == -1.0
        df_bod_unit = df_bod_unit.loc[mask]
        df_bod_unit["bidPrice"] = df_bod_unit["bidPrice"].astype(float)

        # put bid Price into returned dat
        df_bod_unit["Time"] = pd.to_datetime(df_bod_unit.loc[:, "timeFrom"])

        df_merged = df_merged.merge(df_bod_unit[["bidPrice", "Time"]], on=["Time"], how="outer")
        df_merged["bidPrice"].ffill(inplace=True)

        # bid price is negative
        df_merged["energy_mwh"] = df_merged["delta"] * 1 / 60
        df_merged["cost_gbp"] = -df_merged["bidPrice"] * df_merged["energy_mwh"]

    assert "cost_gbp" in df_merged.columns
    assert "energy_mwh" in df_merged.columns
    assert "delta" in df_merged.columns
    assert "Level_After_BOAL" in df_merged.columns
    assert "Level_BOAL" in df_merged.columns
    assert "Level_FPN" in df_merged.columns

    # change Time from UTC to Europe/London, this is because Time is made from timeFrom which is in UTC
    df_merged["Time"] = pd.to_datetime(df_merged["Time"].dt.tz_localize('UTC')).dt.tz_convert("Europe/London")

    return df_merged


def analyze_curtailment(db: DbRepository, start_time, end_time) -> pd.DataFrame:
    """Produces a dataframe characterizing curtailment between `start_time` and `end_time`

    This uses the SQLite Db's as input, generating a DF that can then be loaded to the Postgres Db
    """

    df_fpn, df_boal, df_bod = db.get_data_for_time_range(start_time=start_time, end_time=end_time)

    curtailment_dfs = []
    # get unique names from bods
    units_fpn = df_fpn.index.unique()
    units_boa = df_boal.index.unique()
    units_bod = df_bod.index.unique()

    units = sorted(set(list(units_fpn) + list(units_boa) + list(units_bod)))
    logger.info(f"Looking at {len(units)} units")

    for i, unit in enumerate(units):
        logger.debug(f"Analyzing {unit} ({i}/{len(units)})")

        if unit in units_boa:
            df_boal_unit = df_boal.loc[unit]
        else:
            logger.debug(f"No BOAs for {unit}, so making empty data")
            df_boal_unit = pd.DataFrame(columns=df_boal.columns)

        if unit in units_fpn:
            df_fpn_unit = df_fpn.loc[unit]
        else:
            logger.debug(f"No FPN for {unit}, so making empty data")
            df_fpn_unit = pd.DataFrame(columns=df_fpn.columns)

        if unit in units_bod:
            df_bod_unit = df_bod.loc[unit]
        else:
            logger.debug(f"No BODS for {unit}, so making empty data")
            df_bod_unit = pd.DataFrame(columns=df_bod.columns)

        df_curtailment_unit = analyze_one_unit(
            df_boal_unit=df_boal_unit,
            df_fpn_unit=df_fpn_unit,
            df_bod_unit=df_bod_unit,
        )

        curtailment_in_mwh = calculate_curtailment_in_mwh(df_curtailment_unit)
        generation_in_mwh = calculate_notified_generation_in_mwh(df_curtailment_unit)
        costs_in_gbp = calculate_curtailment_costs_in_gbp(df_curtailment_unit)

        logger.debug(
            f"Curtailment for {unit} is {curtailment_in_mwh:.2f} MWh. "
            f"Generation was {generation_in_mwh:.2f} MWh"
            f"Costs was {costs_in_gbp:.2f} £"
        )
        logger.debug(f"Done {i} out of {len(units)}")

        curtailment_dfs.append(df_curtailment_unit)

    df_curtailment = pd.concat(curtailment_dfs).copy()
    total_curtailment = df_curtailment["delta"].sum() * MINUTES_TO_HOURS
    logger.info(f"Total curtailment was {total_curtailment:.2f} MWh ")

    # this sometimes happens when there are no boas
    df_curtailment["Level_BOAL"] = df_curtailment["Level_BOAL"].fillna(0.0)
    df_curtailment["cost_gbp"] = df_curtailment["cost_gbp"].fillna(0.0)

    # group and sum by time (in 30 mins chunks)
    df_curtailment = df_curtailment.reset_index()
    df_curtailment["Time"] = pd.to_datetime(df_curtailment["Time"]).dt.floor("30T")
    df_curtailment = df_curtailment.groupby(["Time"]).sum(numeric_only=True)

    # Move 'Time' back to a column
    df_curtailment = df_curtailment.reset_index()

    # delta is in MW, so if we sum in each 30 minutes, we to /30 to get the average
    df_curtailment["delta"] = df_curtailment["delta"] / 30
    df_curtailment["Level_After_BOAL"] = df_curtailment["Level_After_BOAL"] / 30
    df_curtailment["Level_BOAL"] = df_curtailment["Level_BOAL"] / 30
    df_curtailment["Level_FPN"] = df_curtailment["Level_FPN"] / 30

    # remove anything after the start and end datetime
    end_time = add_utc_timezone(pd.to_datetime(end_time))
    start_time = add_utc_timezone(pd.to_datetime(start_time))
    df_curtailment = df_curtailment[df_curtailment["Time"] < pd.to_datetime(end_time)]
    df_curtailment = df_curtailment[df_curtailment["Time"] >= pd.to_datetime(start_time)]

    # reset index
    df_curtailment.reset_index(drop=True, inplace=True)

    assert "cost_gbp" in df_curtailment.columns
    assert "energy_mwh" in df_curtailment.columns
    assert "delta" in df_curtailment.columns
    assert "Level_After_BOAL" in df_curtailment.columns
    assert "Level_BOAL" in df_curtailment.columns
    assert "Level_FPN" in df_curtailment.columns

    return df_curtailment
