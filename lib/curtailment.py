import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

from typing import Tuple

MINUTES_TO_HOURS = 1 / 60


def resolve_applied_bid_offer_level(df_linear: pd.DataFrame):
    """
    We can have multiple levels for a given timepoint, because levels are fixed
    at one point and then overwitten at a later timepoint, before the moment in
    question has arrived.

    We need to resolve them, choosing the latest possible commitment for each timepoint.

    We need to upsample the data first to achieve this.
    """

    out = []

    for accept_id, data in df_linear.groupby("Accept ID"):
        high_freq = (
            data.reset_index()
            .rename(columns={"index": "Unit"})
            .set_index("Time")
            .resample("T")
            .first()
        )
        out.append(high_freq.interpolate("ffill").fillna(method="ffill"))

    recombined = pd.concat(out)

    # Select the latest commitment for every timepoint
    resolved = recombined.reset_index().groupby("Time").last()

    return resolved


def linearize_physical_data(df: pd.DataFrame):
    """Convert a From/To horizontal format to a long format with values at different timepoitns"""

    df = df.copy()
    from_columns = ["From Level", "From Time"]
    to_columns = ["To Level", "To Time"]

    base_columns = [x for x in df.columns.copy() if x not in from_columns + to_columns]

    df = pd.concat(
        (
            df[base_columns + from_columns].rename(
                columns={"From Level": "Level", "From Time": "Time"}
            ),
            df[base_columns + to_columns].rename(
                columns={"To Level": "Level", "To Time": "Time"}
            ),
        )
    )

    df["Level"] = df["Level"].astype(float)
    return df


def calculate_curtailment_in_mwh(df_merged: pd.DataFrame) -> float:
    """
    Calculate the curtailment implied by the difference between FPN levels and BOAL

    """

    mw_minutes = df_merged["delta"].sum()

    return mw_minutes * MINUTES_TO_HOURS


def calculate_notified_generation_in_mwh(df_merged: pd.DataFrame) -> float:
    """
    Calculate the total generation implied by the FPN levels

    """

    mw_minutes = df_merged["Level_FPN"].sum()

    return mw_minutes * MINUTES_TO_HOURS


def analyze_one_unit(
    df_boal_unit: pd.DataFrame, df_fpn_unit: pd.DataFrame
) -> pd.DataFrame:
    """Product a dataframe of actual (curtailed) vs. proposed generation"""

    # Make time linear
    df_boal_linear = linearize_physical_data(df_boal_unit)
    df_boal_linear["Accept Time str"] = df_boal_linear["Accept Time"].astype(str)

    # resolve boa data
    unit_boal_resolved = resolve_applied_bid_offer_level(df_boal_linear)
    unit_boal_resolved.head()

    if type(df_fpn_unit) == pd.Series:
        df_fpn_unit = pd.DataFrame(df_fpn_unit).T

    unit_fpn_resolved = (
        linearize_physical_data(df_fpn_unit)
        .set_index("Time")
        .resample("T")
        .mean()
        .interpolate()
    )
    unit_fpn_resolved["Notification Type"] = "FPN"

    # cmobind both BOA and FPN data
    # combined_one_unit = pd.concat((unit_boal_resolved, unit_fpn_resolved)) Does this yield the same result? Not sure

    df_merged = unit_boal_resolved.join(unit_fpn_resolved["Level"], rsuffix="_FPN")
    df_merged["delta"] = df_merged["Level_FPN"] - df_merged["Level"]

    return df_merged
