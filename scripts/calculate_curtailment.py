# Get all data for the specified day, for BOAL
# Get all data for the specified day, for FPN
# Linearize and compute curtailment
# Graph

from pathlib import Path
from sqlite3 import DatabaseError

import pandas as pd

import sys

sys.path.append(str(Path(__file__).parent.parent))


from lib.curtailment import (
    analyze_one_unit,
    calculate_curtailment_in_mwh,
    calculate_notified_generation_in_mwh,
)
from lib.data import *


if __name__ == "__main__":

    DATA_DIR = Path(__file__).parent.parent / "data"
    SAVE_DIR = DATA_DIR / "PHYBM/raw"

    df_bm_units = pd.read_excel(DATA_DIR / "BMUFuelType.xls", header=0)

    df = fetch_physical_data(
        start_date="2022-03-19 13:00", end_date="2022-03-19 13:30", save_dir=SAVE_DIR
    )
    df = format_physical_data(df)
    df = add_bm_unit_type(df, df_bm_units=df_bm_units)

    df_fpn, df_boal = parse_fpn_from_physical_data(df), parse_boal_from_physical_data(
        df
    )

    wind_units = df_boal[df_boal["Fuel Type"] == "WIND"].index.unique()

    curtailment_dfs = []

    for unit in wind_units:
        df_curtailment_unit = analyze_one_unit(
            df_boal_unit=df_boal.loc[unit], df_fpn_unit=df_fpn.loc[unit]
        )

        curtailment_in_mwh = calculate_curtailment_in_mwh(df_curtailment_unit)
        generation_in_mwh = calculate_notified_generation_in_mwh(df_curtailment_unit)

        print(
            f"Curtailment for {unit} is {curtailment_in_mwh:.2f} MWh. Generation was {generation_in_mwh:.2f} MWh"
        )

        curtailment_dfs.append(df_curtailment_unit)

    df_curtailment = pd.concat(curtailment_dfs)
