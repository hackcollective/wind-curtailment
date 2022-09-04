# Get all data for the specified day, for BOAL
# Get all data for the specified day, for FPN
# Linearize and compute curtailment
# Graph

from pathlib import Path

import sys
import plotly.express as px

from lib.db_utils import DbRepository

sys.path.append(str(Path(__file__).parent.parent))

from lib.constants import DATA_DIR, SAVE_DIR, BASE_DIR

from lib.curtailment import (
    analyze_one_unit,
    calculate_curtailment_in_mwh,
    calculate_notified_generation_in_mwh,
)
from lib.data import *

db = DbRepository(BASE_DIR / "scripts/phys_data.db")


def run(data_dir: Path, save_dir: Path):

    # df_bm_units = pd.read_excel(data_dir / "BMUFuelType.xls", header=0)
    #
    # df = fetch_physical_data(start_date="2022-01-01 00:00:00", end_date="2022-01-08 00:00:00", save_dir=save_dir)
    # df = format_physical_data(df)
    # df = add_bm_unit_type(df, df_bm_units=df_bm_units)
    #
    # df_fpn, df_boal = parse_fpn_from_physical_data(df), parse_boal_from_physical_data(df)

    # wind_units = df_boal[df_boal["Fuel Type"] == "WIND"].index.unique()

    df_fpn, df_boal = db.get_data_for_time_range("2022-01-01", "2022-01-02")
    curtailment_dfs = []
    units = df_boal.index.unique()

    for unit in units:
        df_curtailment_unit = analyze_one_unit(df_boal_unit=df_boal.loc[unit], df_fpn_unit=df_fpn.loc[unit])

        curtailment_in_mwh = calculate_curtailment_in_mwh(df_curtailment_unit)
        generation_in_mwh = calculate_notified_generation_in_mwh(df_curtailment_unit)

        print(f"Curtailment for {unit} is {curtailment_in_mwh:.2f} MWh. Generation was {generation_in_mwh:.2f} MWh")

        curtailment_dfs.append(df_curtailment_unit)

    df_curtailment = pd.concat(curtailment_dfs)
    print(f"Total curtailment was {df_curtailment['delta'].sum() * MINUTES_TO_HOURS:.2f} MWh ")

    df_ = df_curtailment.reset_index().groupby(["Fuel Type", "Time"]).sum().reset_index()

    fig = px.area(df_, x="Time", y=["Level_FPN", "Level"], facet_col="Fuel Type")
    fig.update_traces(stackgroup=None, fill="tozeroy")
    fig.update_layout(
        yaxis=dict(title="MW"),
        title=dict(text="Gas units are being switched on, whilst wind is being curtailed"),
    )

    fig.show()


if __name__ == "__main__":

    run(data_dir=DATA_DIR, save_dir=SAVE_DIR)
