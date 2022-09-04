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




def run(db: DbRepository, start_time, end_time):

    df_fpn, df_boal = db.get_data_for_time_range(start_time=start_time, end_time=end_time)
    curtailment_dfs = []
    units = df_boal.index.unique()

    for unit in units:
        df_curtailment_unit = analyze_one_unit(df_boal_unit=df_boal.loc[unit], df_fpn_unit=df_fpn.loc[unit])

        curtailment_in_mwh = calculate_curtailment_in_mwh(df_curtailment_unit)
        generation_in_mwh = calculate_notified_generation_in_mwh(df_curtailment_unit)

        print(f"Curtailment for {unit} is {curtailment_in_mwh:.2f} MWh. Generation was {generation_in_mwh:.2f} MWh")

        curtailment_dfs.append(df_curtailment_unit)

    df_curtailment = pd.concat(curtailment_dfs)
    total_curtailment = df_curtailment['delta'].sum() * MINUTES_TO_HOURS
    print(f"Total curtailment was {total_curtailment:.2f} MWh ")

    df_ = df_curtailment.reset_index().groupby(["Time"]).sum().reset_index()

    fig = px.area(df_, x="Time", y=["Level_FPN", "Level_After_BOAL"])
    fig.update_traces(stackgroup=None, fill="tozeroy")
    fig.update_layout(
        yaxis=dict(title="MW"),
        title=dict(text=f"Total Curtailment {total_curtailment:.2f} MWh"),
    )

    fig.show()


if __name__ == "__main__":
    db = DbRepository(BASE_DIR / "scripts/phys_data.db")
    run(db, start_time="2022-01-01", end_time="2022-04-01")
