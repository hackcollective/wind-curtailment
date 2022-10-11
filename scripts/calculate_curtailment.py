import sys
from pathlib import Path

import plotly.express as px

from lib.db_utils import DbRepository
from lib.sbp_utils import get_sbp_data

sys.path.append(str(Path(__file__).parent.parent))

from lib.constants import BASE_DIR

from lib.curtailment import (
    analyze_one_unit,
    calculate_curtailment_in_mwh,
    calculate_notified_generation_in_mwh,
)
from lib.data import *


def run(db: DbRepository, start_time, end_time):
    """Fetch data from the DB between `start_time` and `end_time`, and calculate and plot the FPN vs the level
    specified by the BOAL.
    """

    df_fpn, df_boal = db.get_data_for_time_range(start_time=start_time, end_time=end_time)
    curtailment_dfs = []
    units = df_boal.index.unique()

    for i, unit in enumerate(units):
        df_curtailment_unit = analyze_one_unit(df_boal_unit=df_boal.loc[unit], df_fpn_unit=df_fpn.loc[unit])

        curtailment_in_mwh = calculate_curtailment_in_mwh(df_curtailment_unit)
        generation_in_mwh = calculate_notified_generation_in_mwh(df_curtailment_unit)

        print(f"Curtailment for {unit} is {curtailment_in_mwh:.2f} MWh. Generation was {generation_in_mwh:.2f} MWh")
        print(f'Done {i} out of {len(units)}')

        curtailment_dfs.append(df_curtailment_unit)

    df_curtailment = pd.concat(curtailment_dfs)
    total_curtailment = df_curtailment["delta"].sum() * MINUTES_TO_HOURS
    print(f"Total curtailment was {total_curtailment:.2f} MWh ")

    df_ = df_curtailment.reset_index().groupby(["Time"]).sum().reset_index()

    fig = px.area(df_, x="Time", y=["Level_FPN", "Level_After_BOAL"])
    fig.update_traces(stackgroup=None, fill="tozeroy")
    fig.update_layout(
        yaxis=dict(title="MW"),
        title=dict(text=f"Total Curtailment {total_curtailment:.2f} MWh"),
    )

    fig.show()

    print('Getting SIP prices')
    sip = pd.DataFrame(get_sbp_data())
    delta = df_[['delta', 'Time']]
    delta.set_index('Time',inplace=True)
    delta = delta.resample('30min').mean()
    sip_and_delta = sip.join(delta, how='inner')

    # plot time series
    fig = px.line(sip_and_delta)
    fig.update_layout(
        yaxis=dict(title="MW or £/MWH"),
        xaxis=dict(title="Time"),
        title=dict(text="SIP and Curtailment"),
    )
    fig.show()

    # plot scatter
    fig = px.scatter(x=sip_and_delta['sip'],y=sip_and_delta['delta'])
    fig.update_layout(
        xaxis=dict(title="SIP [£/MWh]"),
        yaxis=dict(title="Curtailment MW"),
        title=dict(text="Scatter: SIP and Curtailment"),
    )
    fig.show()


if __name__ == "__main__":
    db = DbRepository(BASE_DIR / "scripts/phys_data.db")
    run(db, start_time="2022-01-01", end_time="2022-02-01")
