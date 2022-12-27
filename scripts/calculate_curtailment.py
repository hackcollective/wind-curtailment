import sys
from pathlib import Path

import plotly.express as px

from lib.db_utils import DbRepository
from lib.plot import make_time_series_plot
from lib.sbp_utils import load_sbp_data_from_file

sys.path.append(str(Path(__file__).parent.parent))

from lib.constants import BASE_DIR

from lib.curtailment import (
    analyze_one_unit,
    calculate_curtailment_in_mwh,
    calculate_notified_generation_in_mwh,
    calculate_curtailment_costs_in_gbp,
    analyze_curtailment,
)
from lib.data.utils import *


def run1(db: DbRepository, start_time, end_time):
    """Fetch data from the DB between `start_time` and `end_time`, and calculate and plot the FPN vs the level
    specified by the BOAL.
    """

    df_fpn, df_boal, df_bod = db.get_data_for_time_range(start_time=start_time, end_time=end_time)
    curtailment_dfs = []
    units = df_boal.index.unique()

    for i, unit in enumerate(units):
        df_curtailment_unit = analyze_one_unit(
            df_boal_unit=df_boal.loc[unit],
            df_fpn_unit=df_fpn.loc[unit],
            df_bod_unit=df_bod.loc[unit],
        )

        curtailment_in_mwh = calculate_curtailment_in_mwh(df_curtailment_unit)
        generation_in_mwh = calculate_notified_generation_in_mwh(df_curtailment_unit)
        costs_in_gbp = calculate_curtailment_costs_in_gbp(df_curtailment_unit)

        print(curtailment_in_mwh, generation_in_mwh, costs_in_gbp)
        print(
            f"Curtailment for {unit} is {curtailment_in_mwh:.2f} MWh. "
            f"Generation was {generation_in_mwh:.2f} MWh,"
            f"Costs was {costs_in_gbp:.2f} GBP"
        )
        print(f"Done {i} out of {len(units)}")

        curtailment_dfs.append(df_curtailment_unit)

    df_curtailment = pd.concat(curtailment_dfs)
    total_curtailment = df_curtailment["delta"].sum() * MINUTES_TO_HOURS
    total_cost = (df_curtailment["cost_gbp"]).sum()
    print(f"Total curtailment was {total_curtailment:.2f} MWh ")
    print(f"Total curtailment was {total_cost:.2f} GBP ")

    print(df_curtailment)
    df_ = df_curtailment.reset_index().groupby(["local_datetime"]).sum().reset_index()

    fig = make_time_series_plot(data_df=df_)
    fig.show()

    print("Getting SIP prices")
    sip = pd.DataFrame(load_sbp_data_from_file())
    delta = df_[["delta", "local_datetime"]]
    delta.set_index("local_datetime", inplace=True)
    delta = delta.resample("30min").mean()
    sip_and_delta = sip.join(delta, how="inner")

    # plot time series
    fig = px.line(sip_and_delta)
    fig.update_layout(
        yaxis=dict(title="MW or £/MWH"),
        xaxis=dict(title="local_datetime"),
        title=dict(text="SIP and Curtailment"),
    )
    fig.show()

    # plot scatter
    fig = px.scatter(x=sip_and_delta["sip"], y=sip_and_delta["delta"])
    fig.update_layout(
        xaxis=dict(title="SIP [£/MWh]"),
        yaxis=dict(title="Curtailment MW"),
        title=dict(text="Scatter: SIP and Curtailment"),
    )
    fig.show()


def run(db: DbRepository, start_time, end_time):
    """Fetch data from the DB between `start_time` and `end_time`, and calculate and plot the FPN vs the level
    specified by the BOAL.
    """

    df = analyze_curtailment(db, start_time, end_time)
    df.to_csv(BASE_DIR / f"data/outputs/results-{start_time}-{end_time}.csv")
    make_time_series_plot(df)


if __name__ == "__main__":
    db = DbRepository(BASE_DIR / "scripts/phys_data.db")
    run(db, start_time="2022-01-01", end_time="2022-10-01")
