import sys
from pathlib import Path

import plotly.express as px

from lib.db_utils import DbRepository
from lib.sbp_utils import get_sbp_data

sys.path.append(str(Path(__file__).parent.parent))

from lib.constants import BASE_DIR

from lib.curtailment import (
    analyze_curtailment,
)
from lib.data import *


def plot(df: pd.DataFrame):
    total_curtailment = df["delta"].sum() * MINUTES_TO_HOURS

    fig = px.area(df, x="Time", y=["Level_FPN", "Level_After_BOAL"])
    fig.update_traces(stackgroup=None, fill="tozeroy")
    fig.update_layout(
        yaxis=dict(title="MW"),
        title=dict(text=f"Total Curtailment {total_curtailment:.2f} MWh"),
    )

    fig.show()

    print("Getting SIP prices")
    sip = pd.DataFrame(get_sbp_data())
    delta = df[["delta", "Time"]]
    delta.set_index("Time", inplace=True)
    delta = delta.resample("30min").mean()
    sip_and_delta = sip.join(delta, how="inner")

    # plot time series
    fig = px.line(sip_and_delta)
    fig.update_layout(
        yaxis=dict(title="MW or £/MWH"),
        xaxis=dict(title="Time"),
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
    df.to_csv(BASE_DIR/f"data/outputs/results-{start_time}-{end_time}.csv")
    plot(df)


if __name__ == "__main__":
    db = DbRepository(BASE_DIR / "scripts/phys_data.db")
    run(db, start_time="2022-01-01", end_time="2022-10-01")
