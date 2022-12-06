import plotly.graph_objects as go
from plotly.subplots import make_subplots


def make_time_series_plot(data_df, title: str = None, mw_or_mwh: str = "mw"):
    # data_df needs to have the following columns
    # 'local_datetime' 'cost_gbp' and
    # 'Level_fpn_mw', 'level_after_boal_mw',
    # or
    # 'Level_fpn_mwh', 'level_after_boal_mwh'

    if "local_datetime" not in data_df.columns:
        data_df["local_datetime"] = data_df["time"]

    for col in [
        "local_datetime",
        f"level_fpn_{mw_or_mwh}",
        f"level_after_boal_{mw_or_mwh}",
        "cost_gbp",
    ]:
        assert col in data_df.columns

    assert mw_or_mwh in ["mw", "mwh"]

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces
    fig.add_trace(
        go.Scatter(
            x=data_df["local_datetime"],
            y=data_df[f"level_fpn_{mw_or_mwh}"] / 1000,
            name="FPN",
            fill="tozeroy",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=data_df["local_datetime"],
            y=data_df[f"level_after_boal_{mw_or_mwh}"] / 1000,
            name="level after boal",
            fill="tozeroy",
        ),
        secondary_y=False,
    )
    # 50% opaacity
    fig.add_trace(
        go.Bar(x=data_df["local_datetime"], y=data_df["cost_gbp"], name="cost_gbp", opacity=0.5),
        secondary_y=True,
    )

    # Add figure title
    if title is not None:
        fig.update_layout(title_text=title)

    # Set x-axis title
    fig.update_xaxes(title_text="Time")

    fig.update_layout(barmode="group", bargap=0.5, bargroupgap=0.0)

    # Set y-axes titles
    if mw_or_mwh == "mw":
        fig.update_yaxes(title_text="GW", secondary_y=False)
    else:
        fig.update_yaxes(title_text="GWh", secondary_y=False)
    fig.update_yaxes(title_text="Costs [GBP]", secondary_y=True)

    return fig
