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
        "turnup_cost_gbp",
    ]:
        assert col in data_df.columns

    data_df["total_cost_gbp"] = data_df["cost_gbp"] + data_df["turnup_cost_gbp"]

    assert mw_or_mwh in ["mw", "mwh"]

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces # TODO add MEGA_TO_GIGA
    fig.add_trace(
        go.Scatter(
            x=data_df["local_datetime"],
            y=data_df[f"level_fpn_{mw_or_mwh}"] / 1000,
            name="Wind Potential",
            fill="tozeroy",
            fillcolor="rgba(40,100,180,0.3)",
            line=dict(width=0),
            marker=None,
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=data_df["local_datetime"],
            y=data_df[f"level_after_boal_{mw_or_mwh}"] / 1000,
            name="Wind Delivered",
            fill="tozeroy",
            fillcolor="rgba(40,100,180,.5)",
            line=dict(width=0),
            marker=None,
        ),
        secondary_y=False,
    )
    # 50% opacity
    fig.add_trace(
        go.Bar(x=data_df["local_datetime"], y=data_df["total_cost_gbp"],
               name="Costs",
               opacity=.6,
              marker_color="rgb(250,100,50)"),

        secondary_y=True,
    )

    # Add figure title
    if title is not None:
        fig.update_layout(title_text=title)

    # Set x-axis title
    fig.update_xaxes(title_text="Time")
    fig.update_yaxes(showgrid=False)

    fig.update_layout(
        barmode="group",
        bargap=0.5,
        bargroupgap=0.0,
        margin=dict(l=80, r=80, t=40, b=80),
        legend=dict(orientation="h", y=1.2, xanchor="left"),
    )

    if mw_or_mwh == "mw":
        # this is for the day plot
        fig.update_layout(yaxis_range=[0, 16])
        fig.update_layout(yaxis2_range=[-1000, 250_000])

    # Set y-axes titles
    if mw_or_mwh == "mw":
        fig.update_yaxes(title_text="GW", secondary_y=False)
    else:
        fig.update_yaxes(title_text="GWh", secondary_y=False)
    fig.update_yaxes(title_text="Costs [GBP]", secondary_y=True)

    return fig
