import plotly.graph_objects as go
from plotly.subplots import make_subplots


def make_time_series_plot(data_df, title:str = None):
    # data_df needs to have the following columns
    # 'local_datetime', 'Level_FPN', 'Level_After_BOAL', 'cost_gbp'

    if 'local_datetime' not in data_df.columns:
        data_df['local_datetime'] = data_df['time']

    for col in ['local_datetime', 'level_fpn', 'level_after_boal', 'cost_gbp']:
        assert col in data_df.columns

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces
    fig.add_trace(
        go.Scatter(
            x=data_df["local_datetime"], y=data_df["level_fpn"] / 1000, name="FPN", fill="tozeroy"
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=data_df["local_datetime"],
            y=data_df["level_after_boal"] / 1000,
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

    fig.update_layout(barmode='group', bargap=0.5, bargroupgap=0.0)

    # Set y-axes titles
    fig.update_yaxes(title_text="GW", secondary_y=False)
    fig.update_yaxes(title_text="Costs [GBP]", secondary_y=True)

    return fig
