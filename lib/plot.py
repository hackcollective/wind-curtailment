import plotly.graph_objects as go
from plotly.subplots import make_subplots


def make_time_series_plot(data_df):
    # data_df needs to have the following columns
    # 'local_datetime', 'Level_FPN', 'Level_After_BOAL', 'cost_gbp'
    for col in ['local_datetime', 'Level_FPN', 'Level_After_BOAL', 'cost_gbp']:
        assert col in data_df.columns

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces
    fig.add_trace(
        go.Scatter(
            x=data_df["local_datetime"], y=data_df["Level_FPN"], name="Level_FPN", fill="tozeroy"
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=data_df["local_datetime"],
            y=data_df["Level_After_BOAL"],
            name="Level_After_BOAL",
            fill="tozeroy",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=data_df["local_datetime"], y=data_df["cost_gbp"], name="cost_gbp"),
        secondary_y=True,
    )

    # Add figure title
    fig.update_layout(title_text="Total Curtailment MWh")

    # Set x-axis title
    fig.update_xaxes(title_text="xaxis title")

    # Set y-axes titles
    fig.update_yaxes(title_text="MW", secondary_y=False)
    fig.update_yaxes(title_text="Costs [GBP]", secondary_y=True)

    return fig
