"""
March 19th chosen on the basis of sustained negative prices - see `notebooks/curtailment.ipynb`

Data for March 19th manually downloaded from https://www.bmreports.com/bmrs/?q=balancing/physicaldata
"""

import pandas as pd
from pathlib import Path
import plotly.express as px

data_path = Path(__file__).parent.parent / "data"


def load_fpn_data_and_join_unit_type(
    fpn_path=data_path / "PhysicalData_20220319_27.csv",
    unit_type_path=data_path / "BMUFuelType.xls",
):

    df_fpn = pd.read_csv(
        fpn_path,
        skiprows=lambda x: x > 1541 or x == 0,
        names=[
            "Notification Type",
            "Unit ID",
            "Settlement Period",
            "From Time",
            "From Level",
            "To Time",
            "To Level",
        ],
        parse_dates=["From Time", "To Time"],
        date_parser=lambda x: pd.to_datetime(x, format="%Y%m%d%H%M%S"),
    )  # This includes other kinds of data with different cols, so have to skiprows

    df_bm_units = pd.read_excel(unit_type_path, header=0)
    df_fpn = (
        df_fpn.set_index("Unit ID")
        .join(df_bm_units.set_index("SETT_BMU_ID")["FUEL TYPE"])
        .rename(columns={"FUEL TYPE": "Fuel Type"})
    )

    df_fpn = df_fpn.rename_axis("Unit ID").reset_index()
    df_fpn["delta"] = df_fpn["To Level"] - df_fpn["From Level"]

    print(df_fpn["Fuel Type"].value_counts(dropna=False))

    return df_fpn


def convert_to_linear_type(df: pd.DataFrame):
    base_columns = ["Notification Type", "Settlement Period", "Fuel Type"]
    from_columns = ["From Level", "From Time"]
    to_columns = ["To Level", "To Time"]

    return pd.concat(
        (
            df[base_columns + from_columns].rename(
                columns={"From Level": "Level", "From Time": "Time"}
            ),
            df[base_columns + to_columns].rename(columns={"To Level": "Level", "To Time": "Time"}),
        )
    )


def filter_for_units_that_change(df_fpn: pd.DataFrame) -> pd.DataFrame:
    interesting_units = set(df_fpn[df_fpn["From Level"] != df_fpn["To Level"]]["Unit ID"])

    return df_fpn[df_fpn["Unit ID"].isin(interesting_units)]


def calculate_curtailment_for_settlement_period(df_fpn):
    """Sum all negative deltas i.e. curtailing actions"""

    df_curtailed = df_fpn[df_fpn["delta"] < 0]

    # TODO: convert this all into MWh deltas
    df_curtailed = df_curtailed.groupby("Unit ID").agg({"Fuel Type": "first", "delta": "sum"})

    return df_curtailed


def plot_units_by_type(df: pd.DataFrame):
    df["Fuel Type"] = df["Fuel Type"].apply(lambda x: "Unknown" if pd.isnull(x) else x)

    return px.bar(
        df,
        y="delta",
        color="Fuel Type",
    )


if __name__ == "__main__":
    df_fpn = load_fpn_data_and_join_unit_type()
    df_fpn = filter_for_units_that_change(df_fpn)

    df_curtailed = calculate_curtailment_for_settlement_period(df_fpn)
    fig = plot_units_by_type(df_curtailed)
    fig.show()
