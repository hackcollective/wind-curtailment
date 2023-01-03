import plotly.express as px
import plotly.io as pio

from lib.gcp_db_utils import read_data

pio.renderers.default = "svg"


def get_data():
    df = read_data("2022-01-01", "2023-01-01")
    df["delta_gw"] = df["delta_mw"] * 1e-3
    df.to_csv("year_data_dump.csv")
    return df


def main():
    df = read_data("2022-01-01", "2022-01-10")
    df["delta_gw"] = df["delta_mw"] * 1e-3
    fig = px.histogram(df, "delta_mw")
    fig.show()


if __name__ == "__main__":
    get_data()
