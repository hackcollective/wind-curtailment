"""Yearly stats for the blog post"""
import datetime
import logging

import pandas as pd
import requests

from lib.constants import DATA_DIR
from lib.data.utils import client

logger = logging.getLogger(__name__)


RENEWABLES = ["BIOMASS", "WIND", "NPSHYD", "OTHER"]


def fetch_data_elexon_insights():
    """Fetching from
    https://developer.data.elexon.co.uk/api-details#api=prod-insol-insights-api&operation=get-generation-outturn-summary

    The frequency of returned data varies randomly with the amount requested, amazingly. 2 day chunks seems to yield
    30 minute resolution i.e. SP.
    """
    start_time = pd.to_datetime("2022-01-01")
    today = pd.to_datetime("2022-12-31")
    end_time = start_time + datetime.timedelta(days=2)
    data = []
    while end_time < today:
        r = requests.get(
            "https://data.elexon.co.uk/bmrs/api/v1/generation/outturn/summary",
            params=dict(startTime=start_time, endTime=end_time, format="json"),
        )
        data.extend(r.json())
        start_time = end_time
        end_time += +datetime.timedelta(days=1)
        logger.info(end_time)

    return data

def format_data_as_df(data):
    dfs = []
    for day in data:
        row = pd.json_normalize(day["data"]).set_index("fuelType").T
        row.index = [day["startTime"]]
        dfs.append(row)

    return pd.concat(dfs)


def fetch_data_b1620(start="2022-01-01", end="2022-01-01"):
    logger.info("Fetching")
    data = client.get_B1620(start, end)
    return data


def analyze_data(df: pd.DataFrame):
    df.index = pd.to_datetime(df.index)
    df_hourly = df.resample("H").mean()
    df_total_generation = df_hourly.sum(axis=1)
    total_generation_twh = df_total_generation.sum() / 1e6
    for fuel, item in df_hourly.sum().iteritems():
        print(f"{fuel} {item / 1e6} Twh")

    total_renewables_twh = df_hourly[RENEWABLES].sum().sum() / 1e6
    pct_renewables = total_renewables_twh / total_generation_twh * 100
    print(f"{pct_renewables=}")
    wind_twh = df_hourly.sum()["WIND"] / 1e6
    pct_wind_of_total = wind_twh / total_generation_twh * 100
    print(f"{pct_wind_of_total=}")
    curtailment_twh = 3.6  # from app
    pct_curtailment = curtailment_twh / wind_twh * 100
    print(f"{pct_curtailment=}")


if __name__ == "__main__":

    # https://developer.data.elexon.co.uk/api-details#api=prod-insol-insights-api&operation=get-generation-outturn-summary
    data_path = DATA_DIR / "generation.json"

    logging.basicConfig(level=logging.DEBUG)

    # data = fetch_data_elexon_insights()
    # df = format_data_as_df(data)
    # df.to_csv("elexon_yearly_generation.csv")

    df = pd.read_csv("elexon_yearly_generation.csv", index_col=0)
    analyze_data(df)
