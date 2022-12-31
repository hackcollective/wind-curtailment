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
    start_time = pd.to_datetime("2022-01-01")
    today =  pd.to_datetime("2022-12-31")
    end_time = start_time + datetime.timedelta(days=1)
    data = []
    # while end_time < today:
    r = requests.get(
        "https://data.elexon.co.uk/bmrs/api/v1/generation/outturn/summary",
        params=dict(startTime=start_time, endTime=end_time, format="json"),
    )
    data.append(r.json())
    start_time = end_time
    end_time += +datetime.timedelta(days=1)

    return data

def fetch_data_b1620(start="2022-01-01", end="2022-01-01"):
    logger.info("Fetching")
    data = client.get_B1620(start, end)
    return data


def analyze_data(data: pd.DataFrame):

    data["generation"] = data["generation"].astype(float)
    renewable_generation = data[data["fuelType"].isin(RENEWABLES)]["generation"].sum()

    total_generation = data["generation"].sum()
    start, end = data["startTime"].min(), data["startTime"].max()

    return renewable_generation, total_generation, (start, end)


if __name__ == "__main__":

    # https://developer.data.elexon.co.uk/api-details#api=prod-insol-insights-api&operation=get-generation-outturn-summary
    data_path = DATA_DIR / "generation.json"

    logging.basicConfig(level=logging.DEBUG)

    data = fetch_data_elexon_insights()
    print(data)

    dfs = [pd.concat((pd.json_normalize(d["data"]), df["startTime"]), axis=1) for d in data]
    df = pd.concat(dfs)

    df.to_csv("yearly_generation.csv")
    renewables, total, time_period = analyze_data(df)
    print(f"{renewables*1e-3=:,f} gwh")
    print(f"{total*1e-3=:,f} gwh")
    print(f"{renewables/total*100=:.1f}%")
    print(time_period)
