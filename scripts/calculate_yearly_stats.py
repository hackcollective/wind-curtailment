"""Yearly stats for the blog post"""
import json
import logging

import pandas as pd

from lib.constants import DATA_DIR
from lib.data.utils import client

logger = logging.getLogger(__name__)


RENEWABLES = ["BIOMASS", "WIND", "NPSHYD", "OTHER"]


def fetch_data(start="2022-01-01", end="2022-01-01"):
    logger.info("Fetching")
    data = client.get_B1620(start, end)
    return data


def analyze_data(data: pd.DataFrame):
    data = data[data["documentType"] == "Actual generation per type"]
    data["quantity"] = data["quantity"].astype(float)
    data["powerSystemResourceType"] = data["powerSystemResourceType"].str.strip('"')

    data["mwh"] = data["quantity"] * 0.5
    renewable_generation = data[data["powerSystemResourceType"].isin(RENEWABLES)]["mwh"].sum()
    total_generation = data["mwh"].sum()

    start, end = data["local_datetime"].min(), data["local_datetime"].max()

    return renewable_generation, total_generation, (start, end)


if __name__ == "__main__":

    # https://developer.data.elexon.co.uk/api-details#api=prod-insol-insights-api&operation=get-generation-outturn-summary
    data_path = DATA_DIR / 'generation.json'

    logging.basicConfig(level=logging.DEBUG)
    # df = fetch_data(start='2022-01-01', end='2022-12-31')
    df = pd.read_json(data_path)
    with open(data_path, 'r') as f:
        data = json.load(f)

    dfs = [pd.concat((pd.json_normalize(d["data"]), df['startTime']), axis=1) for d in data]
    df = pd.concat(dfs)

    df.to_csv("yearly_generation.csv")
    renewables, total, time_period = analyze_data(df)
    print(f"{renewables*1e-3=:,f} gwh")
    print(f"{total*1e-3=:,f} gwh")
    print(f"{renewables/total*100=:.1f}%")
    print(time_period)
