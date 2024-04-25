import logging
from datetime import datetime

import pandas as pd
import requests

logger = logging.getLogger(__name__)

"""
The new elexon api url is 
'https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/2024-04-23?format=json'

Note that prices seem to be only available in 2024
"""

url = "https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/"


def call_sbp_api(start_date: str, end_date: str):

    logger.info(f"Getting SIP from {start_date} to {end_date}")

    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date)
    if isinstance(end_date, str):
        end_date = datetime.fromisoformat(end_date)

    start_day = start_date.date()
    end_day = end_date.date()

    days = pd.date_range(start_day, end_day, freq="D")
    data_df = []
    for day in days:
        day = day.strftime("%Y-%m-%d")

        url_day = f"{url}{day}?format=json"
        r = requests.get(url_day)

        data_one_day_df = pd.DataFrame(r.json()["data"])
        data_df.append(data_one_day_df)

    data_df = pd.concat(data_df)

    # "startTime" is time in UTC
    # "systemSellPrice" is the SBP price
    data_df = data_df[["startTime", "systemSellPrice"]]
    data_df["startTime"] = pd.to_datetime(data_df["startTime"])

    # change UTC to Europe/London
    data_df["local_datetime"] = data_df["startTime"].dt.tz_convert("Europe/London")

    # filter on start and end date, as we sometimes get more
    if start_date.tzinfo is None:
        start_date = start_date.tz_localize(tz="UTC")
    if end_date.tzinfo is None:
        end_date = end_date.tz_localize(tz="UTC")

    data_df = data_df[data_df["local_datetime"] >= start_date]
    data_df = data_df[data_df["local_datetime"] < end_date]

    return data_df
