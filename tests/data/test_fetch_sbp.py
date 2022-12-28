import pandas as pd

from lib.data.fetch_sbp_data import call_sbp_api


def test_get_sbp():
    start = pd.Timestamp("2022-01-01 00:00")
    end = pd.Timestamp("2022-12-27 00:00")
    df = call_sbp_api(
        start_date=start,
        end_date=end,
    )

    df.to_csv("sbp.csv")
