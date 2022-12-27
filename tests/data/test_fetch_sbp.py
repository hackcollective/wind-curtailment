import pandas as pd

from lib.data.fetch_sbp_data import call_sbp_api


def test_get_sbp():
    start = pd.Timestamp("2022-06-11 04:00")
    end = pd.Timestamp("2022-06-11 05:00")
    df = call_sbp_api(
        start_date=start,
        end_date=end,
    )

    df

