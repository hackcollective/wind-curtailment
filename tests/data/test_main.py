from lib.data.main import fetch_and_load_data
import pandas as pd

def test_fetch_and_load_data():

    start = '2022-06-11 05:00'
    end = '2022-06-11 06:00'
    df = fetch_and_load_data(start=start, end=end, save=False)

    assert len(df) == 2
    assert df["delta_mw"].mean() > 4000
    assert df["delta_mw"].mean() < 5000
    assert df["level_fpn"].mean() > 12000
    assert df["level_fpn"].mean() < 13000
    assert df["level_after_boal"].mean() > 8000
    assert df["level_after_boal"].mean() < 9000

    assert df["cost_gbp"].mean() >= 206000
    assert df["cost_gbp"].mean() <= 207000

    assert pd.to_datetime(df['time'][0]) == pd.Timestamp("2022-06-11 05:00:00+01:00")


def test_fetch_and_load_data_winter():

    start = '2022-11-11 05:00'
    end = '2022-11-11 06:00'
    df = fetch_and_load_data(start=start, end=end, save=False)

    assert len(df) == 2
    assert df["delta_mw"].mean() > 4000
    assert df["delta_mw"].mean() < 5000
    assert df["level_fpn"].mean() > 15000
    assert df["level_fpn"].mean() < 16000
    assert df["level_after_boal"].mean() > 10000
    assert df["level_after_boal"].mean() < 11000

    assert df["cost_gbp"].mean() >= 206000
    assert df["cost_gbp"].mean() <= 207000

    assert pd.to_datetime(df['time'][0]) == pd.Timestamp("2022-11-11 05:00:00+00:00")
