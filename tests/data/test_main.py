from lib.data.main import fetch_and_load_data
import pandas as pd

def test_fetch_and_load_data():

    start = '2024-04-06 05:00'
    end = '2024-04-06 06:00'
    df = fetch_and_load_data(start=start, end=end, save=False)

    assert len(df) == 2
    assert df["delta_mw"].mean() > 2000
    assert df["delta_mw"].mean() < 3000
    assert df["level_fpn"].mean() > 16000
    assert df["level_fpn"].mean() < 17000
    assert df["level_after_boal"].mean() > 13000
    assert df["level_after_boal"].mean() < 15000

    assert df["cost_gbp"].mean() >= 60000
    assert df["cost_gbp"].mean() <= 70000

    assert pd.to_datetime(df['time'][0]) == pd.Timestamp("2024-04-06 06:00:00+01:00")


def test_fetch_and_load_data_winter():

    start = '2024-02-01 05:00'
    end = '2024-02-01 06:00'
    df = fetch_and_load_data(start=start, end=end, save=False)

    assert len(df) == 2
    assert df["delta_mw"].mean() > 2000
    assert df["delta_mw"].mean() < 3000
    assert df["level_fpn"].mean() > 13000
    assert df["level_fpn"].mean() < 14000
    assert df["level_after_boal"].mean() > 11000
    assert df["level_after_boal"].mean() < 12000

    assert df["cost_gbp"].mean() >= 60000
    assert df["cost_gbp"].mean() <= 70000

    assert pd.to_datetime(df['time'][0]) == pd.Timestamp("2024-02-01 05:00:00+00:00")
