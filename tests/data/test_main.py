from lib.data.main import fetch_and_load_data


def test_fetch_and_load_data():

    #TODO: I think this is running against the PROD database, which seems dangerous!

    start = '2022-06-11 04:00'
    end = '2022-06-11 05:00'
    df = fetch_and_load_data(start=start, end=end)

    assert len(df) == 2
    assert df["delta_mw"].mean() > 4000
    assert df["delta_mw"].mean() < 5000
    assert df["level_fpn"].mean() > 12000
    assert df["level_fpn"].mean() < 13000
    assert df["level_after_boal"].mean() > 8000
    assert df["level_after_boal"].mean() < 9000

    assert df["cost_gbp"].mean() >= 206000
    assert df["cost_gbp"].mean() <= 207000
