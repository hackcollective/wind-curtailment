import pandas as pd

from lib.constants import BASE_DIR


def load_sbp_data_from_file() -> pd.Series:

    # read data
    sip = pd.read_csv(f"{BASE_DIR}/data/sspsbpniv.csv")

    # format datetime
    sip["datetime"] = pd.to_datetime(sip["Settlement Date"], format="%d/%m/%Y")
    sip["datetime"] += pd.to_timedelta((sip["Settlement Period"] - 1) * 30, unit="m")

    # reduce to the data we want
    sip.index = sip["datetime"]
    sip["sip"] = sip["System Buy Price(£/MWh)"]

    return sip["sip"]
