import logging

import pandas as pd
from sqlalchemy import create_engine

from lib.constants import DATA_DIR
from lib.data.fetch_bod_data import run_bod
from lib.db_utils import drop_and_initialize_bod_table

engine = create_engine("sqlite:///phys_data.db", echo=False)
df_bm_units = pd.read_excel(DATA_DIR / "BMUFuelType.xls", header=0)

logging.basicConfig(level=logging.INFO)

MAX_RETRIES = 1

if __name__ == "__main__":

    logger = logging.getLogger(__file__)

    drop_and_initialize_bod_table("phys_data.db")
    wind_units = df_bm_units[df_bm_units["FUEL TYPE"] == "WIND"]["SETT_BMU_ID"].unique()

    start = pd.Timestamp("2022-01-01 00:00")
    end = pd.Timestamp("2022-01-02 00:00")

    run_bod(start_date=start, end_date=end, units=wind_units)
