import logging

import pandas as pd
from sqlalchemy import create_engine

from lib.constants import DATA_DIR
from lib.data.fetch_boa_data import run_boa
from lib.db_utils import drop_and_initialize_tables

engine = create_engine("sqlite:///phys_data.db", echo=False)
df_bm_units = pd.read_excel(DATA_DIR / "BMUFuelType.xls", header=0)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == "__main__":

    logger.info("Get BOA data")

    start = pd.Timestamp("2022-01-01")
    end = pd.Timestamp("2022-02-01")

    drop_and_initialize_tables(f"data/phys_data_{start.isoformat()}_{end.isoformat()}.db")
    wind_units = df_bm_units[df_bm_units["FUEL TYPE"] == "WIND"]["SETT_BMU_ID"].unique()

    run_boa(start_date=start, end_date=end, units=wind_units)
