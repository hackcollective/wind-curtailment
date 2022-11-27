import logging
from datetime import datetime, timedelta, timezone
import click

from typing import Optional

import pandas as pd
from sqlalchemy import create_engine

from lib.constants import DATA_DIR
from lib.db_utils import DbRepository
from lib.data.fetch_boa_data import run_boa
from lib.data.fetch_bod_data import run_bod
from lib.db_utils import drop_and_initialize_bod_table, drop_and_initialize_tables

from lib.curtailment import (
    analyze_curtailment,
)

from lib.gcp_db_utils import write_data, load_data

# engine = create_engine("sqlite:///phys_data.db", echo=False)
df_bm_units = pd.read_excel(DATA_DIR / "BMUFuelType.xls", header=0)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.option("--db_url", default=None)
def main(db_url: Optional[str] = None):

    # get yesterdays date
    now = datetime.now(tz=timezone.utc)
    today = now.date()
    yesterday = today - timedelta(days=1)

    start = pd.Timestamp(yesterday)
    end = pd.Timestamp(today)

    wind_units = df_bm_units[df_bm_units["FUEL TYPE"] == "WIND"]["SETT_BMU_ID"].unique()

    # make new database
    if db_url is None:
        db_url = f"phys_data_{yesterday}_{today}.db"

    # initializedatabase
    drop_and_initialize_tables(db_url)
    drop_and_initialize_bod_table(db_url)

    engine = create_engine(f"sqlite:///{db_url}", echo=False)

    # get BOAs and BODs
    run_boa(
        start_date=start,
        end_date=end,
        units=wind_units,
        chunk_size_in_days=1,
        database_engine=engine,
    )
    run_bod(
        start_date=start,
        end_date=end,
        units=wind_units,
        chunk_size_in_days=1,
        database_engine=engine,
    )

    # run analysis
    db = DbRepository(db_url)
    df = analyze_curtailment(db, str(start), str(end))
    print(df.head())
    df.to_csv(f"./data/outputs/results-{start}-{end}.csv")

    # # load csv and save to database
    df = load_data(f"./data/outputs/results-{start}-{end}.csv")
    write_data(df=df)


if __name__ == "__main__":
    main()
