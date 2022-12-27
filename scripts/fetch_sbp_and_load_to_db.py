import logging
from typing import Optional

import click
import pandas as pd

from lib.data.fetch_sbp_data import call_sbp_api
from lib.gcp_db_utils import write_sbp_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@click.command()
@click.option("--start", default=None)
@click.option("--end", default=None)
def main(start: Optional[str] = None, end: Optional[str] = None, chunk_size_days=2):
    start = pd.Timestamp(start)
    end = pd.Timestamp(end)

    start_chunk = start
    end_chunk = start_chunk + pd.Timedelta(f"{chunk_size_days}D")

    while end_chunk <= end:
        logger.info(f"Fetching chunk {start_chunk}-{end_chunk}")
        df_sbp = call_sbp_api(
            start_date=start_chunk,
            end_date=end_chunk,
        )

        logger.info(f"Data fetched, writing to DB")
        write_sbp_data(df_sbp)
        start_chunk = end_chunk
        end_chunk += pd.Timedelta(f"{chunk_size_days}D")


if __name__ == "__main__":

    main(start="2022-01-01", end="2022-12-27")
