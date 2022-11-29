import logging
from typing import Optional

import click
import schedule
import time

from lib.data.main import fetch_and_load_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.option("--start", default=None)
@click.option("--end", default=None)
@click.option("--chunk_size_minutes", default=24 * 60)
def main(
    start: Optional[str] = None,
    end: Optional[str] = None,
    chunk_size_minutes: Optional[int] = 24 * 60,
):

    logger.info(f"Running ETL service, data is pulled now and every hour at 15 minutes pass "
                f"{start=} {end=} {chunk_size_minutes=}")

    def job():
        try:
            fetch_and_load_data(
                start=start,
                end=end,
                chunk_size_minutes=chunk_size_minutes,
                multiprocess=True,
                pull_data_once=True,
            )
        except Exception as e:
            logger.info("There was an error running 'fetch_and_load_data'")
            logger.warning(e)
            raise e

    # run job now
    job()

    # run job every hour at 15 mins past
    logger.info('Adding job at 15 past the hour every hour')
    schedule.every().hour.at(":15").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
