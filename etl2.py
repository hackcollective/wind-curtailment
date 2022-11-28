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
def main(start: Optional[str] = None, end: Optional[str] = None):

    def job():
        fetch_and_load_data(start=start, end=end, chunk_size_minutes=24*60,multiprocess=True)

    # run job now
    job()

    # run job at 04:00 every day
    schedule.every().day.at("04:00").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
