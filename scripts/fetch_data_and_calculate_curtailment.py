import logging
from typing import Optional

import click
import pandas as pd

from lib.constants import DATA_DIR
from lib.data.main import fetch_and_load_data

# engine = create_engine("sqlite:///phys_data.db", echo=False)
df_bm_units = pd.read_excel(DATA_DIR / "BMUFuelType.xls", header=0)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.option("--start", default=None)
@click.option("--end", default=None)
def main(start: Optional[str] = None, end: Optional[str] = None):
    fetch_and_load_data(start=start, end=end)


if __name__ == "__main__":
    main()
