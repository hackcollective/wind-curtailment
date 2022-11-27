import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

sys.path.append("/src")

from lib import constants
from lib.constants import BASE_DIR

from logging import getLogger

logger = getLogger(__name__)


def get_db_connection():
    logger.info("Connecting to DB")
    address = f"postgresql+psycopg2://{constants.DB_USERNAME}:{constants.DB_PASSWORD}@{constants.DB_IP}:5432"
    return create_engine(url=address)


def load_data(path: Path) -> pd.DataFrame:
    logger.info("Loading Data")
    df = pd.read_csv(path, index_col=0)
    df = df.rename(
        columns={
            "Time": "time",
            "Level_FPN": "level_fpn",
            "Level_BOAL": "level_boal",
            "Level_After_BOAL": "level_after_boal",
            "delta": "delta_mw",
        }
    )
    return df[["time", "level_fpn", "level_boal", "level_after_boal", "delta_mw"]].head(100)


def write_data(df: pd.DataFrame):
    logger.info("Writing to DB")
    engine = get_db_connection()
    with engine.connect() as conn:
        df.to_sql("curtailment", conn, if_exists="append", index=False)

    logger.info("Finished writing")


def main():
    path = BASE_DIR / "data/outputs/results-2022-01-01-2022-10-01.csv"
    df = load_data(path)
    write_data(df)


if __name__ == "__main__":
    main()
