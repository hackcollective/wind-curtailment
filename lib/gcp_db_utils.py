from pathlib import Path
import logging

import pandas as pd
from sqlalchemy import create_engine

from lib import constants

logger = logging.getLogger(__name__)


def get_db_connection():
    address = f"postgresql+psycopg2://{constants.DB_USERNAME}:{constants.DB_PASSWORD}@{constants.DB_IP}:5432"
    return create_engine(url=address)


def write_data(df: pd.DataFrame):
    engine = get_db_connection()

    if 'local_datetime' in df.columns:
        df = df.rename(columns={"local_datetime": "time"})

    logger.info(f'Adding curtailment to database ({len(df)}')

    with engine.connect() as conn:
        df.to_sql("curtailment", conn, if_exists="append", index=False)


def load_data(path: Path) -> pd.DataFrame:
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
    df['time'] = df['local_datetime']
    return df[["time", "level_fpn", "level_boal", "level_after_boal", "delta_mw"]]