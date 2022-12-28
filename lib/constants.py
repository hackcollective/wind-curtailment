import os
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
SAVE_DIR = DATA_DIR / "PHYBM/raw"
SQL_DIR = BASE_DIR / "sql"

DB_IP = os.environ.get("DB_IP")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
HOST = os.environ.get("HOST")
DB_NAME = os.environ.get("DB_NAME")
df_bm_units = pd.read_excel(DATA_DIR / "BMUFuelType.xls", header=0)
