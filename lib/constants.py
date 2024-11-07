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


df_bm_units = pd.read_csv(DATA_DIR / "BMU.csv", header=0)


G_CO2E_PER_KWH_GAS = 430 # https://unece.org/sites/default/files/2021-10/LCA-2.pdf Table 7.2.1
MW_TO_TW = 1e-6
MW_TO_GW = 1e-3
GBP_TO_MGBP = 1e-6
GBP_TO_KGBP = 1e-3
MW_30m_TO_MWH = 0.5
G_TO_TONNE = 1e-6
MWH_TO_KWH = 1e3