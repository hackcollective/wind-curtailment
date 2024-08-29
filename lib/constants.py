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


def get_df_bm_units():
    # extra units from
    df_bm_units = pd.read_excel(DATA_DIR / "BMUFuelType.xls", header=0)

    # extra units from https://github.com/hackcollective/wind-curtailment/pull/74
    extra_wind_units= [
        "T_SGRWO-2",
        "T_SGRWO-3",
        "T_SGRWO-4",
        "T_SGRWO-5",
        "T_SGRWO-6",
        "E_FDUN-1",
        "T_GYMRW-1",
        "T_GYMRW-2",
        "E_GFLDW-1",
        "E_THNTW-1",
        "E_THNTW-2",
    ]
    df_bm_extra_units = pd.DataFrame(extra_wind_units, columns=['SETT_BMU_ID'])
    df_bm_extra_units['FUEL TYPE'] = 'WIND'
    df_bm_units = pd.concat([df_bm_units, df_bm_extra_units])

    return df_bm_units


df_bm_units = get_df_bm_units()

G_CO2E_PER_KWH_GAS = 430 # https://unece.org/sites/default/files/2021-10/LCA-2.pdf Table 7.2.1
MW_TO_TW = 1e-6
MW_TO_GW = 1e-3
GBP_TO_MGBP = 1e-6
GBP_TO_KGBP = 1e-3
MW_30m_TO_MWH = 0.5
G_TO_TONNE = 1e-6
MWH_TO_KWH = 1e3