import pandas as pd
from pathlib import Path

from ElexonDataPortal import api
API_KEY = "xutthojn7xa28q6"

client = api.Client(API_KEY)


def fetch_physical_data(start_date, end_date, save_dir: Path):
    """From a brief visual inspection, this returns data that looks the same as the stuff I downloaded manually"""
    file_name = save_dir/f"{start_date}-{end_date}.fthr"
    if file_name.exists():
        return pd.read_feather(file_name)
        
    df = client.get_PHYBMDATA(start_date=start_date, end_date=end_date)
    df.to_feather(file_name)

    return df

def add_bm_unit_type(df:pd.DataFrame, df_bm_units: pd.DataFrame) -> pd.DataFrame:
    df = (
    df.set_index("bmUnitID").join(df_bm_units.set_index("SETT_BMU_ID")["FUEL TYPE"])
    .rename(columns={"FUEL TYPE": "Fuel Type"})
)   
    df['Fuel Type'].fillna('Battery?', inplace=True)
    return df.dropna(axis=1, how="all")


def parse_fpn_from_physical_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["recordType"]=="PN"]
    return df.dropna(axis=1, how="all")

def parse_boal_from_physical_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df['recordType']=="BOALF"]
    return df.dropna(axis=1, how="all")