# want to investigate the Gas BOAs when the sbp price is negative and not
from lib.data.fetch_bod_data import fetch_bod_data
from lib.data.fetch_boa_data import fetch_physical_data
from lib.constants import SAVE_DIR, DATA_DIR
import pandas as pd

from lib.curtailment import linearize_physical_data

df_bm_units = pd.read_excel(DATA_DIR / "BMUFuelType.xls", header=0)
gas_units = df_bm_units[df_bm_units["FUEL TYPE"] == "CCGT"]

# sbp price starts at -44, and goes up to 200
start_date = "2022-12-29 00:00:00"
end_date = "2022-12-30 00:00:00"

df_bod = fetch_bod_data(start_date=start_date, end_date=end_date, save_dir=SAVE_DIR)
df_physical = fetch_physical_data(start_date=start_date, end_date=end_date, save_dir=SAVE_DIR)

df_bod = df_bod[df_bod["ngcBMUnitName"].isin(list(gas_units["NGC_BMU_ID"]))]
df_physical = df_physical[df_physical["ngcBMUnitName"].isin(list(gas_units["NGC_BMU_ID"]))]
df_physical = df_physical[df_physical["recordType"] == "BOALF"]
df_physical = df_physical[df_physical["soFlag"] == "T"]
df_physical["levelTo"] = df_physical["bidOfferLevelTo"]
df_physical["levelFrom"] = df_physical["bidOfferLevelFrom"]


# calculate total gas production from BOAs
df_physical["time_delta"] = pd.to_datetime(df_physical["timeTo"]) - pd.to_datetime(
    df_physical["timeFrom"]
)
df_physical["up_turn_mwh"] = (
    (df_physical["levelFrom"].astype(float) + df_physical["levelTo"].astype(float))
    * 0.5
    * df_physical["time_delta"].dt.seconds
    / 3600
)

df_physical.groupby('local_datetime').sum()


