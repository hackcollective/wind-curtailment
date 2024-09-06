"""Script to get all bm units

Once extra have been found, they can be added to lib.constants.df_bm_units.
Of course this would be good to be automatic, but for now this is a manual process.

notes:
1. This is used in a github actions to update the bm units
2. You need to install elexonpy from pypi to run this script.
"""
from datetime import datetime, timedelta
from pathlib import Path

import elexonpy
import pandas as pd

from lib.constants import df_bm_units  # these are the current bm units

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"

api_instance = elexonpy.DatasetsApi()
start_date = datetime(2000, 1, 1)
end_date = datetime.today()

all_bm_units = []
# loop over years of data
while start_date.year < end_date.year + 1:
    print(start_date)

    api_response = api_instance.datasets_igcpu_get(
        start_date, start_date + timedelta(days=365), format="json"
    )
    bm_units = pd.DataFrame([x.__dict__ for x in api_response.data])
    all_bm_units.append(bm_units)
    start_date = start_date.replace(year=start_date.year + 1)

bm_units = pd.concat(all_bm_units)

# only select _psr_type = 'WInd Onshore' or 'Wind Offshore'
bm_units = bm_units[bm_units["_psr_type"].str.contains("Wind")]


# look which units are missing in bmufuel_types
missing_units = bm_units[~bm_units["_bm_unit"].isin(df_bm_units["SETT_BMU_ID"])]
missing_units = bm_units[~bm_units["_registered_resource_name"].isin(df_bm_units["NGC_BMU_ID"])]

# get rid of units with no bm_unit
missing_units = missing_units[missing_units["_bm_unit"].notnull()]

# order by _publish_time
missing_units = missing_units.sort_values("_publish_time")

print(f"Found {len(missing_units)} extra units in the BM data.")
print("Found the following extra units:")

for _, m in missing_units.iterrows():
    print(m._bm_unit, m._installed_capacity, m._publish_time, m._registered_resource_name)

print(f'Total extra capacity is {missing_units["_installed_capacity"].sum()} MW')

# append extra units
missing_units.rename(
    columns={
        "_bm_unit": "SETT_BMU_ID",
        "_effective_from": "EFF_FROM",
        "_registered_resource_name": "NGC_BMU_ID",
    },
    inplace=True,
)
missing_units["FUEL TYPE"] = "WIND"
missing_units = missing_units[["SETT_BMU_ID", "EFF_FROM", "NGC_BMU_ID", "FUEL TYPE"]]
# order by EFF_FROM
missing_units = missing_units.sort_values("EFF_FROM")
df_bm_units_extra = pd.concat([df_bm_units, missing_units])

# drop duplicated in the SETT_BMU_ID column
df_bm_units_extra = df_bm_units_extra.drop_duplicates(subset="NGC_BMU_ID")

# save to csv
# note this override the current CSV file
df_bm_units_extra.to_csv(DATA_DIR / "BMU.csv", index=False)
