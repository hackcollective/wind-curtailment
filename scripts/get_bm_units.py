"""Script to get all bm units

Once extra have been found, they can be added to lib.constants.df_bm_units
"""
import elexonpy
from datetime import datetime, timedelta
import pandas as pd
from lib.constants import df_bm_units # these are the current bm units

api_instance = elexonpy.DatasetsApi()
start_date = datetime(2000, 1, 1)
end_date = datetime.today()

all_bm_units = []
# loop over years of data
while start_date.year < end_date.year+1:
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
# get rid of units with no bm_unit
missing_units = missing_units[missing_units["_bm_unit"].notnull()]

# order by _publish_time
missing_units = missing_units.sort_values("_publish_time")

print(f'Found {len(missing_units)} extra units in the BM data.')
print('Found the following extra units:')

for _, m in missing_units.iterrows():
    print(m._bm_unit, m._installed_capacity, m._publish_time)

print(f'Total extra capacity is {missing_units["_installed_capacity"].sum()} MW')