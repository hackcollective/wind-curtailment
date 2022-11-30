from lib.curtailment import (
    analyze_one_unit,
    calculate_curtailment_costs_in_gbp,
)
from lib.data.utils import *

# ***********
# this os how to make the test data
# db = DbRepository(BASE_DIR / "scripts/phys_data.db")
# start_time="2022-01-01"
# end_time="2022-01-03"
#
# df_fpn, df_boal, df_bod = db.get_data_for_time_range(start_time=start_time, end_time=end_time)
# df_fpn.to_parquet('tests/data/fpn.parquet')
# df_boal.to_parquet('tests/data/boal.parquet')
# df_bod.to_parquet('tests/data/bod.parquet')
# ***********


def test_analyze_one_unit():
    df_fpn = pd.read_parquet("tests/test_data/fpn.parquet")
    df_boal = pd.read_parquet("tests/test_data/boal.parquet")
    df_bod = pd.read_parquet("tests/test_data/bod.parquet")

    units = df_boal.index.unique()
    unit = units[0]

    _ = analyze_one_unit(
        df_boal_unit=df_boal.loc[unit], df_fpn_unit=df_fpn.loc[unit], df_bod_unit=df_bod.loc[unit]
    )


def test_calculate_curtailment_costs_in_gbp():
    df_fpn = pd.read_parquet("tests/test_data/fpn.parquet")
    df_boal = pd.read_parquet("tests/test_data/boal.parquet")
    df_bod = pd.read_parquet("tests/test_data/bod.parquet")

    units = df_boal.index.unique()
    unit = units[0]

    df_curtailment_unit = analyze_one_unit(
        df_boal_unit=df_boal.loc[unit], df_fpn_unit=df_fpn.loc[unit], df_bod_unit=df_bod.loc[unit]
    )

    costs = calculate_curtailment_costs_in_gbp(df_curtailment_unit)
    assert costs == 447971.25
