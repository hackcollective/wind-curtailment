import pandas as pd
import streamlit as st

from lib.curtailment import MINUTES_TO_HOURS
from lib.gcp_db_utils import read_data
from lib.plot import make_time_series_plot


@st.cache
def filter_data(df, start_date, end_date):
    return df[(df["time"] >= pd.to_datetime(start_date)) & (df["time"] <= pd.to_datetime(end_date))]


df = read_data()

if 'cost_gbp' not in df.columns:
    df['cost_gbp'] = 99.99999

df = df.rename(columns={'level_fpn':'level_fpn_mw',
                        'level_after_boal':'level_after_boal_mw'})

# go from 30 min mean mw to mwh
df['level_fpn_mwh'] = df['level_fpn_mw'] * 0.5
df['level_after_boal_mwh'] = df['level_after_boal_mw'] * 0.5


MIN_DATE = pd.to_datetime("2022-01-01")
MAX_DATE = pd.to_datetime("2023-01-01")
INITIAL_END_DATE = pd.to_datetime("2023-01-01")

st.title("UK Wind Curtailment")

start_date = st.date_input("Start Time", min_value=MIN_DATE, max_value=MAX_DATE, value=MIN_DATE)
end_date = st.date_input("End Time", min_value=MIN_DATE, max_value=MAX_DATE, value=INITIAL_END_DATE)
filtered_df = filter_data(df.copy(), start_date, end_date)

total_curtailment = filtered_df["delta_mw"].sum() * MINUTES_TO_HOURS

year_df = filtered_df.copy()

year_df["month_idx"] = year_df["time"].dt.month
year_df["time"] = year_df["time"].dt.month_name()
year_df_mean = year_df.groupby("time").mean()
year_df = year_df.groupby("time").sum()
year_df["time"] = year_df.index
year_df['month_idx'] = year_df_mean['month_idx']
year_df = year_df.sort_values(by=['month_idx'])

yearly_curtailment_twh = year_df["delta_mw"].sum() / 10 ** 6 * 0.5
yearly_curtailment_mgbp = year_df["cost_gbp"].sum() / 10 ** 6
st.write(f"Wind Curtailment {yearly_curtailment_twh:.2f} TWh: £ {yearly_curtailment_mgbp:.2f} M")

# monthly plot
fig = make_time_series_plot(year_df.copy(), title=f"Wind Curtailment for 2022", mw_or_mwh='mwh')
st.plotly_chart(fig)

# drop down box for months
option_month = st.selectbox(label="Select a month",
                            options=list(year_df["time"]),
                            index=len(year_df)-1)

# get the monthly data
monthly_df = filtered_df[filtered_df["time"].dt.month_name() == option_month]
monthly_df["time"] = monthly_df["time"].dt.date
monthly_df = monthly_df.groupby("time").sum()
monthly_df["time"] = monthly_df.index

monthly_curtailment_gwh = monthly_df["delta_mw"].sum() / 10 ** 3 * 0.5
monthly_curtailment_kgbp = monthly_df["cost_gbp"].sum() / 10 ** 6
st.write(f"Wind Curtailment {monthly_curtailment_gwh:.2f} GWh: £ {monthly_curtailment_kgbp:.2f} M")

# daily plot plot
fig = make_time_series_plot(monthly_df.copy(), title=f"Wind Curtailment for {option_month}", mw_or_mwh='mwh')
st.plotly_chart(fig)

# day drop droplet
option_day = st.selectbox(label="Select a date",
                          options=list(monthly_df["time"]),
                          index=len(monthly_df) - 1)
# get the day data

daily_df = filtered_df[filtered_df["time"].dt.date == option_day]
daily_df = daily_df.groupby("time").sum()
daily_df["time"] = daily_df.index

daily_curtailment_gwh = daily_df["delta_mw"].sum() / 10 ** 3 * 0.5
daily_curtailment_kgbp = daily_df["cost_gbp"].sum() / 10 ** 6
st.write(f"Wind Curtailment {daily_curtailment_gwh:.2f} GWh: £ {daily_curtailment_kgbp:.2f} M")

fig = make_time_series_plot(daily_df.copy(), title=f"Wind Curtailment for {option_day}")
st.plotly_chart(fig)
