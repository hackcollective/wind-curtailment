import pandas as pd
import streamlit as st

from lib.curtailment import MINUTES_TO_HOURS
from lib.plot import make_time_series_plot
from lib.gcp_db_utils import read_data

@st.cache(allow_output_mutation=True)
def load_csv_data():
    return read_data()


@st.cache
def filter_data(df, start_date, end_date):
    return df[(df["time"] >= pd.to_datetime(start_date)) & (df["time"] <= pd.to_datetime(end_date))]


df = load_csv_data()
if 'cost_gbp' not in df.columns:
    df['cost_gbp'] = 99.99999

MIN_DATE = pd.to_datetime("2022-01-01")
MAX_DATE = pd.to_datetime("2022-12-01")
INITIAL_END_DATE = pd.to_datetime("2022-12-01")

st.title("UK Wind Curtailment")
start_date = st.date_input("Start Time", min_value=MIN_DATE, max_value=MAX_DATE, value=MIN_DATE)
end_date = st.date_input("End Time", min_value=MIN_DATE, max_value=MAX_DATE, value=INITIAL_END_DATE)
filtered_df = filter_data(df.copy(), start_date, end_date)

total_curtailment = filtered_df["delta_mw"].sum() * MINUTES_TO_HOURS

year_df = filtered_df.copy()
year_df['time'] = filtered_df['time'].dt.month_name()
year_df = year_df.groupby('time').sum()
year_df['time'] = year_df.index

# monthly plot
fig = make_time_series_plot(year_df.copy())
st.plotly_chart(fig)

# drop down box for months
option_month = st.selectbox('Select a month',list(year_df['time']))

# get the monthly data
monthly_df = filtered_df[filtered_df['time'].dt.month_name() == option_month]
monthly_df['time'] = monthly_df['time'].dt.date
monthly_df = monthly_df.groupby('time').sum()
monthly_df['time'] = monthly_df.index

# daily plot plot
fig = make_time_series_plot(monthly_df.copy())
st.plotly_chart(fig)

# day drop droplet
option_day = st.selectbox('Select a date', list(monthly_df['time']))
# get the day data

daily_df = filtered_df[filtered_df['time'].dt.date == option_day]
daily_df = daily_df.groupby('time').sum()
daily_df['time'] = daily_df.index

fig = make_time_series_plot(daily_df.copy())
st.plotly_chart(fig)



