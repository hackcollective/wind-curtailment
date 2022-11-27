import pandas as pd
import plotly.express as px
import streamlit as st

from lib.constants import BASE_DIR
from lib.curtailment import MINUTES_TO_HOURS
from lib.plot import make_time_series_plot


@st.cache
def load_csv_data():
    return pd.read_csv(BASE_DIR / "data/outputs/results-2022-01-01-2022-10-01.csv", parse_dates=["Time"])


@st.cache
def filter_data(start_date, end_date):
    return df[(df["Time"] >= pd.to_datetime(start_date)) & (df["Time"] <= pd.to_datetime(end_date))].copy()


df = load_csv_data()
if 'cost_gbp' not in df.columns:
    df['cost_gbp'] = 99.99999

MIN_DATE = pd.to_datetime("2022-01-01")
MAX_DATE = pd.to_datetime("2022-05-01")
INITIAL_END_DATE = pd.to_datetime("2022-02-01")

st.title("UK Wind Curtailment")
start_date = st.date_input("Start Time", min_value=MIN_DATE, max_value=MAX_DATE, value=MIN_DATE)
end_date = st.date_input("End Time", min_value=MIN_DATE, max_value=MAX_DATE, value=INITIAL_END_DATE)
filtered_df = filter_data(start_date, end_date)

total_curtailment = filtered_df["delta"].sum() * MINUTES_TO_HOURS

print(filtered_df.columns)

fig = make_time_series_plot(filtered_df)

st.plotly_chart(fig)
