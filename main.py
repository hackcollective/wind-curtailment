import pandas as pd
import streamlit as st

from lib.constants import BASE_DIR
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

print(filtered_df.columns)

fig = make_time_series_plot(filtered_df.copy())

st.plotly_chart(fig)
