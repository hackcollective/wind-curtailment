import datetime

import pandas as pd
import streamlit as st

from lib.curtailment import MINUTES_TO_HOURS
from lib.gcp_db_utils import read_data
from lib.plot import make_time_series_plot

MIN_DATE = pd.to_datetime("2022-01-01")
MAX_DATE = pd.to_datetime("2024-01-01")


@st.cache
def get_data(current_hour: str) -> pd.DataFrame:
    """
    Use the current hour as the key for the cache, so that when the hour changes, we
    will pull a new version of the data, but we aren't constantly reading
    from the database within each hour
    """

    return read_data()


@st.cache
def filter_data(df, start_date, end_date):
    return df[(df["time"] >= pd.to_datetime(start_date)) & (df["time"] <= pd.to_datetime(end_date))]


@st.cache
def transform_data(df: pd.DataFrame):
    if "cost_gbp" not in df.columns:
        df["cost_gbp"] = 99.99999

    filtered_df = filter_data(df, MIN_DATE, MAX_DATE).copy()
    filtered_df["month_and_year"] = filtered_df["time"].dt.month_name() + " " + filtered_df["time"].dt.year.astype(str)
    total_curtailment = filtered_df["delta_mw"].sum() * MINUTES_TO_HOURS

    return filtered_df, total_curtailment


def write_yearly_plot(df: pd.DataFrame) -> None:
    year_df = df.copy()

    year_df["month_idx"] = year_df["time"].dt.month
    year_df_mean = year_df.groupby("month_and_year").mean()
    year_df = year_df.groupby("month_and_year").sum()
    year_df["month_and_year"] = year_df.index
    year_df["time"] = year_df["month_and_year"]
    year_df["month_idx"] = year_df_mean["month_idx"]
    year_df = year_df.sort_values(by=["month_idx"])

    yearly_curtailment_twh = year_df["delta_mw"].sum() / 10**6 * 0.5
    yearly_curtailment_mgbp = year_df["cost_gbp"].sum() / 10**6
    st.write(f"Total Wind Curtailment {yearly_curtailment_twh:.2f} TWh: £ {yearly_curtailment_mgbp:.2f} M")
    fig = make_time_series_plot(year_df.copy(), title=f"Wind Curtailment for 2022", mw_or_mwh="mwh")

    st.plotly_chart(fig)


def write_monthly_plot(df: pd.DataFrame, month_and_year: str) -> None:
    monthly_df = df[df["month_and_year"] == month_and_year]
    monthly_df["time"] = monthly_df["time"].dt.date
    monthly_df = monthly_df.groupby("time").sum()
    monthly_df["time"] = monthly_df.index

    monthly_curtailment_gwh = monthly_df["delta_mw"].sum() / 10**3 * 0.5
    monthly_curtailment_kgbp = monthly_df["cost_gbp"].sum() / 10**6
    st.write(
        f"Wind Curtailment for {month_and_year} {monthly_curtailment_gwh:.2f} GWh: £ {monthly_curtailment_kgbp:.2f} M"
    )

    fig = make_time_series_plot(monthly_df.copy(), title=f"Wind Curtailment for {month_and_year}", mw_or_mwh="mwh")
    st.plotly_chart(fig)


def write_daily_plot(df: pd.DataFrame, select_date: str) -> None:
    daily_df = df[df["time"].dt.date == select_date]
    daily_df = daily_df.groupby("time").sum()
    daily_df["time"] = daily_df.index

    daily_curtailment_gwh = daily_df["delta_mw"].sum() / 10**3 * 0.5
    daily_curtailment_kgbp = daily_df["cost_gbp"].sum() / 10**6
    st.write(f"Wind Curtailment for {select_date} {daily_curtailment_gwh:.2f} GWh: £ {daily_curtailment_kgbp:.2f} M")

    fig = make_time_series_plot(daily_df.copy(), title=f"Wind Curtailment for {select_date}")
    st.plotly_chart(fig)


current_hour = datetime.datetime.now().strftime("%d/%m/%Y %H")
df = get_data(current_hour=current_hour)

filtered_df, total_curtailment = transform_data(df)

st.session_state.today_date = pd.to_datetime("today").date()
INITIAL_END_DATE = pd.to_datetime("2023-01-01")

st.title("UK Wind Curtailment")
select_date = st.date_input("Select Date", min_value=MIN_DATE, max_value=MAX_DATE, value=st.session_state.today_date)
month_and_year = pd.to_datetime(select_date).month_name() + " " + str(pd.to_datetime(select_date).year)

write_daily_plot(filtered_df, select_date)
write_yearly_plot(filtered_df)
write_monthly_plot(filtered_df, month_and_year)
